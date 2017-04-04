package exprdb.store;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.DecoderResult;
import io.netty.util.AsciiString;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.*;
import static io.netty.handler.codec.http.HttpHeaderNames.*;
import java.io.File;
import io.netty.buffer.ByteBuf;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.commons.configuration2.*;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import exprdb.proto.Database.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import exprdb.store.rsm.ZabException;
import java.util.List;
import java.util.ArrayList;

public class Transport {
  private static Logger log = LogManager.getLogger(Transport.class);
  private static Transport instance = new Transport();
  String dataaddr;
  private static final AsciiString CONTENT_TYPE = new AsciiString("Content-Type");
  private static final AsciiString CONTENT_LENGTH = new AsciiString("Content-Length");
  private static final AsciiString CONNECTION = new AsciiString("Connection");
  private static final AsciiString KEEP_ALIVE = new AsciiString("keep-alive");

  private Transport() { }

  public static Transport get() {
    return instance;
  }

  private static class HostPort {
    public String host;
    public int port;

    public static HostPort parse(String dataaddr) {
      String[] parts= dataaddr.split(":");
      HostPort hostport = new HostPort();
      hostport.host = "localhost";
      hostport.port = 8000;
      if(parts.length != 2) {
        log.info("dataaddr error");
      }
      hostport.host = parts[0];
      try {
        hostport.port = Integer.parseInt(parts[1]);
      } catch(NumberFormatException e) {
        log.info("dataaddr error");
      }
      return hostport;
    }
  }

  public void start() {
    PropertiesConfiguration config = Config.get();
    dataaddr = config.getString("dataaddr");
    HostPort hostport = HostPort.parse(dataaddr);
    boolean SSL = config.getBoolean("ssl", false);
    final SslContext sslCtx;
    if (SSL) {
      try {
        SelfSignedCertificate ssc = new SelfSignedCertificate();
        sslCtx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
      } catch(Exception e) {
        throw new KdbException(e);
      }
    } else {
      sslCtx = null;
    }

    boolean standalone = config.getBoolean("standalone", false);
    Store.get().bind(config.getString("store"), config.getString("store.wal"));
    DataNode datanode = new DataNode();

    EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    EventLoopGroup workerGroup = new NioEventLoopGroup();
    try {
      ServerBootstrap b = new ServerBootstrap();
      b.option(ChannelOption.SO_BACKLOG, 512)
        .option(ChannelOption.SO_REUSEADDR, true)
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .childOption(ChannelOption.TCP_NODELAY, true)
        .childOption(ChannelOption.SO_RCVBUF, 4*1024*1024)
        .childOption(ChannelOption.SO_SNDBUF, 4*1024*1024)
        .childOption(ChannelOption.SO_TIMEOUT, 30000);
      b.group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        //.handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(new HttpKdbServerInitializer(sslCtx, datanode));
      Channel ch = b.bind(hostport.port).sync().channel();
      ch.closeFuture().sync();
    } catch(Exception e) {
      throw new KdbException(e);
    } finally {
      bossGroup.shutdownGracefully();
      workerGroup.shutdownGracefully();
    }
  }

  public static void redirect(Object ctx, String uri) {
    ChannelHandlerContext context = (ChannelHandlerContext)ctx;
    FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1,
                                                            TEMPORARY_REDIRECT,
                                                            Unpooled.EMPTY_BUFFER);
    response.headers().set(LOCATION, "http://"+uri);
    context.writeAndFlush(response);
    context.close();
  }

  public static void reply(Object ctx, Message msg) {
    ChannelHandlerContext context = (ChannelHandlerContext)ctx;
    if (context == null) {
      // This request is sent from other instance.
      return;
    }
    FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1,
                                                            OK,
                                                            Unpooled.wrappedBuffer(msg.toByteArray()));
    response.headers().set(CONTENT_TYPE, "application/octet-stream");
    response.headers().setInt(CONTENT_LENGTH, response.content().readableBytes());
    //response.headers().set(CONNECTION, KEEP_ALIVE);
    //context.write(response).addListener(ChannelFutureListener.CLOSE);;
    context.writeAndFlush(response);
  }

  public static class HttpKdbServerHandler extends ChannelInboundHandlerAdapter {
    private DataNode datanode;

    public HttpKdbServerHandler(DataNode datanode) {
      this.datanode = datanode;
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
      ctx.flush();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object data) {
      if (data instanceof HttpRequest) {
        FullHttpResponse response;
        HttpRequest req = (HttpRequest)data;
        if (HttpUtil.is100ContinueExpected(req)) {
          ctx.write(new DefaultFullHttpResponse(HTTP_1_1, CONTINUE));
        }
        Message msg = MessageBuilder.emptyMsg;
        //log.info("req method {}", req.method());
        if(req.method() == HttpMethod.POST) {
          FullHttpMessage m = (FullHttpMessage) data;
          ByteBuf buf = null;
          try {
            buf = m.content();
            byte[] bytes = new byte[buf.readableBytes()];
            buf.readBytes(bytes);
            msg = Message.parseFrom(bytes);
            datanode.process(msg, ctx);
            return;
          } catch(InvalidProtocolBufferException e) {
            //log.info(e);
            msg = MessageBuilder.buildErrorResponse("InvalidProtocolBufferException");
          } catch(KdbException e) {
            //log.info(e);
            msg = MessageBuilder.buildErrorResponse(e.getMessage());
          } finally {
            if(buf != null)
              buf.release();
          }
          response = new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.wrappedBuffer(Unpooled.wrappedBuffer(msg.toByteArray())));
          response.headers().set(CONTENT_TYPE, "application/octet-stream");
          response.headers().setInt(CONTENT_LENGTH, response.content().readableBytes());
        } else {
          //log.info("req uri {}", req.uri());
          String value = datanode.stats(req.uri().substring(1));
          response = new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.wrappedBuffer(Unpooled.wrappedBuffer(value.getBytes())));
          response.headers().set(CONTENT_TYPE, "text/json");
          response.headers().setInt(CONTENT_LENGTH, value.length());
        }
        boolean keepAlive = HttpUtil.isKeepAlive(req);
        if (!keepAlive) {
          ctx.write(response).addListener(ChannelFutureListener.CLOSE);
        } else {
          response.headers().set(CONNECTION, KEEP_ALIVE);
          ctx.write(response);
        }
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      log.info("remote {} exception {}", ctx.channel().remoteAddress(), cause.getMessage());
      ctx.close();
    }
  }

  public static class HttpKdbServerInitializer extends ChannelInitializer<SocketChannel> {

    private final SslContext sslCtx;
    private DataNode datanode;

    public HttpKdbServerInitializer(SslContext sslCtx, DataNode datanode) {
      this.sslCtx = sslCtx;
      this.datanode = datanode;
    }

    @Override
    public void initChannel(SocketChannel ch) {
      ChannelPipeline p = ch.pipeline();
      if (sslCtx != null) {
        p.addLast(sslCtx.newHandler(ch.alloc()));
      }
      p.addLast(new HttpServerCodec());
      p.addLast("aggregator", new HttpObjectAggregator(Integer.MAX_VALUE));
      p.addLast(new HttpKdbServerHandler(datanode));
    }
  }

  public static void main(String[] args) throws Exception {
    if(args.length < 1) {
      System.out.println("java -cp ./target/kdb-1.0-SNAPSHOT.jar kdb.Transport conf/datanode.properties");
      return;
    }
    File propertiesFile = new File(args[0]);
    if(!propertiesFile.exists()) {
      System.out.printf("config file %s does not exist", propertiesFile.getName());
      return;
    }
    Configurations configs = new Configurations();
    PropertiesConfiguration config = configs.properties(propertiesFile);
    Config.init(config);
    Transport.get().start();
  }
}
