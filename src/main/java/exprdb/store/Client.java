package exprdb.store;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.stream.Collectors;
import static java.util.stream.Collectors.toList;
import java.nio.ByteBuffer;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.commons.configuration2.*;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.asynchttpclient.*;
import java.util.concurrent.Future;
import io.netty.util.HashedWheelTimer;

import exprdb.proto.Database.*;
import com.google.protobuf.InvalidProtocolBufferException;

public final class Client implements Closeable {
  private static Logger log = LogManager.getLogger(Client.class);
  private static List<byte[]> emptyValues = new ArrayList<>();
  final static HashedWheelTimer timer;
  AsyncHttpClient client;
  int timeout;
  private String uri;
  private String table;
  private String token;
  private String options;

  public enum Status {
    OK,
    Error
  }

  public static class Result {
    exprdb.proto.Database.Response rsp;

    Result(exprdb.proto.Database.Response rsp) {
      this.rsp = rsp;
    }

    public Status status() {
      switch(rsp.getType()) {
      case Error:
        return Status.Error;
      }
      return Status.OK;
    }

    public String token() {
      return rsp.getToken();
    }

    public int count() {
      return rsp.getKeysCount();
    }

    public List<byte[]> keys() {
      return rsp.getKeysList().stream().map(k -> k.toByteArray()).collect(toList());
    }

    public List<byte[]> values() {
      return rsp.getValuesList().stream().map(v -> v.toByteArray()).collect(toList());
    }

    public byte[] logops() {
      return rsp.getLogops().toByteArray();
    }

    public long seqno() {
      return rsp.getSeqno();
    }

    public byte[] getKey(int index) {
      return rsp.getKeys(index).toByteArray();
    }

    public byte[] getValue(int index) {
      return rsp.getValues(index).toByteArray();
    }

    public String toString() {
      return rsp.toString();
    }
  }

  static {
    timer = new HashedWheelTimer();
    timer.start();
  }

  public Client(String uri, String table, String options, int timeout) {
    try {
      final AsyncHttpClientConfig config = new DefaultAsyncHttpClientConfig.Builder()
        .setRequestTimeout(timeout)
        .setNettyTimer(timer)
        .setFollowRedirect(true)
        .build();
      client = new DefaultAsyncHttpClient(config);
    } catch(Exception e) {
      throw new KdbException(e);
    }
    this.uri = uri;
    this.table = table;
    this.token = "";
    this.options = options;
  }

  public Client(String uri, String table, String options) {
    this(uri, table, options, Integer.MAX_VALUE);
  }

  public Client(String uri, String table) {
    this(uri, table, null);
  }

  Client() {
    try {
      final AsyncHttpClientConfig config = new DefaultAsyncHttpClientConfig.Builder()
        .setRequestTimeout(Integer.MAX_VALUE)
        .setNettyTimer(timer)
        .setFollowRedirect(true)
        .build();
      client = new DefaultAsyncHttpClient(config);
    } catch(Exception e) {
      throw new KdbException(e);
    }
  }

  public Result open() {
    return open((String)null);
  }

  public Result openCompressed(String compression) {
    return open(-1, compression);
  }

  public Result open(int ttl) {
    return open(ttl, null);
  }

  public Result open(int ttl, String compression) {
    return open((String)null, ttl, compression);
  }

  public Result open(String merge) {
    return open(merge, -1);
  }

  public Result open(String merge, int ttl) {
    return open(merge, ttl, null);
  }

  public Result open(String merge, int ttl, String compression) {
    Message msg = sendMsg(MessageBuilder.buildOpenOp(table, null, merge, ttl, compression, options));
    return new Result(msg.getResponse());
  }

  public Result open(List<String> columns) {
    return open(columns, null);
  }

  public Result openCompressed(List<String> columns, String compression) {
    return open(columns, null, -1, compression);
  }

  public Result open(List<String> columns, int ttl) {
    return open(columns, ttl, null);
  }

  public Result open(List<String> columns, int ttl, String compression) {
    return open(columns, null, ttl, compression);
  }

  public Result open(List<String> columns, String merge) {
    return open(columns, merge, null);
  }

  public Result open(List<String> columns, String merge, String compression) {
    return open(columns, merge, -1, compression);
  }

  public Result open(List<String> columns, String merge, int ttl) {
    return open(columns, merge, ttl, null);
  }

  public Result open(List<String> columns, String merge, int ttl, String compression) {
    Message msg = sendMsg(MessageBuilder.buildOpenOp(table, columns, merge, ttl, compression, options));
    return new Result(msg.getResponse());
  }

  public long getLatestSequenceNumber() {
    Message msg = sendMsg(MessageBuilder.buildSeqOp(table));
    return msg.getResponse().getSeqno();
  }

  public Result scanlog(long seqno, int limit) {
    Message msg = sendMsg(MessageBuilder.buildScanlogOp(table, seqno, limit));
    return new Result(msg.getResponse());
  }

  public Result compact() {
    return compact(null, null, null);
  }

  public Result compact(String column) {
    return compact(column, null, null);
  }

  public Result compact(byte[] begin, byte[] end) {
    return compact(null, begin, end);
  }

  public Result compact(String column, byte[] begin, byte[] end) {
    Message msg = sendMsg(MessageBuilder.buildCompactOp(table, column, begin, end));
    return new Result(msg.getResponse());
  }

  public Result drop(String column) {
    Message msg = sendMsg(MessageBuilder.buildDropOp(table, column));
    return new Result(msg.getResponse());
  }

  public Result drop() {
    Message msg = sendMsg(MessageBuilder.buildDropOp(table));
    return new Result(msg.getResponse());
  }

  public Result put(List<byte[]> keys, List<byte[]> values) {
    Message msg = sendMsg(MessageBuilder.buildPutOp(table, null, keys, values));
    return new Result(msg.getResponse());
  }

  public Result put(List<byte[]> keys) {
    Message msg = sendMsg(MessageBuilder.buildPutOp(table, null, keys, emptyValues));
    return new Result(msg.getResponse());
  }

  public Result put(String column, List<byte[]> keys, List<byte[]> values) {
    Message msg = sendMsg(MessageBuilder.buildPutOp(table, column, keys, values));
    return new Result(msg.getResponse());
  }

  public Result put(String column, List<byte[]> keys) {
    Message msg = sendMsg(MessageBuilder.buildPutOp(table, column, keys, emptyValues));
    return new Result(msg.getResponse());
  }

  public Result get(List<byte[]> keys) {
    return get(null, keys);
  }

  public Result get(String column, List<byte[]> keys) {
    Message msg = sendMsg(MessageBuilder.buildGetOp(table, column, keys));
    return new Result(msg.getResponse());
  }

  public Result scanFirst(int limit) {
    return scanFirst(null, limit);
  }

  public Result scanLast(int limit) {
    return scanLast(null, limit);
  }

  public Result scanForward(byte[] prefix, byte[] suffix, int limit) {
    return scanForward(null, prefix, suffix, limit);
  }

  public Result scanForward(byte[] prefix, int limit) {
    return scanForward(null, prefix, null, limit);
  }

  public Result scanNext(int limit) {
    Message msg = sendMsg(MessageBuilder.buildScanOp(ScanOperation.Type.Next,
                                                     table,
                                                     null,
                                                     token,
                                                     limit,
                                                     null,
                                                     null
                                                     ));
    return new Result(msg.getResponse());
  }

  public Result scanBackwards(byte[] prefix, byte[] suffix, int limit) {
    return scanBackwards(null, prefix, suffix, limit);
  }

  public Result scanBackwards(byte[] prefix, int limit) {
    return scanBackwards(null, prefix, null, limit);
  }

  public Result scanPrev(int limit) {
    Message msg = sendMsg(MessageBuilder.buildScanOp(ScanOperation.Type.Prev,
                                                     table,
                                                     null,
                                                     token,
                                                     limit,
                                                     null,
                                                     null
                                                     ));
    return new Result(msg.getResponse());
  }


  public Result scanFirst(String column, int limit) {
    releaseToken();
    Message msg = sendMsg(MessageBuilder.buildScanOp(ScanOperation.Type.First,
                                                     table,
                                                     column,
                                                     null,
                                                     limit,
                                                     null,
                                                     null));
    token = msg.getResponse().getToken();
    return new Result(msg.getResponse());
  }

  public Result scanLast(String column, int limit) {
    releaseToken();
    Message msg = sendMsg(MessageBuilder.buildScanOp(ScanOperation.Type.Last,
                                                     table,
                                                     column,
                                                     null,
                                                     limit,
                                                     null,
                                                     null));
    token = msg.getResponse().getToken();
    return new Result(msg.getResponse());
  }

  public Result scanForward(String column, byte[] prefix, byte[] suffix, int limit) {
    releaseToken();
    Message msg = sendMsg(MessageBuilder.buildScanOp(ScanOperation.Type.ScanNext,
                                                     table,
                                                     column,
                                                     null,
                                                     limit,
                                                     prefix,
                                                     suffix));
    token = msg.getResponse().getToken();
    return new Result(msg.getResponse());
  }

  public Result scanBackwards(String column, byte[] prefix, byte[] suffix, int limit) {
    releaseToken();
    Message msg = sendMsg(MessageBuilder.buildScanOp(ScanOperation.Type.ScanPrev,
                                                     table,
                                                     column,
                                                     null,
                                                     limit,
                                                     prefix,
                                                     suffix));
    return new Result(msg.getResponse());
  }

  public Result listBackup() {
    Message msg = sendMsg(MessageBuilder.buildBackupOp(BackupOperation.Type.List, table));
    return new Result(msg.getResponse());
  }

  public void backup() {
    Message msg = sendMsg(MessageBuilder.buildBackupOp(BackupOperation.Type.Create, table));
  }

  public Result restore(int backupid) {
    Message msg = sendMsg(MessageBuilder.buildRestoreOp(table, backupid));
    return new Result(msg.getResponse());
  }

  Result scanlog(String uri, String table, long seqno, int limit) {
    Message msg = MessageBuilder.buildScanlogOp(table, seqno, limit);
    Message rsp = MessageBuilder.nullMsg;
    try {
      //log.info("msg {}", msg);
      org.asynchttpclient.Response r = client
        .preparePost(uri)
        .setBody(msg.toByteArray())
        .execute()
        .get();
      byte[] data = r.getResponseBodyAsBytes();
      rsp = Message.parseFrom(data);
    } catch(InterruptedException e) {
      log.debug(e);
      //e.printStackTrace();
    } catch(ExecutionException e) {
      log.debug(e);
      throw new KdbException(e);
      //e.printStackTrace();
    } catch(InvalidProtocolBufferException e) {
      log.debug(e);
      throw new KdbException(e);
      //e.printStackTrace();
    }
    return new Result(rsp.getResponse());
  }

  private Message sendMsg(Message msg) {
    Message rsp = MessageBuilder.nullMsg;
    try {
      //log.info("msg {}", msg);
      org.asynchttpclient.Response r = client
        .preparePost(uri)
        .setBody(msg.toByteArray())
        .execute()
        .get();
      byte[] data = r.getResponseBodyAsBytes();
      rsp = Message.parseFrom(data);
    } catch(InterruptedException e) {
      log.debug(e);
      //e.printStackTrace();
    } catch(ExecutionException e) {
      log.debug(e);
      throw new KdbException(e);
      //e.printStackTrace();
    } catch(InvalidProtocolBufferException e) {
      log.debug(e);
      throw new KdbException(e);
      //e.printStackTrace();
    }
    return rsp;
  }

  private void releaseToken() {
    if(token.length() > 0) {
      sendMsg(MessageBuilder.buildScanOp(ScanOperation.Type.Close, table, null, token, 0, null, null));
    }
    token = "";
  }

  public void close() {
    try {
      releaseToken();
      client.close();
    } catch(Exception e) {
      log.info(e);
    }
  }

}
