package exprdb.store;

import java.net.*;
import java.time.Duration;
import java.util.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.commons.configuration2.*;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import exprdb.store.rsm.PendingRequests;
import exprdb.store.rsm.PendingRequests.Tuple;
import exprdb.store.rsm.StateMachine;
import exprdb.store.rsm.Zab;
import exprdb.store.rsm.ZabConfig;
import exprdb.store.rsm.ZabException;
import exprdb.store.rsm.Zxid;
import exprdb.proto.Database.Message;
import exprdb.proto.Database.Message.MessageType;

class Ring implements StateMachine {
  private static Logger log = LogManager.getLogger(Ring.class);

  private String serverId;
  private final ZabConfig config = new ZabConfig();
  Store store;
  private String leader;
  private String leaderd;
  private Set<String> members;
  private Set<String> followers;

  public Zab zab;

  public static List<Ring> configRings() {
    PropertiesConfiguration config = Config.get();
    boolean standalone = config.getBoolean("standalone", false);
    List<Ring> rings = new ArrayList<Ring>();
    if(!standalone) {
      List ringaddrs = config.getList("ringaddr");
      List leaders = config.getList("leader");
      List logs = config.getList("logDir");

      int len = ringaddrs.size();
      if((leaders.size() > 0 && len != leaders.size()) || len != logs.size())
        throw new KdbException("ring config error");

      for(int i = 0; i < len; i++) {
        Ring ring = new Ring((String)ringaddrs.get(i), leaders.size() == 0? null: (String)leaders.get(i), (String)logs.get(i));
        rings.add(ring);
        if(!standalone) {
          ring.bind(Store.get());
        }
      }
    }
    return rings;
  }

  public Ring(String serverId, String joinPeer, String logDir) {
    try {
      this.serverId = serverId;
      this.leader = null;
      this.leaderd = null;
      if (this.serverId != null && joinPeer == null) {
        // It's the first server in cluster, joins itself.
        joinPeer = this.serverId;
      }
      if (this.serverId != null && logDir == null) {
        logDir = this.serverId;
      }
      config.setLogDir(logDir);
      File logdata = new File(logDir);
      if (!logdata.exists()) {
        logdata.mkdirs();
        zab = new Zab(this, config, this.serverId, joinPeer);
      } else {
        // Recovers from log directory.
        zab = new Zab(this, config);
      }
      this.serverId = zab.getServerId();

    } catch (Exception ex) {
      log.error("Caught exception : ", ex);
      throw new RuntimeException();
    }
  }

  public void bind(Store store) {
    this.store = store;
  }

  public boolean isLeader() {
    return this.leader == null || this.leader.equals(serverId);
  }

  public String leader() {
    return leader == null? serverId: leader;
  }

  private void setLeaderd() {
    String[] parts = leader().split(":");
    int port = -1;
    try {
      port = Integer.parseInt(parts[1]);
    } catch(NumberFormatException e) {
      log.info("dataaddr error");
    }
    leaderd = parts[0] + ":" + (--port);
  }

  public String serverid() {
    return serverId;
  }

  public String leaderd() {
    return leaderd;
  }

  @Override
  public ByteBuffer preprocess(Zxid zxid, ByteBuffer message) {
    //log.info("Preprocessing a message: {}", message);
    return message;
  }

  @Override
  public void deliver(Zxid zxid, ByteBuffer stateUpdate, String clientId,
                      Object ctx) {
    //log.info("deliver {}, {}", stateUpdate, ctx);
    Message msg = MessageBuilder.nullMsg;
    Message ret = MessageBuilder.nullMsg;
    try {
      byte[] arr = new byte[stateUpdate.remaining()];
      stateUpdate.get(arr);
      msg = Message.parseFrom(arr);
      ret = store.handle(msg);
    } catch(IOException e) {
      log.info("deliver callback handle {}", e);
    } finally {
      if(msg.getType() != MessageType.Sequence) {
        Transport.reply(ctx, ret);
      } else {
        //log.info("msg {} => r {}", msg, ret);
      }
    }
  }

  @Override
  public void flushed(Zxid zxid, ByteBuffer request, Object ctx) {
    log.info("flush {} message: {}", zxid, ctx);
  }

  @Override
  public void save(FileOutputStream fos) {
    log.info("save snapshot");
  }

  @Override
  public void restore(FileInputStream fis) {
    log.info("restore snapshot");
  }

  @Override
  public void snapshotDone(String filePath, Object ctx) {
    log.info("snapshotDone");
  }

  @Override
  public void removed(String peerId, Object ctx) {
    log.info("removed");
  }

  @Override
  public void recovering(PendingRequests pendingRequests) {
    log.info("Ring recovering ... pending sizes {}", pendingRequests.pendingSends.size());
    Message msg = MessageBuilder.buildErrorResponse("Service Error");
    for (Tuple tp : pendingRequests.pendingSends) {
      if(tp.param instanceof io.netty.channel.ChannelHandlerContext)
        Transport.reply(tp.param, msg);
    }
  }

  @Override
  public void leading(Set<String> activeFollowers, Set<String> clusterMembers) {
    this.followers = activeFollowers;
    this.members = clusterMembers;
    this.leader = serverId;
    setLeaderd();
    if(log.isDebugEnabled()) {
      log.info("LEADING with active followers : ");
      for (String peer : activeFollowers) {
        log.info(" -- {}", peer);
      }
      log.info("Cluster configuration change : ", clusterMembers.size());
      for (String peer : clusterMembers) {
        log.info(" -- {}", peer);
      }
    }
  }

  @Override
  public void following(String leader, Set<String> clusterMembers) {
    this.leader = leader;
    this.members = clusterMembers;
    setLeaderd();
    if(log.isDebugEnabled()) {
      log.info("FOLLOWING {}", leader);
      log.info("Cluster configuration change : ", clusterMembers.size());
      for (String peer : clusterMembers) {
        log.info(" -- {}", peer);
      }
    }
  }
}
