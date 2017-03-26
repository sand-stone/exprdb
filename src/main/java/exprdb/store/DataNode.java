package exprdb.store;

import java.io.*;
import java.util.*;
import java.util.function.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.nio.ByteBuffer;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import exprdb.store.proto.Database.*;
import exprdb.store.rsm.ZabException;

final class DataNode {
  private static Logger log = LogManager.getLogger(DataNode.class);
  private Store store;
  private boolean standalone;
  private List<Ring> rings;
  private Random rnd;

  public DataNode() {
    this.rings = Ring.configRings();
    this.store = Store.get();
    this.standalone = Config.get().getBoolean("standalone", false);
    this.rnd = new Random();
  }

  public String stats(String table) {
    return store.stats(table);
  }

  public boolean isMaster() {
    return ring().isLeader();
  }

  private Ring ring() {
    return rings.get(rnd.nextInt(rings.size()));
  }

  private void rsend(Message msg, Object ctx) {
    try {
      ring().zab.send(ByteBuffer.wrap(msg.toByteArray()), ctx);
    } catch(ZabException.InvalidPhase e) {
      throw new KdbException(e);
    } catch(ZabException.TooManyPendingRequests e) {
      throw new KdbException(e);
    }
  }

  public void process(Message msg, Object context) {
    Message r = MessageBuilder.nullMsg;
    String table;
    //log.info("msg {} context {} standalone {}", msg, context, standalone);
    switch(msg.getType()) {
    case Open:
      table = msg.getOpenOp().getTable();
      //log.info("server {} open table {} ", ring().serverid(), table);
      if(standalone) {
        r = store.open(msg.getOpenOp());
      } else {
        if(!store.tableOpened(table))
          rsend(msg, context);
        else
          r = MessageBuilder.emptyMsg;
      }
      break;
    case Compact:
      table = msg.getCompactOp().getTable();
      if(standalone) {
        r = store.compact(msg.getCompactOp());
      } else {
        rsend(msg, context);
      }
      break;
    case Drop:
      //log.info("msg {} context {}", msg, context);
      table = msg.getDropOp().getTable();
      if(standalone) {
        r = store.drop(msg.getDropOp());
      } else {
        rsend(msg, context);
      }
      break;
    case Get:
      r = store.get(msg.getGetOp());
      break;
    case Scan:
      r = store.scan(msg.getScanOp());
      break;
    case Sequence:
      r = store.seqno(msg.getSeqOp());
      break;
    case Scanlog:
      r = store.scanlog(msg.getScanlogOp());
      break;
    case Backup:
      r = store.backup(msg.getBackupOp());
      break;
    case Restore:
      r = store.restore(msg.getRestoreOp());
      break;
    case Put:
      if(standalone || isMaster()) {
        table = msg.getPutOp().getTable();
        r = store.update(msg.getPutOp());
        if(!standalone) {
          //log.info(" seq {}", r.getResponse().getSeqno());
          Message repl = MessageBuilder.buildSeqOp(table,
                                                   Transport.get().dataaddr,
                                                   r.getResponse().getSeqno());
          rsend(repl, context);
        }
      } else {
        //log.info("from {} ===> {}", Transport.get().dataaddr, ring().leaderd());
        Transport.redirect(context, ring().leaderd());
        return;
      }
      break;
    default:
      r = MessageBuilder.emptyMsg;
      break;
    }
    if(r != MessageBuilder.nullMsg) {
      Transport.reply(context, r);
    }
  }

}
