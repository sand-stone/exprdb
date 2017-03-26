package exprdb.store;

import java.nio.*;
import java.io.*;
import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.*;
import static java.util.stream.Collectors.toList;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.concurrent.*;
import java.time.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;
import org.rocksdb.*;
import exprdb.store.proto.Database.*;
import exprdb.store.proto.Database.Message.MessageType;
import com.google.common.hash.Hashing;
import com.google.common.hash.HashFunction;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

class KQueue implements Closeable {
  private static Logger log = LogManager.getLogger(KQueue.class);
  private static KQueue instance = new KQueue();
  Object[] queues;
  HashFunction hfunc;
  static final int MAX_PENDING_REQS = 100000;
  Thread[] workers;

  private KQueue() {
    workers = new Thread[23];
    queues = new Object[workers.length];
    for(int i = 0; i < workers.length; i++) {
      queues[i] = new LinkedBlockingQueue<>(MAX_PENDING_REQS);
      workers[i] = new Thread(new Updater(i));
      workers[i].start();
    }
    hfunc = Hashing.crc32();
  }

  public void close() {}

  public static KQueue get() {
    return instance;
  }

  public void add(SequenceOperation op) {
    try {
      String table = op.getTable();
      int hashcode =  hfunc.hashUnencodedChars(table).hashCode();
      int bucket = Math.abs(hashcode) % queues.length;
      LinkedBlockingQueue<SequenceOperation> queue = (LinkedBlockingQueue<SequenceOperation>)queues[bucket];
      queue.put(op);
    } catch(InterruptedException e) {
      log.info("missed {}", op);
    }
  }

  private class Updater implements Runnable {
    Client client;
    LinkedBlockingQueue<SequenceOperation> queue;

    public Updater(int bucket) {
      client = new Client();
      this.queue = (LinkedBlockingQueue<SequenceOperation>)queues[bucket];
    }

    public void run() {
      while(true) {
        try {
          SequenceOperation op = queue.take();
          if(queue.size() > 0) {
            int count = queue.size();
            while(count--> 0) {
              op = queue.take();
            }
          }
          long seqno = op.getSeqno();
          Store.DataTable dt = Store.get().tables.get(op.getTable());
          long lsn = dt.db.getLatestSequenceNumber();
          lsn++;
          while (seqno > lsn) {
            int delta = (int)(seqno - lsn);
            int limit = delta < 1000? delta : 1000;
            Client.Result rsp = client.scanlog("http://"+op.getEndpoint(), op.getTable(), lsn, limit);
            //log.info("target {} fetch wal table {} rsp count {} seqno {}", seqno, op.getTable(), rsp.count(), rsp.seqno());
            lsn = Math.max(Store.get().update(op.getTable(), rsp), rsp.seqno() + 1);
            //log.info("table {} seqno {} lsn {}", op.getTable(), seqno, lsn);
          }
        } catch(Exception e) {
          e.printStackTrace();
          //log.info("failed to reach master {} exception: {}", op.getEndpoint(), e);
        }
      }
    }
  }

}
