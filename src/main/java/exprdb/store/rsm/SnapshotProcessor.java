package exprdb.store.rsm;

import com.google.protobuf.TextFormat;
import java.io.File;
import java.io.FileOutputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import exprdb.store.rsm.proto.ZabMessage.Message;
import exprdb.store.rsm.proto.ZabMessage.Message.MessageType;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 * The processor which is responsible for taking the snapshot of application
 * when it's required.
 */
class SnapshotProcessor implements RequestProcessor, Callable<Void> {
  private final BlockingQueue<MessageTuple> requestQueue =
      new LinkedBlockingQueue<MessageTuple>();

  private static final Logger LOG =
      LogManager.getLogger(SnapshotProcessor.class);

  private final StateMachine stateMachine;

  private final PersistentState persistence;

  private final Transport transport;

  private final String serverId;

  Future<Void> ft;

  public SnapshotProcessor(StateMachine stateMachine,
                           PersistentState persistence,
                           String serverId,
                           Transport transport) {
    this.stateMachine = stateMachine;
    this.persistence = persistence;
    this.serverId = serverId;
    this.transport = transport;
    ExecutorService es =
        Executors.newSingleThreadExecutor(DaemonThreadFactory.FACTORY);
    ft = es.submit(this);
    es.shutdown();
  }

  @Override
  public void processRequest(MessageTuple request) {
    this.requestQueue.add(request);
  }

  @Override
  public void shutdown() throws InterruptedException, ExecutionException {
    this.requestQueue.add(MessageTuple.REQUEST_OF_DEATH);
    this.ft.get();
    LOG.debug("SnapshotProcessor has been shut down.");
  }

  @Override
  public Void call() throws Exception {
    LOG.debug("SnapshotProcessor gets started.");
    try {
      while (true) {
        MessageTuple request = requestQueue.take();
        if (request == MessageTuple.REQUEST_OF_DEATH) {
          break;
        }
        Message msg = request.getMessage();
        if (msg.getType() == MessageType.SNAPSHOT) {
          Zxid zxid =
            MessageBuilder.fromProtoZxid(msg.getSnapshot().getLastZxid());
          LOG.debug("Got SNAPSHOT, the zxid of last transaction which is " +
              "guaranteed in log is {}.", zxid);
          // Create a temporary file for snapshot.
          File temp = persistence.createTempFile("snapshot");
          try (FileOutputStream fout = new FileOutputStream(temp)) {
            stateMachine.save(fout);
            fout.close();
            // Mark it valid.
            File file = persistence.setSnapshotFile(temp, zxid);
            Message done = MessageBuilder.buildSnapshotDone(file.getPath());
            // Sends it back to main thread.
            this.transport.send(this.serverId, done);
          }
        } else {
          if (LOG.isWarnEnabled()) {
            LOG.warn("Got unexpected message {}.",
                     TextFormat.shortDebugString(msg));
          }
        }
      }
    } catch (RuntimeException e) {
      LOG.error("Caught exception", e);
      throw e;
    }
    return null;
  }
}
