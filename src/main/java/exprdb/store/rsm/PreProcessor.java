package exprdb.store.rsm;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.HashMap;
import java.util.Map;
import exprdb.store.rsm.proto.ZabMessage;
import exprdb.store.rsm.proto.ZabMessage.Message;
import exprdb.store.rsm.proto.ZabMessage.Message.MessageType;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 * This processor is used to let clients to convert requests into transaction
 * and hands the transaction to BroadcastProcessor to broadcast to all peers.
 */
class PreProcessor implements RequestProcessor, Callable<Void> {

  private final BlockingQueue<MessageTuple> requestQueue =
      new LinkedBlockingQueue<MessageTuple>();

  private static final Logger LOG =
      LogManager.getLogger(PreProcessor.class);

  private final StateMachine stateMachine;

  private final Map<String, PeerHandler> quorumMapOriginal;

  private final Map<String, PeerHandler> quorumMap;

  private final Future<Void> ft;

  private ClusterConfiguration clusterConfig;

  public PreProcessor(StateMachine stateMachine,
                      Map<String, PeerHandler> quorumMap,
                      ClusterConfiguration config) {
    this.stateMachine = stateMachine;
    this.quorumMapOriginal = quorumMap;
    this.quorumMap = new HashMap<String, PeerHandler>(quorumMap);
    this.clusterConfig = config.clone();
    ExecutorService es =
        Executors.newSingleThreadExecutor(DaemonThreadFactory.FACTORY);
    ft = es.submit(this);
    es.shutdown();
  }

  @Override
  public void processRequest(MessageTuple request) {
    this.requestQueue.add(request);
  }

  private void addToQuorumSet(String serverId) {
    PeerHandler ph = this.quorumMapOriginal.get(serverId);
    if (ph != null) {
      this.quorumMap.put(serverId, ph);
    }
  }

  private void removeFromQuorumSet(String serverId) {
    this.quorumMap.remove(serverId);
  }

  @Override
  public Void call() throws Exception {
    LOG.debug("PreProcessor gets started.");
    try {
      while (true) {
        MessageTuple request = this.requestQueue.take();
        if (request == MessageTuple.REQUEST_OF_DEATH) {
          break;
        }
        Message msg = request.getMessage();
        String source = request.getServerId();
        Zxid zxid = request.getZxid();
        if (msg.getType() == MessageType.REQUEST) {
          ZabMessage.Request req = request.getMessage().getRequest();
          String clientId = request.getServerId();
          ByteBuffer bufReq = req.getRequest().asReadOnlyByteBuffer();
          // Invoke the callback to convert the request into transaction.
          ByteBuffer update = this.stateMachine.preprocess(zxid,
                                                           bufReq);
          Transaction txn = new Transaction(zxid, update);
          Message prop = MessageBuilder.buildProposal(txn, clientId);
          for (PeerHandler ph : quorumMap.values()) {
            ph.queueMessage(prop);
          }
        } else if (msg.getType() == MessageType.JOIN) {
          LOG.debug("Got JOIN from {}.", source);
          if (!clusterConfig.contains(source)) {
            clusterConfig.addPeer(source);
          }
          clusterConfig.setVersion(zxid);
          Message prop =
            MessageBuilder.buildProposal(clusterConfig.toTransaction());
          // Broadcasts COP.
          for (PeerHandler ph : quorumMap.values()) {
            ph.queueMessage(prop);
          }
          // Adds it to quorum set.
          addToQuorumSet(source);
        } else if (msg.getType() == MessageType.ACK_EPOCH) {
          LOG.debug("Got ACK_EPOCH from {}.", source);
          addToQuorumSet(source);
        } else if (msg.getType() == MessageType.REMOVE) {
          String peerId = msg.getRemove().getServerId();
          String clientId = request.getServerId();
          LOG.debug("Got REMOVE for {}.", peerId);
          clusterConfig.removePeer(peerId);
          clusterConfig.setVersion(zxid);
          Message prop =
            MessageBuilder.buildProposal(clusterConfig.toTransaction(),
                                         clientId);
          // Broadcasts COP.
          for (PeerHandler ph : quorumMap.values()) {
            ph.queueMessage(prop);
          }
          // Removes it from quorum set.
          removeFromQuorumSet(peerId);
        } else if (msg.getType() == MessageType.DISCONNECTED) {
          String peerId = msg.getDisconnected().getServerId();
          LOG.debug("Got DISCONNECTED from {}.", peerId);
          removeFromQuorumSet(peerId);
        } else {
          LOG.warn("Got unexpected Message.");
        }
      }
    } catch (RuntimeException e) {
      LOG.error("Caught exception in PreProcessor!", e);
      throw e;
    }
    LOG.debug("PreProcessor has been shut down.");
    return null;
  }

  @Override
  public void shutdown() throws InterruptedException, ExecutionException {
    this.requestQueue.add(MessageTuple.REQUEST_OF_DEATH);
    this.ft.get();
  }
}
