package exprdb.store.rsm;

import exprdb.store.rsm.proto.ZabMessage.Message.MessageType;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

/**
 * ElectionMessageFilter is a filter class which acts as a successor of
 * MessageQueueFilter. It handles and filters ELECTION_INFO message, which will
 * be passed to its Election object.
 */
class ElectionMessageFilter extends MessageQueueFilter {
  private final Election election;

  ElectionMessageFilter(BlockingQueue<MessageTuple> messageQueue,
                        Election election) {
    super(messageQueue);
    this.election = election;
  }

  @Override
  protected MessageTuple getMessage(int timeoutMs)
      throws InterruptedException, TimeoutException {
    int startMs = (int)(System.nanoTime() / 1000000);
    while (true) {
      int nowMs = (int)(System.nanoTime() / 1000000);
      int remainMs = timeoutMs - (nowMs - startMs);
      if (remainMs < 0) {
        remainMs = 0;
      }
      MessageTuple tuple = super.getMessage(remainMs);
      if (tuple.getMessage().getType() == MessageType.ELECTION_INFO) {
        this.election.reply(tuple);
      } else {
        return tuple;
      }
    }
  }
}
