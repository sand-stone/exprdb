package exprdb.store.rsm;

import exprdb.store.rsm.Participant.LeftCluster;
import exprdb.store.rsm.proto.ZabMessage.Message.MessageType;
import com.google.protobuf.TextFormat;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 * MessageQueueFilter is a filter class, filter classes filter and handle the
 * specific messages they are interested in.
 * This is the most basic filter class, it only acts as the base class for other
 * filters. It only filters and handles SHUT_DOWN message, which is the
 * message for shutting down Jzab. For this message it simply throws a
 * LeftCluster exception.
 */
class MessageQueueFilter {

  private final BlockingQueue<MessageTuple> messageQueue;

  private static final Logger LOG =
    LogManager.getLogger(MessageQueueFilter.class);

  MessageQueueFilter(BlockingQueue<MessageTuple> messageQueue) {
    this.messageQueue = messageQueue;
  }

  /**
   * Takes the filtered message from message queue. This method will be blocked
   * until the message becomes available or timeout is detected.
   *
   * @param timeoutMs the timeout for blocking, in millisecond.
   * @return filtered message tuple.
   * @throws InterruptedException in case of interrupt on blocking.
   * @throws TimeoutException if timeout happens during blocking.
   */
  protected MessageTuple getMessage(int timeoutMs)
      throws InterruptedException, TimeoutException {
    MessageTuple tuple = messageQueue.poll(timeoutMs,
                                           TimeUnit.MILLISECONDS);
    if (tuple == null) {
      throw new TimeoutException("Timeout while waiting for the message.");
    }
    if (tuple.getMessage().getType() == MessageType.SHUT_DOWN) {
      // If it's SHUT_DOWN message.
      throw new LeftCluster("Left cluster");
    }
    return tuple;
  }

  /**
   * Takes the expected message from message queue. This method will be blocked
   * until the expected message from the expected source becomes
   * available or timeout is detected.
   *
   * @param type the expected message type.
   * @param source the expected source of the message, or null if the source
   * doesn't matter.
   * @param timeoutMs the timeout for blocking, in millisecond.
   * @return filtered message tuple.
   * @throws InterruptedException in case of interrupt on blocking.
   * @throws TimeoutException if timeout happens during blocking.
   */
  protected MessageTuple getExpectedMessage(MessageType type,
                                            String source,
                                            int timeoutMs)
      throws TimeoutException, InterruptedException {
    int startTime = (int) (System.nanoTime() / 1000000);
    // Waits until the expected message is received.
    while (true) {
      MessageTuple tuple = getMessage(timeoutMs);
      String from = tuple.getServerId();
      if (tuple.getMessage().getType() == type &&
          (source == null || source.equals(from))) {
        // Return message only if it's expected type and expected source.
        return tuple;
      } else {
        int curTime = (int) (System.nanoTime() / 1000000);
        if (curTime - startTime >= timeoutMs) {
          throw new TimeoutException("Timeout in getExpectedMessage.");
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Got an unexpected message from {}: {}",
                    tuple.getServerId(),
                    TextFormat.shortDebugString(tuple.getMessage()));
        }
      }
    }
  }
}
