package exprdb.store.rsm;

import java.util.concurrent.ExecutionException;

/**
 * Interface for request processor. Different processors are chained together
 * to process request in order.
 */
interface RequestProcessor {
  /**
   * This function should be asynchronous normally, the request should be
   * processed in separate thread.
   *
   * @param request the request object.
   */
  void processRequest(MessageTuple request);

  /**
   * Shutdown the processor.
   *
   * @throws ExecutionException in case of ExecutionException in Processors.
   * @throws InterruptedException in case of interruption.
   */
  void shutdown() throws InterruptedException, ExecutionException;
}
