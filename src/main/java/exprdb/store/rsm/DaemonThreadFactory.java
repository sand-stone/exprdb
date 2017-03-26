package exprdb.store.rsm;

import java.util.concurrent.ThreadFactory;

/**
 * The factory for creating daemon thread.
 */
final class DaemonThreadFactory implements ThreadFactory {
  public static final DaemonThreadFactory FACTORY = new DaemonThreadFactory();

  private DaemonThreadFactory() {
    // Singleton instance.
  }

  @Override
  public Thread newThread(Runnable r) {
    Thread thread = new Thread(r);
    thread.setDaemon(true);
    return thread;
  }
}
