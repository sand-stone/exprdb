package exprdb.store.rsm;

import java.util.List;
import java.util.LinkedList;
import java.util.Collections;

/**
 * Stores different kinds of pending requests.
 */
public class PendingRequests {
  PendingRequests() {}

  /**
   * The pending send requests.
   *
   * The first element of the tuple is the request sent, the second element is
   * the corresponding context
   */
  public final List<Tuple> pendingSends= Collections.synchronizedList(new LinkedList<>());

  /**
   * The pending flush requests.
   * The first element of the tuple is flush request, the second element is ctx.
   */
  public final List<Tuple> pendingFlushes = Collections.synchronizedList(new LinkedList<>());

  /**
   * The pending remove requests.
   * The first element of tuple is serverId, the second element is ctx.
   */
  public final List<Tuple> pendingRemoves = Collections.synchronizedList(new LinkedList<>());

  /**
   * The pending snapshot requests.
   */
  public final List<Object> pendingSnapshots = Collections.synchronizedList(new LinkedList<>());

  /**
   * The tuple holds both request and ctx.
   */
  public static class Tuple {
    public final Object param;
    public final Object ctx;

    Tuple(Object param, Object ctx) {
      this.param = param;
      this.ctx = ctx;
    }
  }
}
