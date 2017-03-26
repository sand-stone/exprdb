package exprdb.store.rsm;

/**
 * Exceptions thrown by {@link Zab}.
 */
public abstract class ZabException extends Exception {

  ZabException() {}

  ZabException(String desc) { super(desc); }

  /**
   * {@link Zab} is in an invalid phase to perform a certain operation.
   */
  public static class InvalidPhase extends ZabException {
    InvalidPhase() {}
    InvalidPhase(String desc) { super(desc); }
  }

  /**
   * Thrown when there are too many pending requests.
   */
  public static class TooManyPendingRequests extends ZabException {
    TooManyPendingRequests() {}
    TooManyPendingRequests(String desc) { super(desc); }
  }
}
