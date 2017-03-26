package exprdb.store;

public class KdbException extends RuntimeException {

  public KdbException() {
    super();
  }

  public KdbException(String msg) {
    super(msg);
  }

  public KdbException(Exception e) {
    super(e);
  }

}
