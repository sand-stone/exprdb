package exprdb.store.rsm;

import java.nio.ByteBuffer;
import static kdb.rsm.proto.ZabMessage.Proposal.ProposalType;

/**
 * Transaction.
 */
class Transaction {
  private final Zxid zxid;
  private final ByteBuffer body;
  private final int type;

  public Transaction(Zxid zxid, ByteBuffer body) {
    this(zxid, ProposalType.USER_REQUEST_VALUE, body);
  }

  public Transaction(Zxid zxid, int type, ByteBuffer body) {
    this.zxid = zxid;
    this.type = type;
    this.body = body;
  }

  /**
   * Get the id of this transaction.
   *
   * @return id of the transaction
   */
  public Zxid getZxid() {
    return this.zxid;
  }

  /**
   * Get the body of the transaction.
   *
   * @return an array of bytes representing the body of the transaction
   */
  public ByteBuffer getBody() {
    return this.body;
  }

  @Override
  public String toString() {
    return this.zxid + " : " + this.body;
  }

  public int getType() {
    return this.type;
  }
}
