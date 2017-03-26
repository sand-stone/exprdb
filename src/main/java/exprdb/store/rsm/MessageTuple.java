package exprdb.store.rsm;

import exprdb.store.rsm.proto.ZabMessage.Message;

/**
 * Class that holds both message and source/destionation.
 */
class MessageTuple {
  private final String serverId;
  private final Message message;
  private Zxid zxid;

  public static final MessageTuple REQUEST_OF_DEATH =
      new MessageTuple(null, null);

  public MessageTuple(String serverId, Message message) {
    this(serverId, message, null);
  }

  public MessageTuple(String serverId, Message message, Zxid zxid) {
    this.serverId = serverId;
    this.message = message;
    this.zxid = zxid;
  }

  public String getServerId() {
    return this.serverId;
  }

  public Message getMessage() {
    return this.message;
  }

  public void setZxid(Zxid z) {
    this.zxid = z;
  }

  public Zxid getZxid() {
    return this.zxid;
  }
}
