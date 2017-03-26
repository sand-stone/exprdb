package exprdb.store.rsm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.Properties;
import exprdb.store.rsm.proto.ZabMessage;
import exprdb.store.rsm.proto.ZabMessage.Proposal.ProposalType;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 * Configurations(ensemble of cluster, serverId, version).
 */
class ClusterConfiguration implements Cloneable {
  private Zxid version;
  private final Set<String> peers;
  private final String serverId;

  private static final Logger LOG =
      LogManager.getLogger(ClusterConfiguration.class);

  public ClusterConfiguration(Zxid version,
                              Collection<String> peers,
                              String serverId) {
    this.version = version;
    this.peers = new HashSet<String>(peers);
    this.serverId = serverId;
  };

  public Zxid getVersion() {
    return this.version;
  }

  public void setVersion(Zxid newVersion) {
    this.version = newVersion;
  }

  public Set<String> getPeers() {
    return this.peers;
  }

  public String getServerId() {
    return this.serverId;
  }

  public void addPeer(String peer) {
    peers.add(peer);
  }

  public void removePeer(String peer) {
    this.peers.remove(peer);
  }

  public Properties toProperties() {
    Properties prop = new Properties();
    StringBuilder strBuilder = new StringBuilder();
    String strVersion = this.version.toSimpleString();
    if (!this.peers.isEmpty()) {
      String[] peersArray = this.peers.toArray(new String[this.peers.size()]);
      strBuilder.append(peersArray[0]);
      for (int i = 1; i < peersArray.length; ++i) {
        strBuilder.append("," + peersArray[i]);
      }
    }
    prop.setProperty("peers", strBuilder.toString());
    prop.setProperty("version", strVersion);
    prop.setProperty("serverId", this.serverId);
    return prop;
  }

  public static ClusterConfiguration fromProperties(Properties prop) {
    String strPeers = prop.getProperty("peers");
    Zxid version = Zxid.fromSimpleString(prop.getProperty("version"));
    String serverId = prop.getProperty("serverId");
    Set<String> peerSet;
    if (strPeers.equals("")) {
      peerSet = new HashSet<String>();
    } else {
      peerSet = new HashSet<String>(Arrays.asList(strPeers.split(",")));
    }
    return new ClusterConfiguration(version, peerSet, serverId);
  }

  public static
  ClusterConfiguration fromProto(ZabMessage.ClusterConfiguration cnf,
                                 String serverId) {
    Zxid version = MessageBuilder.fromProtoZxid(cnf.getVersion());
    return new ClusterConfiguration(version, cnf.getServersList(), serverId);
  }

  public ZabMessage.ClusterConfiguration toProto() {
    return MessageBuilder.buildConfig(this);
  }

  public ByteBuffer toByteBuffer() {
    return ByteBuffer.wrap(toProto().toByteArray());
  }

  public static ClusterConfiguration fromByteBuffer(ByteBuffer buffer,
                                                    String serverId)
      throws IOException {
    byte[] bufArray = new byte[buffer.remaining()];
    buffer.get(bufArray);
    return fromProto(ZabMessage.ClusterConfiguration.parseFrom(bufArray),
                     serverId);
  }

  public Transaction toTransaction() {
    ByteBuffer cop = this.toByteBuffer();
    Transaction txn = new Transaction(version, ProposalType.COP_VALUE, cop);
    return txn;
  }

  public boolean contains(String peerId) {
    return this.peers.contains(peerId);
  }

  @Override
  public String toString() {
    return toProperties().toString();
  }

  /**
   * Gets the minimal quorum size.
   */
  public int getQuorumSize() {
    int clusterSize = this.getPeers().size();
    if (clusterSize == 0) {
      return 0;
    } else {
      return clusterSize / 2 + 1;
    }
  }

  public ClusterConfiguration clone() {
    return new ClusterConfiguration(version, peers, serverId);
  }
}
