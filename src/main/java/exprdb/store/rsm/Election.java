package exprdb.store.rsm;

/**
 * Interface for the leader election implementation.
 */
interface Election {
  /**
   * Starts one round leader election.
   *
   * @return the elected leader.
   * @throws Exception in case of exception.
   */
  String electLeader() throws Exception;

  /**
   * Replies its vote to peer. The leader/follower might receive election
   * message in non-electing phase, it ask Election object to reply its vote.
   *
   * @param tuple the message from the querier.
   */
  void reply(MessageTuple tuple);

  /**
   * Specifies the leader explicitly. If the server starts by joining the
   * cluster, it doesn't need to go through the leader election. However,
   * the other servers who go back to recovery phase might ask it about
   * the leader information. They can initialize them the knowledge about
   * leader explicitly once they join a cluster.
   */
  void specifyLeader(String leader);
}
