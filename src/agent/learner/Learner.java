package agent.learner;

import network.message.protocols.GenericPaxosMessage;

/**
 * @author : Swimiltylers
 * @version : 2019/2/14 17:37
 */
public interface Learner {
    void handleAckAccept(GenericPaxosMessage.ackAccept ackAccept, CommitUpdater updater);
    void handleCommit(GenericPaxosMessage.Commit commit, CommitUpdater updater);
}
