package rsm.agent;

import network.message.protocols.GenericPaxosMessage;

/**
 * @author : Swimiltylers
 * @version : 2019/2/14 17:37
 */
public interface Learner {
    void handleAckAccept(GenericPaxosMessage.ackAccept ackAccept);
    void handleCommit(GenericPaxosMessage.Commit commit);
}
