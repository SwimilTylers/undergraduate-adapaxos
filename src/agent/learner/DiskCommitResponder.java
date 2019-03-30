package agent.learner;

import network.message.protocols.DiskPaxosMessage;

/**
 * @author : Swimiltylers
 * @version : 2019/3/24 21:39
 */
public interface DiskCommitResponder {
    boolean isValidMessage(int inst_no, long token);
    boolean respond_ackWrite(DiskPaxosMessage.ackWrite ackWrite, CommitUpdater updater);
    boolean respond_ackRead(DiskPaxosMessage.ackRead ackRead, CommitUpdater updater);
}
