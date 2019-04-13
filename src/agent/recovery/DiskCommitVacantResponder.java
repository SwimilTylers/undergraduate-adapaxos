package agent.recovery;

import agent.learner.CommitUpdater;
import network.message.protocols.DiskPaxosMessage;

/**
 * @author : Swimiltylers
 * @version : 2019/4/13 22:25
 */
public interface DiskCommitVacantResponder {
    boolean isValidMessage(int inst_no, long token);
    boolean respond_ackRead(DiskPaxosMessage.ackRead ackRead, CommitUpdater cUpdater, VacantInstanceUpdater vUpdater);
}
