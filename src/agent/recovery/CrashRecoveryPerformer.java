package agent.recovery;

import agent.learner.CommitUpdater;
import network.message.protocols.GenericPaxosMessage;

/**
 * @author : Swimiltylers
 * @version : 2019/4/13 22:31
 */
public interface CrashRecoveryPerformer {
    void handleSync(GenericPaxosMessage.Sync sync);
    void handleAckSync(GenericPaxosMessage.ackSync ackSync, CommitUpdater cUpdater);
}
