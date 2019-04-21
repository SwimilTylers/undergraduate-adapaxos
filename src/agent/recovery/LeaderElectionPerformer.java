package agent.recovery;

import network.message.protocols.LeaderElectionMessage;

/**
 * @author : Swimiltylers
 * @version : 2019/4/14 22:09
 */
public interface LeaderElectionPerformer {
    boolean onLeaderElection();
    boolean isLeaderSurvive(final int expire);
    void stateSet(LeaderElectionState set);
    boolean stateCompareAndSet(LeaderElectionState expect, LeaderElectionState set);

    void handleResidualLEMessages(long restartExpire);

    void handleLEStart(LeaderElectionMessage.LeStart leStart);
    void handleLEPropaganda(LeaderElectionMessage.Propaganda propaganda);
    void handleLEVote(LeaderElectionMessage.Vote vote, LeaderElectionResultUpdater updater);

    enum LeaderElectionState{
        COMPLETE, ON_RUNNING, RECOVERED, RECOVERING
    }
}
