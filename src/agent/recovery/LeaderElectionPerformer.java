package agent.recovery;

import network.message.protocols.LeaderElectionMessage;
import network.service.module.controller.LeaderElectionProvider;

/**
 * @author : Swimiltylers
 * @version : 2019/4/14 22:09
 */
public interface LeaderElectionPerformer {
    boolean onLeaderElection();
    boolean isLeaderSurvive(final int expire);

    void markFileSyncComplete(LeaderElectionResultUpdater updater);
    void markLeaderChosen(int chosen, LeaderElectionResultUpdater updater);

    void markLeaderElection(boolean isFsync, long token);
    void handleLEForce(LeaderElectionMessage.LEForce force, LeaderElectionResultUpdater updater);

    enum LeaderElectionState{
        COMPLETE, WAITING, RECOVERED, RECOVERING
    }
}
