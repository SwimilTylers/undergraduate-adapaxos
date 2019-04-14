package agent.recovery;

import network.message.protocols.LeaderElectionMessage;

/**
 * @author : Swimiltylers
 * @version : 2019/4/14 22:09
 */
public interface LeaderElectionPerformer {
    boolean isLeaderSurvive();

    void readyForLeaderElection();
    boolean initLeaderElection(long token);
    boolean initLeaderElection(long token, int waitingTime);

    void handleLEPropaganda(LeaderElectionMessage.Propaganda propaganda);
    void handleLEVote(LeaderElectionMessage.Vote vote);
}
