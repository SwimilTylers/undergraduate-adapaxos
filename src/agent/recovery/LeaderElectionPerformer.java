package agent.recovery;

import network.message.protocols.LeaderElectionMessage;

/**
 * @author : Swimiltylers
 * @version : 2019/4/14 22:09
 */
public interface LeaderElectionPerformer {
    boolean isLeaderSurvive(final int expire, final int decisionDelay);

    void prepareForLeaderElection(LeaderElectionState state, long token);
    void tryLeaderElection(int ticket);

    void handleLEPropaganda(LeaderElectionMessage.Propaganda propaganda);
    void handleLEVote(LeaderElectionMessage.Vote vote);

    void initLESync();
    void handleLESync(LeaderElectionMessage.LESync leSync);

    enum LeaderElectionState{
        COMPLETE, RECOVERED, RECOVERING
    }
}
