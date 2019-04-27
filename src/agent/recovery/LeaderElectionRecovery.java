package agent.recovery;

import javafx.util.Pair;
import logger.PaxosLogger;
import network.message.protocols.LeaderElectionMessage;
import network.service.module.connection.ConnectionModule;
import network.service.module.controller.LeaderElectionProvider;
import network.service.sender.PeerMessageSender;

import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author : Swimiltylers
 * @version : 2019/4/16 20:42
 */
public class LeaderElectionRecovery implements LeaderElectionPerformer{
    protected int serverId;
    protected int peerSize;

    protected ConnectionModule conn;
    protected PeerMessageSender sender;

    private AtomicReference<Pair<Long, Integer>> leaderIdPair;
    private AtomicReference<LeaderElectionState> state;

    private AtomicLong leToken;
    private PaxosLogger logger;

    public LeaderElectionRecovery(int serverId, int peerSize, int leaderId, PeerMessageSender sender, ConnectionModule conn, PaxosLogger logger) {
        this.serverId = serverId;
        this.peerSize = peerSize;
        this.conn = conn;
        this.sender = sender;
        this.leaderIdPair = new AtomicReference<>(new Pair<>(0L, leaderId));
        this.logger = logger;

        state = new AtomicReference<>(LeaderElectionState.COMPLETE);
        leToken = new AtomicLong();

    }

    @Override
    public boolean onLeaderElection() {
        return state.get() != LeaderElectionState.COMPLETE;
    }

    @Override
    public boolean isLeaderSurvive(int expire) {
        int leader = leaderIdPair.get().getValue();
        if (leader == serverId)
            return true;
        else
            return conn.survive(leader, expire);
    }

    private void completeSequence(LeaderElectionResultUpdater updater){
        Pair<Long, Integer> leader = leaderIdPair.get();
        updater.update(leader.getKey(), leader.getValue());
        logger.record(false, "diag", "[" + System.currentTimeMillis() + "][leader election][COMPLETE, chosen=" + leader.getValue() + "]\n");
    }

    @Override
    public void markFileSyncComplete(LeaderElectionResultUpdater updater) {
        LeaderElectionState formerState = state.getAndUpdate(oldState -> {
            if (oldState == LeaderElectionState.RECOVERING)
                return LeaderElectionState.RECOVERED;
            else if (oldState == LeaderElectionState.WAITING)
                return LeaderElectionState.COMPLETE;
            else
                return oldState;
        });

        if (formerState == LeaderElectionState.WAITING){
            completeSequence(updater);
        }
    }

    @Override
    public void markLeaderChosen(int chosen, LeaderElectionResultUpdater updater) {
        if (state.get() != LeaderElectionState.COMPLETE){
            leaderIdPair.set(new Pair<>(leToken.get(), chosen));

            LeaderElectionState newState = state.updateAndGet(oldState -> {
                if (oldState == LeaderElectionState.RECOVERED)
                    return LeaderElectionState.COMPLETE;
                else if (oldState == LeaderElectionState.RECOVERING) {
                    if (chosen != serverId)
                        return LeaderElectionState.COMPLETE;
                    else
                        return LeaderElectionState.WAITING;
                }
                else
                    return oldState;
            });

            if (newState == LeaderElectionState.COMPLETE) {
                completeSequence(updater);
            }
        }
    }

    @Override
    public void markLeaderElection(boolean isFsync, long leToken) {
        this.leToken.set(leToken);
        state.set(isFsync ? LeaderElectionState.RECOVERING : LeaderElectionState.RECOVERED);
    }

    @Override
    public void handleLEForce(LeaderElectionMessage.LEForce force, LeaderElectionResultUpdater updater) {
        if (leToken.get() == force.token){
            markLeaderChosen(force.leaderId, updater);
        }
    }
}
