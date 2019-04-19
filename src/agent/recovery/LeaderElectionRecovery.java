package agent.recovery;

import javafx.util.Pair;
import logger.PaxosLogger;
import network.message.protocols.LeaderElectionMessage;
import network.service.module.ConnectionModule;
import network.service.sender.PeerMessageSender;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
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

    private int leTicket;
    private long leToken;
    private int leCount;

    private AtomicInteger maxRecvInstance;
    private int[] tickets;

    private PaxosLogger logger;

    public LeaderElectionRecovery(int serverId, int peerSize, int leaderId, AtomicInteger maxRecvInstance, PeerMessageSender sender, ConnectionModule conn, PaxosLogger logger) {
        this.serverId = serverId;
        this.peerSize = peerSize;
        this.conn = conn;
        this.sender = sender;
        this.leaderIdPair = new AtomicReference<>(new Pair<>(0L, leaderId));
        this.maxRecvInstance = maxRecvInstance;
        this.logger = logger;

        state = new AtomicReference<>(LeaderElectionState.COMPLETE);
        tickets = new int[peerSize];
        Arrays.fill(tickets, 0);
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

    @Override
    public void stateSet(LeaderElectionState set) {
        state.set(set);
    }

    @Override
    public boolean stateCompareAndSet(LeaderElectionState expect, LeaderElectionState set) {
        return state.compareAndSet(expect, set);
    }

    @Override
    public void handleLEStart(LeaderElectionMessage.LeStart leStart) {
        if (state.compareAndSet(LeaderElectionState.RECOVERED, LeaderElectionState.ON_RUNNING) && serverId == leStart.fromId){    // check if LLE prepared
            leTicket = leStart.LeTicket_local;
            leToken = leStart.LeDialog_no;
            leCount = 0;
            tickets[serverId] = leStart.LeTicket_local;

            sender.broadcastPeerMessage(new LeaderElectionMessage.Propaganda(serverId, leToken, tickets));
            logger.record(false, "diag", "[" + System.currentTimeMillis() + "][leader election][start][ON_RUNNING, token="+leToken+"]\n");
        }
    }

    @Override
    public void handleLEPropaganda(LeaderElectionMessage.Propaganda propaganda) {
        LeaderElectionState s = state.get();
        if (s != LeaderElectionState.RECOVERING){
            tickets[serverId] = s == LeaderElectionState.COMPLETE ? maxRecvInstance.get() : leTicket;
            for (int i = 0; i < tickets.length; i++) {
                tickets[i] = Integer.max(tickets[i], propaganda.tickets[i]);
            }
            logger.record(false, "diag", "[" + System.currentTimeMillis() + "][leader election]["+propaganda.toString()+"]\n");
            sender.sendPeerMessage(propaganda.fromId, new LeaderElectionMessage.Vote(serverId, propaganda.token, tickets));
        }
    }

    @Override
    public void handleLEVote(LeaderElectionMessage.Vote vote, LeaderElectionResultUpdater updater) {
        if (state.get() == LeaderElectionState.ON_RUNNING && leToken == vote.token){

            ++leCount;

            for (int i = 0; i < tickets.length; i++) {
                tickets[i] = Integer.max(tickets[i], vote.tickets[i]);
            }

            logger.record(false, "diag", "[" + System.currentTimeMillis() + "][leader election]["+vote.toString()+"]\n");

            if (leCount >= (peerSize + 1)/2 - 1){     // bare minority
                int formalLeader = leaderIdPair.get().getValue();
                int maxTicket = tickets[formalLeader == 0 ? 1 : 0];
                int correspondingServer = formalLeader == 0 ? 1 : 0;

                for (int i = correspondingServer + 1; i < tickets.length; i++) {
                    if (i != formalLeader && tickets[i] > maxTicket){
                        maxTicket = tickets[i];
                        correspondingServer = i;
                    }
                }

                leaderIdPair.set(new Pair<>(leToken, correspondingServer));

                logger.record(false, "diag", "[" + System.currentTimeMillis() + "][leader election][COMPLETE, chosen="+correspondingServer+"]\n");
                updater.update(leToken, correspondingServer);

                state.set(LeaderElectionState.COMPLETE);
            }


        }
    }
}
