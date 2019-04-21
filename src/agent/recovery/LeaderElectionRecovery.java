package agent.recovery;

import javafx.util.Pair;
import logger.PaxosLogger;
import network.message.protocols.LeaderElectionMessage;
import network.service.module.connection.ConnectionModule;
import network.service.sender.PeerMessageSender;

import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
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

    private Queue<LeaderElectionMessage> residual;

    private int leTicket;
    private long leToken;
    private int leCount;
    private int[] leVotes;

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
        leVotes = new int[peerSize];
        Arrays.fill(leVotes, 0);

        residual = new ConcurrentLinkedQueue<>();
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
    public void handleResidualLEMessages(long restartExpire) {
        LeaderElectionState s = state.get();
        if (s != LeaderElectionState.RECOVERING) {
            while (!residual.isEmpty()) {
                LeaderElectionMessage msg = residual.poll();
                if (msg instanceof LeaderElectionMessage.Propaganda)
                    handleLEPropagandaInternal(s, (LeaderElectionMessage.Propaganda) msg);
            }
            if (s == LeaderElectionState.ON_RUNNING){
                long crt = System.currentTimeMillis();
                if (crt - leToken > restartExpire){
                    leToken = crt;
                    sender.broadcastPeerMessage(new LeaderElectionMessage.Propaganda(serverId, leToken, tickets));
                    logger.record(false, "diag", "[" + System.currentTimeMillis() + "][leader election][restart][ON_RUNNING, token="+leToken+"]\n");

                }
            }
        }
    }

    @Override
    public void handleLEStart(LeaderElectionMessage.LeStart leStart) {
        if (state.compareAndSet(LeaderElectionState.RECOVERED, LeaderElectionState.ON_RUNNING) && serverId == leStart.fromId){    // check if LLE prepared
            leTicket = leStart.LeTicket_local;
            leToken = leStart.LeDialog_no;
            leCount = 0;
            Arrays.fill(leVotes, 0);
            tickets[serverId] = leStart.LeTicket_local;

            sender.broadcastPeerMessage(new LeaderElectionMessage.Propaganda(serverId, leToken, tickets));
            logger.record(false, "diag", "[" + System.currentTimeMillis() + "][leader election][start][ON_RUNNING, token="+leToken+"]\n");
        }
    }

    @Override
    public void handleLEPropaganda(LeaderElectionMessage.Propaganda propaganda) {
        LeaderElectionState s = state.get();
        if (s != LeaderElectionState.RECOVERING){
            handleLEPropagandaInternal(s, propaganda);
            while (!residual.isEmpty()) {
                LeaderElectionMessage msg = residual.poll();
                if (msg instanceof LeaderElectionMessage.Propaganda)
                    handleLEPropagandaInternal(s, (LeaderElectionMessage.Propaganda) msg);
            }
        }
        else {
            logger.record(false, "diag", "[" + System.currentTimeMillis() + "][leader election][residual]["+propaganda.toString()+"]\n");
            residual.offer(propaganda);
        }
    }

    private void handleLEPropagandaInternal(LeaderElectionState crtState, LeaderElectionMessage.Propaganda propaganda){
        tickets[serverId] = crtState == LeaderElectionState.COMPLETE ? maxRecvInstance.get() : leTicket;
        for (int i = 0; i < tickets.length; i++) {
            tickets[i] = Integer.max(tickets[i], propaganda.tickets[i]);
        }
        int[] votes = new int[peerSize];
        Arrays.fill(votes, 0);
        if (crtState == LeaderElectionState.COMPLETE)
            votes[leaderIdPair.get().getValue()] = 1;
        logger.record(false, "diag", "[" + System.currentTimeMillis() + "][leader election]["+propaganda.toString()+"]\n");
        sender.sendPeerMessage(propaganda.fromId, new LeaderElectionMessage.Vote(serverId, propaganda.token, tickets, votes));
    }

    @Override
    public void handleLEVote(LeaderElectionMessage.Vote vote, LeaderElectionResultUpdater updater) {
        if (state.get() == LeaderElectionState.ON_RUNNING && leToken == vote.token){

            ++leCount;

            for (int i = 0; i < tickets.length; i++) {
                tickets[i] = Integer.max(tickets[i], vote.tickets[i]);
                leVotes[i] = leVotes[i] + vote.votes[i];
            }

            logger.record(false, "diag", "[" + System.currentTimeMillis() + "][leader election]["+vote.toString()+"]\n");

            if (vote.fromId == leaderIdPair.get().getValue()){
                leaderIdPair.set(new Pair<>(leToken, vote.fromId));

                logger.record(false, "diag", "[" + System.currentTimeMillis() + "][leader election][COMPLETE, chosen="+vote.fromId+"]\n");
                updater.update(leToken, vote.fromId);

                state.set(LeaderElectionState.COMPLETE);
            }
            else {
                if (leCount >= (peerSize + 1)/2 - 1){     // bare minority
                    int formalLeader = leaderIdPair.get().getValue();
                    int maxTicket = tickets[formalLeader == 0 ? 1 : 0];
                    int correspondingServer = formalLeader == 0 ? 1 : 0;

                    for (int i = correspondingServer + 1; i < tickets.length; i++) {
                        if (i != formalLeader){
                            if (tickets[i] > maxTicket) {
                                maxTicket = tickets[i];
                                correspondingServer = i;
                            }
                            else if (tickets[i] == maxTicket && leVotes[i] > leVotes[correspondingServer]) {
                                maxTicket = tickets[i];
                                correspondingServer = i;
                            }
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
}
