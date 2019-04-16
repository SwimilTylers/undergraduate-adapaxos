package agent.recovery;

import agent.learner.CommitUpdater;
import instance.AdaPaxosInstance;
import instance.InstanceStatus;
import instance.maintenance.AdaRecoveryMaintenance;
import instance.maintenance.LeaderElectionMaintenance;
import instance.store.RemoteInstanceStore;
import javafx.util.Pair;
import logger.PaxosLogger;
import network.message.protocols.DiskPaxosMessage;
import network.message.protocols.GenericPaxosMessage;
import network.message.protocols.LeaderElectionMessage;
import network.service.module.ConnectionModule;
import network.service.sender.PeerMessageSender;

import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * @author : Swimiltylers
 * @version : 2019/4/13 20:49
 */
public class AdaRecovery implements CrashRecoveryPerformer, DiskCommitVacantResponder, LeaderElectionPerformer {
    private final int serverId;
    private final int peerSize;

    private final PeerMessageSender sender;
    private final RemoteInstanceStore remoteStore;

    private final AtomicReferenceArray<AdaPaxosInstance> instanceSpace;
    private final AtomicReferenceArray<AdaRecoveryMaintenance> recoveryList;;

    private final AtomicBoolean forceFsync;

    private final PaxosLogger logger;

    public AdaRecovery(int serverId, int peerSize,
                       AtomicBoolean forceFsync,
                       PeerMessageSender sender,
                       RemoteInstanceStore remoteStore,
                       AtomicReferenceArray<AdaPaxosInstance> instanceSpace,
                       AtomicReferenceArray<AdaRecoveryMaintenance> recoveryList,
                       PaxosLogger logger) {
        this.serverId = serverId;
        this.peerSize = peerSize;
        this.sender = sender;
        this.remoteStore = remoteStore;
        this.instanceSpace = instanceSpace;
        this.recoveryList = recoveryList;
        this.forceFsync = forceFsync;
        this.logger = logger;
    }

    @Override
    public void handleSync(GenericPaxosMessage.Sync sync){

    }

    @Override
    public void handleAckSync(GenericPaxosMessage.ackSync ackSync, CommitUpdater cUpdater, VacantInstanceUpdater vUpdater){

    }

    @Override
    public boolean isValidMessage(int inst_no, long token) {
        AdaPaxosInstance inst = instanceSpace.get(inst_no);
        return inst == null || inst.status != InstanceStatus.COMMITTED;
    }

    @Override
    public boolean respond_ackRead(DiskPaxosMessage.ackRead ackRead, CommitUpdater cUpdater, VacantInstanceUpdater vUpdater) {
        // TODO: 2019/4/14 Update
        AdaPaxosInstance[] winner = new AdaPaxosInstance[] {null};
        if (ackRead.status == DiskPaxosMessage.DiskStatus.READ_NO_SUCH_FILE) {
            recoveryList.updateAndGet(ackRead.inst_no, unit -> {
                if (unit != null && unit.token == ackRead.dialog_no) {
                    unit.readCount++;

                    if (unit.readCount == peerSize)
                        winner[0] = unit.potential;
                }

                return unit;
            });
        }
        else if (ackRead.status == DiskPaxosMessage.DiskStatus.READ_SUCCESS){
            recoveryList.updateAndGet(ackRead.inst_no, unit -> {
                if (unit != null && unit.token == ackRead.dialog_no) {
                    if (unit.potential == null
                            || unit.potential.crtInstBallot < ackRead.load.crtInstBallot
                            || (unit.potential.crtInstBallot == ackRead.load.crtInstBallot
                            && !InstanceStatus.earlierThan(unit.potential.status, ackRead.load.status))) {
                        unit.potential = (AdaPaxosInstance) ackRead.load;
                    }

                    unit.readCount++;

                    if (unit.readCount == peerSize)
                        winner[0] = unit.potential;
                }

                return unit;
            });
        }

        if (winner[0] != null){

            logger.logFormatted(false, "msync", "nominal", "inst="+winner[0].toString());
            boolean[] commitUpdate = new boolean[]{false};

            instanceSpace.updateAndGet(ackRead.inst_no, instance -> {
                if (instance == null
                        || instance.crtInstBallot < winner[0].crtInstBallot
                        || (instance.crtInstBallot == winner[0].crtInstBallot
                        && !InstanceStatus.earlierThan(instance.status, winner[0].status))) {

                    if ((instance == null || instance.status != InstanceStatus.COMMITTED)
                            && winner[0].status == InstanceStatus.COMMITTED) {
                        commitUpdate[0] = true;
                    }

                    logger.logFormatted(false, "msync", "confirmed", "inst="+winner[0].toString());
                    return winner[0];
                }
                else
                    return instance;
            });

            if (commitUpdate[0]){
                logger.logCommit(ackRead.inst_no, new GenericPaxosMessage.Commit(ackRead.inst_no, winner[0].crtLeaderId, winner[0].crtInstBallot, winner[0].requests), "settled");
                cUpdater.update(ackRead.inst_no);
            }

            return true;
        }

        return false;
    }

    private AtomicReference<LeaderElectionState> leaderElectionState;
    private AtomicReference<LeaderElectionMaintenance> lemu;

    private ConnectionModule conn;

    @Override
    public boolean isLeaderSurvive(final int expire, final int decisionDelay) {
        Pair<Long, Integer> leaderId = lemu.get().leaderChosen;
        if (leaderId != null){
            if (!conn.survive(leaderId.getValue(), expire)){
                try {
                    Thread.sleep(decisionDelay);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                return conn.survive(leaderId.getValue(), expire);
            }
            else
                return true;
        }
        else
            return false;
    }

    @Override
    public void prepareForLeaderElection(LeaderElectionState state, long token) {
        assert state != LeaderElectionState.COMPLETE;
        LeaderElectionState oldState = leaderElectionState.getAndSet(state);
        if (oldState == LeaderElectionState.COMPLETE){
            lemu.updateAndGet(leaderElectionMaintenance -> {
                leaderElectionMaintenance = LeaderElectionMaintenance.copyOf(leaderElectionMaintenance);
                leaderElectionMaintenance.leToken = token;
                leaderElectionMaintenance.leCount = 0;

                return leaderElectionMaintenance;
            });
        }
        else{
            lemu.updateAndGet(leaderElectionMaintenance -> {
                leaderElectionMaintenance = LeaderElectionMaintenance.copyOf(leaderElectionMaintenance);
                leaderElectionMaintenance.leToken = token;
                leaderElectionMaintenance.leCount = 0;
                leaderElectionMaintenance.leaderChosen = null;

                return leaderElectionMaintenance;
            });
        }

    }

    @Override
    public void tryLeaderElection(int ticket) {
        if (leaderElectionState.get() == LeaderElectionState.RECOVERED){
            lemu.updateAndGet(leaderElectionMaintenance -> {

            });
        }
    }

    public boolean tryLeaderElection(long token) {
        if (onLeaderElection.get()) {
            try {
                int ticket = maxRecvInstance.get();
                final int bare_minority = (peerSize + 1) / 2 - 1;
                LLE_Map.updateAndGet(candidates -> {
                    candidates = LeaderElectionCandidates.restart(candidates, token, bare_minority);
                    candidates.lle[serverId] = ticket;
                    return candidates;
                });
                sender.broadcastPeerMessage(new LeaderElectionMessage.Propaganda(serverId, token, ticket));

                leToken.put(token);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            onLeaderElection.set(true);

            return true;
        }
        else
            return false;
    }

    public boolean getLeaderElectionResult() {
        onLeaderElection.set(true);

        try {
            logger.logFormatted(true, "leader election", "token=" + leToken.take());
            CountDownLatch latch = LLE_Map.get().recvCount;
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        LeaderElectionCandidates candidates = LLE_Map.get();
        return findPotentialLeader(candidates.token, candidates.lle);
    }

    public boolean getLeaderElectionResult(int timeout) {
        Future<Boolean> ret = internalThread.submit((Callable<Boolean>) this::getLeaderElectionResult);
        try {
            return ret.get(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            logger.logFormatted(true, "leader election", "timeout");
            LeaderElectionCandidates candidates = LLE_Map.get();
            return findPotentialLeader(candidates.token, candidates.lle);
        }

        return false;
    }

    private boolean findPotentialLeader(long token, int[] response) {
        int leader = 0;
        int max_lle = response[leader];

        for (int i = 1; i < response.length; i++) {
            if (max_lle < response[i]){
                max_lle = response[i];
                leader = i;
            }
        }
        crtLeaderId.set(new Pair<>(token, leader));
        onLeaderElection.set(false);
        logger.logFormatted(true, "leader election", "leader="+(leader==serverId));
        return leader == serverId;
    }

    @Override
    public void handleLEPropaganda(LeaderElectionMessage.Propaganda propaganda) {
        LeaderElectionCandidates candidates = LLE_Map.updateAndGet(candidate -> {
            candidate = LeaderElectionCandidates.copyOf(candidate);
            int oldValue = candidate.lle[propaganda.fromId];
            candidate.lle[propaganda.fromId] = Integer.max(oldValue, propaganda.ticket);
            candidate.lle[serverId] = maxRecvInstance.get();

            return candidate;
        });

        sender.sendPeerMessage(propaganda.fromId, new LeaderElectionMessage.Vote(serverId, propaganda.token, candidates.lle));
    }

    @Override
    public void handleLEVote(LeaderElectionMessage.Vote vote) {
        LLE_Map.updateAndGet(candidates -> {
            candidates = LeaderElectionCandidates.copyOf(candidates);

            for (int i = 0; i < candidates.lle.length; i++)
                candidates.lle[i] = Integer.max(candidates.lle[i], vote.tickets[i]);

            if (candidates.token == vote.token)
                candidates.recvCount.countDown();
            return candidates;
        });
    }

    @Override
    @Deprecated
    public void initLESync() {
        LeaderElectionCandidates entry = LLE_Map.get();
        Pair<Long, Integer> leaderId = crtLeaderId.get();
        sender.broadcastPeerMessage(new LeaderElectionMessage.LESync(serverId, entry.token, entry.lle, leaderId != null && leaderId.getValue() == serverId));
    }

    @Override
    @Deprecated
    public void handleLESync(LeaderElectionMessage.LESync leSync) {
        LLE_Map.updateAndGet(candidates -> {
            candidates = LeaderElectionCandidates.copyOf(candidates);

            for (int i = 0; i < candidates.lle.length; i++)
                candidates.lle[i] = Integer.max(candidates.lle[i], leSync.tickets[i]);

            return candidates;
        });

        if (leSync.asLeader) {
            crtLeaderId.updateAndGet(leaderId -> {
                if (leaderId == null)
                    return new Pair<>(leSync.token, leSync.fromId);
                else
                    return leaderId;
            });
        }
    }
}
