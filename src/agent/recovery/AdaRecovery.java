package agent.recovery;

import agent.learner.CommitUpdater;
import instance.AdaPaxosInstance;
import instance.InstanceStatus;
import instance.maintenance.AdaRecoveryMaintenance;
import instance.store.RemoteInstanceStore;
import logger.PaxosLogger;
import network.message.protocols.DiskPaxosMessage;
import network.message.protocols.GenericPaxosMessage;
import network.message.protocols.LeaderElectionMessage;
import network.service.sender.PeerMessageSender;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;

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

    @Override
    public boolean isLeaderSurvive(final int expire, final int decisionDelay) {
        return false;
    }

    @Override
    public void readyForLeaderElection() {

    }

    @Override
    public boolean initLeaderElection(long token) {
        return false;
    }

    @Override
    public boolean initLeaderElection(long token, int waitingTime) {
        return false;
    }

    @Override
    public void handleLEPropaganda(LeaderElectionMessage.Propaganda propaganda) {

    }

    @Override
    public void handleLEVote(LeaderElectionMessage.Vote vote) {

    }
}
