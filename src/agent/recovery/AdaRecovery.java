package agent.recovery;

import agent.learner.CommitUpdater;
import instance.AdaPaxosInstance;
import instance.InstanceStatus;
import instance.maintenance.AdaRecoveryMaintenance;
import instance.store.RemoteInstanceStore;
import logger.PaxosLogger;
import network.message.protocols.DiskPaxosMessage;
import network.message.protocols.GenericPaxosMessage;
import network.service.module.connection.ConnectionModule;
import network.service.sender.PeerMessageSender;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.*;

/**
 * @author : Swimiltylers
 * @version : 2019/4/13 20:49
 */
public class AdaRecovery extends LeaderElectionRecovery implements CrashRecoveryPerformer, DiskCommitVacantResponder {
    private final int diskSize;
    private RemoteInstanceStore remoteStore;
    private AtomicInteger consecutiveCommit;

    private final AtomicReferenceArray<AdaPaxosInstance> instanceSpace;
    private final AtomicReferenceArray<AdaRecoveryMaintenance> recoveryList;

    private final BlockingQueue<Integer> restartList;

    private final PaxosLogger logger;

    public AdaRecovery(int serverId, int peerSize,
                       int leaderId,
                       PeerMessageSender sender,
                       ConnectionModule conn,
                       RemoteInstanceStore remoteStore, AtomicInteger consecutiveCommit, AtomicReferenceArray<AdaPaxosInstance> instanceSpace,
                       AtomicReferenceArray<AdaRecoveryMaintenance> recoveryList,
                       BlockingQueue<Integer> restartList, PaxosLogger logger) {
        super(serverId, peerSize, leaderId, sender, conn, logger);
        this.remoteStore = remoteStore;
        this.diskSize = remoteStore.getDiskSize();
        this.consecutiveCommit = consecutiveCommit;
        this.restartList = restartList;
        this.serverId = serverId;
        this.peerSize = peerSize;
        this.sender = sender;

        this.instanceSpace = instanceSpace;
        this.recoveryList = recoveryList;

        this.logger = logger;
    }

    @Override
    public void handleSync(GenericPaxosMessage.Sync sync){
        AdaPaxosInstance inst = instanceSpace.get(sync.inst_no);
        sender.sendPeerMessage(sync.fromId, new GenericPaxosMessage.ackSync(sync.inst_no, sync.dialog_no, inst));
    }

    @Override
    public void handleAckSync(GenericPaxosMessage.ackSync ackSync, CommitUpdater cUpdater, VacantInstanceUpdater vUpdater){
        AdaRecoveryMaintenance armu = recoveryList.getAndUpdate(ackSync.inst_no, unit -> {
            if (unit != null && unit.token == ackSync.dialog_no && !unit.recovered) {
                if (unit.potential == null
                        || unit.potential.crtInstBallot < ackSync.load.crtInstBallot
                        || (unit.potential.crtInstBallot == ackSync.load.crtInstBallot
                        && !InstanceStatus.earlierThan(unit.potential.status, ackSync.load.status))) {
                    unit.potential = (AdaPaxosInstance) ackSync.load;
                }

                ++unit.peerCount;

                unit.recovered = unit.peerCount >= (peerSize+1)/2;

                if (unit.recovered)
                    return null;
            }

            return unit;
        });

        if (armu != null && armu.recovered){
            AdaPaxosInstance chosenInstance = armu.potential;

            if (chosenInstance == null){
                logger.logFormatted(false, "psync", "vacant", "upto="+ackSync.inst_no);
                vUpdater.update(ackSync.dialog_no, ackSync.inst_no);
            }
            else {
                logger.logFormatted(false, "psync", "nominal", "inst_no" + ackSync.inst_no, "inst="+chosenInstance.toString());
                instanceSync(cUpdater, chosenInstance, ackSync.inst_no);

                int next_inst = Integer.max(consecutiveCommit.get(), ackSync.inst_no + 1);
                long token = ackSync.dialog_no;
                logger.logFormatted(false, "psync", "continue", "inst_no=" + next_inst);

                recoveryList.set(next_inst, new AdaRecoveryMaintenance(token, diskSize));
                for (int disk_no = 0; disk_no < diskSize; disk_no++) {
                    for (int leaderId = 0; leaderId < peerSize; leaderId++)
                        remoteStore.launchRemoteFetch(token, disk_no, leaderId, next_inst);
                }
            }
        }
    }

    @Override
    public boolean isValidMessage(int inst_no, long token) {
        AdaPaxosInstance inst = instanceSpace.get(inst_no);
        return inst == null || inst.status != InstanceStatus.COMMITTED;
    }

    @Override
    public boolean respond_ackRead(DiskPaxosMessage.ackRead ackRead, CommitUpdater cUpdater, VacantInstanceUpdater vUpdater) {
        // TODO: 2019/4/14 Update

        boolean chosen = false;
        AdaPaxosInstance chosenInstance = null;

        if (ackRead.status == DiskPaxosMessage.DiskStatus.READ_NO_SUCH_FILE) {
            AdaRecoveryMaintenance armu = recoveryList.getAndUpdate(ackRead.inst_no, unit -> {
                if (unit != null && unit.token == ackRead.dialog_no && !unit.recovered) {
                    unit.readCount[ackRead.disk_no]++;

                    if (unit.readCount[ackRead.disk_no] == peerSize)
                        ++unit.diskCount;

                    unit.recovered = unit.diskCount >= (diskSize+1)/2;

                    if (unit.recovered)
                        return null;
                }

                return unit;
            });

            if (armu != null && armu.recovered){
                chosen = true;
                chosenInstance = armu.potential;
            }
        }
        else if (ackRead.status == DiskPaxosMessage.DiskStatus.READ_SUCCESS){
            AdaRecoveryMaintenance armu = recoveryList.getAndUpdate(ackRead.inst_no, unit -> {
                if (unit != null && unit.token == ackRead.dialog_no && !unit.recovered) {
                    if (unit.potential == null
                            || unit.potential.crtInstBallot < ackRead.load.crtInstBallot
                            || (unit.potential.crtInstBallot == ackRead.load.crtInstBallot
                            && !InstanceStatus.earlierThan(unit.potential.status, ackRead.load.status))) {
                        unit.potential = (AdaPaxosInstance) ackRead.load;
                    }

                    unit.readCount[ackRead.disk_no]++;

                    if (unit.readCount[ackRead.disk_no] == peerSize)
                        ++unit.diskCount;

                    unit.recovered = unit.diskCount >= (diskSize+1)/2;

                    if (unit.recovered)
                        return null;
                }

                return unit;
            });

            if (armu != null && armu.recovered){
                chosen = true;
                chosenInstance = armu.potential;
            }
        }

        if (chosen){
            if (chosenInstance == null){
                logger.logFormatted(false, "msync", "vacant", "upto="+ackRead.inst_no);
                vUpdater.update(ackRead.dialog_no, ackRead.inst_no);
            }
            else {
                logger.logFormatted(false, "msync", "nominal", "inst_no" + ackRead.inst_no, "inst="+chosenInstance.toString());
                instanceSync(cUpdater, chosenInstance, ackRead.inst_no);

                int next_inst = Integer.max(consecutiveCommit.get(), ackRead.inst_no + 1);
                long token = ackRead.dialog_no;
                logger.logFormatted(false, "msync", "continue", "inst_no=" + next_inst);

                recoveryList.set(next_inst, new AdaRecoveryMaintenance(token, diskSize));
                for (int disk_no = 0; disk_no < diskSize; disk_no++) {
                    for (int leaderId = 0; leaderId < peerSize; leaderId++)
                        remoteStore.launchRemoteFetch(token, disk_no, leaderId, next_inst);
                }

                return true;
            }
        }

        return false;
    }

    private void instanceSync(CommitUpdater cUpdater, AdaPaxosInstance chosenInstance, int inst_no) {
        final AdaPaxosInstance updated = chosenInstance;

        AdaPaxosInstance oldInst = instanceSpace.getAndUpdate(inst_no, instance -> {
            if (instance == null
                    || instance.crtInstBallot < updated.crtInstBallot
                    || (instance.crtInstBallot == updated.crtInstBallot
                    && !InstanceStatus.earlierThan(instance.status, updated.status))) {

                logger.logFormatted(false, "msync", "confirmed", "inst=" + updated.toString());
                return updated;
            } else
                return instance;
        });

        if (oldInst == null || oldInst.status != InstanceStatus.COMMITTED) {
            if (updated.status == InstanceStatus.COMMITTED) {
                logger.logCommit(inst_no, new GenericPaxosMessage.Commit(inst_no, updated.crtLeaderId, updated.crtInstBallot, updated.requests), "settled");
                cUpdater.update(inst_no);
            } else {
                logger.logFormatted(false, "msync", "restart", "inst=" + updated.toString());
                restartList.offer(inst_no);
            }
        }
    }
}
