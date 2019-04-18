package agent.recovery;

import agent.learner.CommitUpdater;
import instance.AdaPaxosInstance;
import instance.InstanceStatus;
import instance.maintenance.AdaRecoveryMaintenance;
import logger.PaxosLogger;
import network.message.protocols.DiskPaxosMessage;
import network.message.protocols.GenericPaxosMessage;
import network.service.module.ConnectionModule;
import network.service.sender.PeerMessageSender;

import java.util.concurrent.atomic.*;

/**
 * @author : Swimiltylers
 * @version : 2019/4/13 20:49
 */
public class AdaRecovery extends LeaderElectionRecovery implements CrashRecoveryPerformer, DiskCommitVacantResponder {
    private final int diskSize;

    private final AtomicReferenceArray<AdaPaxosInstance> instanceSpace;
    private final AtomicReferenceArray<AdaRecoveryMaintenance> recoveryList;;

    private final PaxosLogger logger;

    public AdaRecovery(int serverId, int peerSize,
                       int diskSize, int leaderId,
                       PeerMessageSender sender,
                       ConnectionModule conn,
                       AtomicInteger maxRecvInstance,
                       AtomicReferenceArray<AdaPaxosInstance> instanceSpace,
                       AtomicReferenceArray<AdaRecoveryMaintenance> recoveryList,
                       PaxosLogger logger) {
        super(serverId, peerSize, leaderId, maxRecvInstance, sender, conn, logger);
        this.diskSize = diskSize;
        this.serverId = serverId;
        this.peerSize = peerSize;
        this.sender = sender;

        this.instanceSpace = instanceSpace;
        this.recoveryList = recoveryList;

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

        boolean chosen = false;
        AdaPaxosInstance chosenInstance = null;

        if (ackRead.status == DiskPaxosMessage.DiskStatus.READ_NO_SUCH_FILE) {
            AdaRecoveryMaintenance armu = recoveryList.updateAndGet(ackRead.inst_no, unit -> {
                if (unit != null && unit.token == ackRead.dialog_no && !unit.recovered) {
                    unit.readCount[ackRead.disk_no]++;

                    if (unit.readCount[ackRead.disk_no] == peerSize)
                        ++unit.diskCount;

                    unit.recovered = unit.diskCount == diskSize;
                }

                return unit;
            });

            if (armu.recovered){
                chosen = true;
                chosenInstance = armu.potential;
            }
        }
        else if (ackRead.status == DiskPaxosMessage.DiskStatus.READ_SUCCESS){
            AdaRecoveryMaintenance armu = recoveryList.updateAndGet(ackRead.inst_no, unit -> {
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

                    unit.recovered = unit.diskCount == diskSize;
                }

                return unit;
            });

            if (armu.recovered){
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
                logger.logFormatted(false, "msync", "nominal", "inst="+chosenInstance.toString());
                final AdaPaxosInstance updated = chosenInstance;

                AdaPaxosInstance oldInst = instanceSpace.getAndUpdate(ackRead.inst_no, instance -> {
                    if (instance == null
                            || instance.crtInstBallot < updated.crtInstBallot
                            || (instance.crtInstBallot == updated.crtInstBallot
                            && !InstanceStatus.earlierThan(instance.status, updated.status))) {

                        /*
                        if ((instance == null || instance.status != InstanceStatus.COMMITTED)
                                && updated.status == InstanceStatus.COMMITTED) {
                            commitUpdate[0] = true;
                        }
                        */

                        logger.logFormatted(false, "msync", "confirmed", "inst="+updated.toString());
                        return updated;
                    }
                    else
                        return instance;
                });

                if ((oldInst == null || oldInst.status != InstanceStatus.COMMITTED) && updated.status == InstanceStatus.COMMITTED){
                    logger.logCommit(ackRead.inst_no, new GenericPaxosMessage.Commit(ackRead.inst_no, updated.crtLeaderId, updated.crtInstBallot, updated.requests), "settled");
                    cUpdater.update(ackRead.inst_no);
                }

                return true;
            }
        }

        return false;
    }
}