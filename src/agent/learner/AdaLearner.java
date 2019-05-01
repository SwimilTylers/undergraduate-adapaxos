package agent.learner;

import client.ClientRequest;
import instance.AdaPaxosInstance;
import instance.InstanceStatus;
import instance.maintenance.HistoryMaintenance;
import instance.store.RemoteInstanceStore;
import logger.PaxosLogger;
import network.message.protocols.DiskPaxosMessage;
import network.message.protocols.GenericPaxosMessage;
import network.service.sender.PeerMessageSender;
import utils.AdaAgents;

import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static utils.AdaAgents.broadcastOnDisks;

/**
 * @author : Swimiltylers
 * @version : 2019/3/15 17:31
 */
public class AdaLearner implements Learner, DiskCommitResponder {
    private final int serverId;
    private final int peerSize;
    private final int diskSize;

    private final PeerMessageSender sender;
    private final RemoteInstanceStore remoteStore;

    private final AtomicReferenceArray<AdaPaxosInstance> instanceSpace;
    private final Queue<ClientRequest> restoreRequests;

    private final AtomicBoolean forceFsync;

    private final PaxosLogger logger;

    public AdaLearner(int serverId, int peerSize,
                       AtomicBoolean forceFsync,
                       PeerMessageSender sender,
                       RemoteInstanceStore remoteStore,
                       AtomicReferenceArray<AdaPaxosInstance> instanceSpace,
                       Queue<ClientRequest> restoreRequests,
                       PaxosLogger logger) {
        this.serverId = serverId;
        this.peerSize = peerSize;
        this.sender = sender;
        this.remoteStore = remoteStore;
        this.diskSize = remoteStore.getDiskSize();
        this.instanceSpace = instanceSpace;
        this.restoreRequests = restoreRequests;
        this.forceFsync = forceFsync;
        this.logger = logger;
    }

    @Override
    public void handleAckAccept(GenericPaxosMessage.ackAccept ackAccept, CommitUpdater updater) {
        AdaPaxosInstance inst = instanceSpace.get(ackAccept.inst_no);

        if (inst != null && inst.crtLeaderId == serverId && inst.status == InstanceStatus.PREPARED){         // on this client, local server works as a leader
            if (ackAccept.type == GenericPaxosMessage.ackMessageType.PROCEEDING
                    || ackAccept.type == GenericPaxosMessage.ackMessageType.RESTORE){
                if (ackAccept.type == GenericPaxosMessage.ackMessageType.PROCEEDING
                        && ackAccept.ack_leaderId == serverId
                        && ackAccept.inst_ballot == inst.crtInstBallot){  // normal case
                    inst = instanceSpace.updateAndGet(ackAccept.inst_no, instance -> {
                        instance = AdaPaxosInstance.copy(instance);
                        ++instance.lmu.response;
                        return instance;
                    });
                }
                else if (ackAccept.type == GenericPaxosMessage.ackMessageType.RESTORE
                        && ackAccept.ack_leaderId == serverId
                        && ackAccept.inst_ballot == inst.crtInstBallot){  // restore-last case

                    inst = instanceSpace.updateAndGet(ackAccept.inst_no, instance -> {
                        instance = AdaPaxosInstance.copy(instance);
                        ++instance.lmu.response;

                        if (ackAccept.load != null){     // a meaningful restoration request
                            instance.hmu = HistoryMaintenance.restoreHelper(
                                    instance.hmu,
                                    HistoryMaintenance.RESTORE_TYPE.LATE,
                                    restoreRequests,
                                    ackAccept.load.crtLeaderId,
                                    ackAccept.load.crtInstBallot,
                                    ackAccept.load.requests
                            );
                        }

                        return instance;
                    });
                }

                if (inst.lmu.response > peerSize/2)
                    furtherStep(ackAccept.inst_no, updater);
            }
            else if (ackAccept.type == GenericPaxosMessage.ackMessageType.RECOVER){ // recovery case
                /* vacant, due to the property mentioned in handleAccept.[recovery case] */
            }
            else if (ackAccept.type == GenericPaxosMessage.ackMessageType.ABORT){   // abort case
                inst = instanceSpace.getAndUpdate(ackAccept.inst_no, instance -> (AdaPaxosInstance) ackAccept.load);
                sender.sendPeerMessage(ackAccept.load.crtLeaderId, new GenericPaxosMessage.Restore(ackAccept.inst_no, inst));  // apply for restoration

                /* after this point, this server will no longer play the role of leader in this client.
                 * ABORT msg will only react once, since control flow will not reach here again.
                 * There must be only ONE leader in the network ! */
            }
        }
    }

    @Override
    public void handleCommit(GenericPaxosMessage.Commit commit, CommitUpdater updater) {
        AdaPaxosInstance inst = instanceSpace.get(commit.inst_no);
        if (inst == null){     // back-online case: catch up with current situation
            inst = AdaPaxosInstance.subInst(commit.leaderId, commit.inst_ballot, InstanceStatus.COMMITTED, commit.cmds);
            instanceSpace.set(commit.inst_no, inst);
            logger.logCommit(commit.inst_no, commit, "settled");
            updater.update(commit.inst_no);
        }
        else{
            if (inst.crtLeaderId == commit.leaderId){      // normal case: whatever the status is, COMMIT demands comply
                if (inst.crtInstBallot <= commit.inst_ballot){
                    instanceSpace.updateAndGet(commit.inst_no, instance -> {
                        instance = AdaPaxosInstance.copy(instance);
                        instance.crtInstBallot = commit.inst_ballot;
                        instance.requests = commit.cmds;
                        instance.status = InstanceStatus.COMMITTED;

                        return instance;
                    });
                    //System.out.println("successfully committed");
                    logger.logCommit(commit.inst_no, commit, "settled");
                    updater.update(commit.inst_no);
                }

                /* otherwise, drop the message, which is expired */
            }
            else if (inst.crtLeaderId < commit.leaderId){
                // TODO: 2019/3/15 assertion
            }

            /* otherwise, drop the message, which is expired */
        }
    }

    @Override
    public boolean isValidMessage(int inst_no, long token) {
        AdaPaxosInstance inst = instanceSpace.get(inst_no);
        if (inst != null && inst.lmu != null){
            return inst.lmu.token == token && inst.status == InstanceStatus.PREPARED;
        }
        else
            return false;
    }

    @Override
    public boolean respond_ackWrite(DiskPaxosMessage.ackWrite ackWrite, CommitUpdater updater) {
        if (isValidMessage(ackWrite.inst_no, ackWrite.dialog_no)){
            AdaPaxosInstance inst = instanceSpace.updateAndGet(ackWrite.inst_no, instance -> {
                instance = AdaPaxosInstance.copy(instance);
                boolean check = ackWrite.status == DiskPaxosMessage.DiskStatus.WRITE_SUCCESS;
                if (!instance.lmu.writeSign[ackWrite.disk_no] && check) {
                    instance.lmu.writeSign[ackWrite.disk_no] = true;
                    Arrays.fill(instance.lmu.readCount, 0);
                    for (int disk_no = 0; disk_no < diskSize; disk_no++) {
                        for (int access_id = 0; access_id < peerSize; access_id++) {
                            if (access_id != serverId)
                                remoteStore.launchRemoteFetch(ackWrite.dialog_no, disk_no, access_id, ackWrite.inst_no);
                        }
                    }
                }
                return instance;
            });

            /* accumulating until reach Paxos threshold
             * BROADCASTING_ACCEPT activated only once in each Paxos period (only in PREPARING status) */

            if (inst.lmu.response > diskSize/2) {
                furtherStep(ackWrite.inst_no, updater);
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean respond_ackRead(DiskPaxosMessage.ackRead ackRead, CommitUpdater updater) {
        if (isValidMessage(ackRead.inst_no, ackRead.dialog_no)){
            if (ackRead.status == DiskPaxosMessage.DiskStatus.READ_NO_SUCH_FILE) {
                AdaPaxosInstance inst = instanceSpace.updateAndGet(ackRead.inst_no, instance -> {
                    instance = AdaPaxosInstance.copy(instance);

                    ++instance.lmu.readCount[ackRead.disk_no];
                    if (instance.lmu.writeSign[ackRead.disk_no]
                            && instance.lmu.readCount[ackRead.disk_no] == peerSize - 1) {
                        ++instance.lmu.response;
                    }
                    return instance;
                });

                /* accumulating until reach Paxos threshold
                 * BROADCASTING_ACCEPT activated only once in each Paxos period (only in PREPARING status) */
                if (inst.lmu.response > diskSize/2) {
                    furtherStep(ackRead.inst_no, updater);
                    return true;
                }
            }
            else if (ackRead.status == DiskPaxosMessage.DiskStatus.READ_SUCCESS && ackRead.accessId != serverId){
                AdaPaxosInstance inst = instanceSpace.get(ackRead.inst_no);
                if (ackRead.load.crtInstBallot > inst.crtInstBallot){   // abort case
                    inst = instanceSpace.getAndUpdate(ackRead.inst_no, instance -> (AdaPaxosInstance) ackRead.load);
                    sender.sendPeerMessage(ackRead.load.crtLeaderId, new GenericPaxosMessage.Restore(ackRead.inst_no, inst));  // apply for restoration

                    /* after this point, this server will no longer play the role of leader in this client.
                     * ABORT msg will only react once, since control flow will not reach here again.
                     * There must be only ONE leader in the network ! */

                    return true;
                }
                else {
                    inst = instanceSpace.updateAndGet(ackRead.inst_no, instance -> {
                        instance = AdaPaxosInstance.copy(instance);

                        ++instance.lmu.readCount[ackRead.disk_no];
                        if (instance.lmu.writeSign[ackRead.disk_no]
                                && instance.lmu.readCount[ackRead.disk_no] == peerSize - 1) {
                            ++instance.lmu.response;
                        }
                        return instance;
                    });

                    /* accumulating until reach Paxos threshold
                     * BROADCASTING_ACCEPT activated only once in each Paxos period (only in PREPARING status) */
                    if (inst.lmu.response > diskSize/2) {
                        furtherStep(ackRead.inst_no, updater);
                        return true;
                    }
                }
            }
        }

        return false;
    }

    private void furtherStep(int inst_no, CommitUpdater updater) {
        AdaPaxosInstance inst = instanceSpace.updateAndGet(inst_no, instance -> {
            instance = AdaPaxosInstance.copy(instance);
            instance.status = InstanceStatus.COMMITTED;
            instance.lmu.refresh(AdaAgents.newToken(), serverId);

            return instance;
        });

        GenericPaxosMessage.Commit sendOut = new GenericPaxosMessage.Commit(inst_no, serverId, inst.crtInstBallot, inst.requests);
        logger.logCommit(inst_no, sendOut, "settled");

        if (!forceFsync.get()) {
            sender.broadcastPeerMessage(sendOut);
        }
        else {
            broadcastOnDisks(inst.lmu.token, inst_no, inst, diskSize, remoteStore);
        }

        updater.update(inst_no);
    }
}
