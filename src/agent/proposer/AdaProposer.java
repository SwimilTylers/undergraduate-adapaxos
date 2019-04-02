package agent.proposer;

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
 * @version : 2019/3/15 12:17
 */
public class AdaProposer implements Proposer, DiskResponder {
    private final int serverId;
    private final int peerSize;

    private final PeerMessageSender sender;
    private final RemoteInstanceStore remoteStore;

    private final AtomicReferenceArray<AdaPaxosInstance> instanceSpace;
    private final Queue<ClientRequest> restoreRequests;

    private final AtomicBoolean forceFsync;

    private final PaxosLogger logger;

    public AdaProposer(int serverId, int peerSize,
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
        this.instanceSpace = instanceSpace;
        this.restoreRequests = restoreRequests;
        this.forceFsync = forceFsync;
        this.logger = logger;
    }



    @Override
    public void handleRequests(int inst_no, int ballot, ClientRequest[] requests) {
        long token = AdaAgents.newToken();
        AdaPaxosInstance inst = AdaPaxosInstance.leaderInst(token, serverId, peerSize, ballot, InstanceStatus.PREPARING, requests);
        instanceSpace.set(inst_no, inst);

        if (!forceFsync.get()) {
            logger.logFormatted(false, ""+System.currentTimeMillis(), "prepare="+inst_no, "fsync=false");
            sender.broadcastPeerMessage(new GenericPaxosMessage.Prepare(inst_no, inst.crtLeaderId, inst.crtInstBallot));
        }
        else {
            logger.logFormatted(false, ""+System.currentTimeMillis(), "prepare="+inst_no, "fsync=true");
            broadcastOnDisks(token, inst_no, inst, serverId, peerSize, remoteStore);
        }
    }

    @Override
    public void handleAckPrepare(final GenericPaxosMessage.ackPrepare ackPrepare) {
        AdaPaxosInstance inst = instanceSpace.get(ackPrepare.inst_no);

        if (inst != null  && inst.crtLeaderId == serverId && inst.status == InstanceStatus.PREPARING){   // on this instance, local server works as a leader
            if (ackPrepare.type == GenericPaxosMessage.ackMessageType.PROCEEDING
                    || ackPrepare.type == GenericPaxosMessage.ackMessageType.RESTORE){
                if (ackPrepare.type == GenericPaxosMessage.ackMessageType.PROCEEDING
                        && ackPrepare.ack_leaderId == serverId
                        && ackPrepare.inst_ballot == inst.crtInstBallot){  // normal case
                    inst = instanceSpace.updateAndGet(ackPrepare.inst_no, instance -> {
                        instance = AdaPaxosInstance.copy(instance);
                        ++instance.lmu.response;
                        return instance;
                    });
                }
                else if (ackPrepare.type == GenericPaxosMessage.ackMessageType.RESTORE
                        && ackPrepare.ack_leaderId == serverId
                        && ackPrepare.inst_ballot == inst.crtInstBallot){  // restore-early case

                    inst = instanceSpace.updateAndGet(ackPrepare.inst_no, instance -> {
                        instance = AdaPaxosInstance.copy(instance);
                        ++instance.lmu.response;

                        if (ackPrepare.load != null){     // a meaningful restoration request
                            instance.hmu = HistoryMaintenance.restoreHelper(
                                    instance.hmu,
                                    HistoryMaintenance.RESTORE_TYPE.EARLY,
                                    restoreRequests,
                                    ackPrepare.load.crtLeaderId,
                                    ackPrepare.load.crtInstBallot,
                                    ackPrepare.load.requests
                            );
                        }

                        return instance;
                    });
                }

                /* accumulating until reach Paxos threshold
                 * BROADCASTING_ACCEPT activated only once in each Paxos period (only in PREPARING status) */

                if (inst.lmu.response > peerSize/2)
                    furtherStep(ackPrepare.inst_no);
            }
            else if (ackPrepare.type == GenericPaxosMessage.ackMessageType.RECOVER){
                // recovery case: check status to avoid broadcasting duplicated COMMIT
                restoreRequests.addAll(Arrays.asList(inst.requests));

                inst = instanceSpace.updateAndGet(ackPrepare.inst_no, instance -> {
                    instance = AdaPaxosInstance.copy(instance);
                    instance.requests = ackPrepare.load.requests;
                    instance.status = InstanceStatus.COMMITTED;
                    instance.lmu.refresh(AdaAgents.newToken(), serverId);

                    return instance;
                });

                if (!forceFsync.get()) {
                    sender.broadcastPeerMessage(new GenericPaxosMessage.Commit(ackPrepare.inst_no, serverId, inst.crtInstBallot, inst.requests));
                }
                else {
                    broadcastOnDisks(inst.lmu.token, ackPrepare.inst_no, inst, serverId, peerSize, remoteStore);
                }
            }
            else if (ackPrepare.type == GenericPaxosMessage.ackMessageType.ABORT){  // abort case
                inst = instanceSpace.getAndUpdate(ackPrepare.inst_no, instance -> (AdaPaxosInstance) ackPrepare.load);
                sender.sendPeerMessage(ackPrepare.load.crtLeaderId, new GenericPaxosMessage.Restore(ackPrepare.inst_no, inst));  // apply for restoration

                /* after this point, this server will no longer play the role of leader in this client.
                 * ABORT msg will only react once, since control flow will not reach here again.
                 * There must be only ONE leader in the network ! */
            }
        }
    }

    @Override
    public boolean isValidMessage(int inst_no, long token) {
        AdaPaxosInstance inst = instanceSpace.get(inst_no);
        if (inst != null && inst.lmu != null){
            return inst.lmu.token == token && inst.status == InstanceStatus.PREPARING;
        }
        else
            return false;
    }

    @Override
    public boolean respond_ackWrite(DiskPaxosMessage.ackWrite ackWrite) {
        if (isValidMessage(ackWrite.inst_no, ackWrite.dialog_no)){
            AdaPaxosInstance inst = instanceSpace.updateAndGet(ackWrite.inst_no, instance -> {
                instance = AdaPaxosInstance.copy(instance);
                boolean check = ackWrite.status == DiskPaxosMessage.DiskStatus.WRITE_SUCCESS;
                instance.lmu.writeSign[ackWrite.disk_no] = check;
                if (check && instance.lmu.readCount[ackWrite.disk_no] == peerSize-1){
                    ++instance.lmu.response;
                }
                return instance;
            });

            /* accumulating until reach Paxos threshold
             * BROADCASTING_ACCEPT activated only once in each Paxos period (only in PREPARING status) */

            if (inst.lmu.response > peerSize/2) {
                furtherStep(ackWrite.inst_no);
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean respond_ackRead(DiskPaxosMessage.ackRead ackRead) {
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
                if (inst.lmu.response > peerSize/2) {
                    furtherStep(ackRead.inst_no);
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

                    return false;
                }
                else {
                    instanceSpace.updateAndGet(ackRead.inst_no, instance -> {   // early-restore case
                        instance = AdaPaxosInstance.copy(instance);

                        instance.hmu = HistoryMaintenance.restoreHelper(
                                instance.hmu,
                                HistoryMaintenance.RESTORE_TYPE.EARLY,
                                restoreRequests,
                                ackRead.accessId,
                                ackRead.inst_ballot,
                                ackRead.load.requests
                        );

                        ++instance.lmu.readCount[ackRead.disk_no];
                        if (instance.lmu.writeSign[ackRead.disk_no]
                                && instance.lmu.readCount[ackRead.disk_no] == peerSize - 1) {
                            ++instance.lmu.response;
                        }
                        return instance;
                    });

                    /* accumulating until reach Paxos threshold
                     * BROADCASTING_ACCEPT activated only once in each Paxos period (only in PREPARING status) */
                    if (inst.lmu.response > peerSize/2) {
                        furtherStep(ackRead.inst_no);
                        return true;
                    }
                }
            }
        }

        return false;
    }

    private void furtherStep(int inst_no) {
        // restore-early case: exists formal paxos conversation
        // restore local requests
        AdaPaxosInstance inst = instanceSpace.updateAndGet(inst_no, instance -> {
            instance = AdaPaxosInstance.copy(instance);
            if (instance.hmu != null && instance.hmu.HOST_RESTORE) { // restore-early case: exists formal paxos conversation
                restoreRequests.addAll(Arrays.asList(instance.requests));   // restore local requests
                instance.requests = instance.hmu.reservedCmds;

            }
            instance.status = InstanceStatus.PREPARED;
            instance.lmu.refresh(AdaAgents.newToken(), serverId);
            return instance;
        });

        if (!forceFsync.get()) {
            sender.broadcastPeerMessage(new GenericPaxosMessage.Accept(inst_no, serverId, inst.crtInstBallot, inst.requests));
        }
        else {
            System.out.println("tikz");
            broadcastOnDisks(inst.lmu.token, inst_no, inst, serverId, peerSize, remoteStore);
        }
    }
}
