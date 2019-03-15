package agent.learner;

import agent.proposer.AdaProposer;
import client.ClientRequest;
import instance.AdaPaxosInstance;
import instance.InstanceStatus;
import instance.StaticPaxosInstance;
import instance.maintenance.HistoryMaintenance;
import instance.store.RemoteInstanceStore;
import logger.PaxosLogger;
import network.message.protocols.GenericPaxosMessage;
import network.service.sender.PeerMessageSender;
import utils.AdaAgents;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static utils.AdaAgents.broadcastOnDisks;

/**
 * @author : Swimiltylers
 * @version : 2019/3/15 17:31
 */
public class AdaLearner implements Learner{
    private final int serverId;
    private final int peerSize;

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
        this.instanceSpace = instanceSpace;
        this.restoreRequests = restoreRequests;
        this.forceFsync = forceFsync;
        this.logger = logger;
    }

    @Override
    public void handleAckAccept(GenericPaxosMessage.ackAccept ackAccept) {
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

                if (inst.lmu.response > peerSize/2){
                    inst = instanceSpace.updateAndGet(ackAccept.inst_no, instance -> {
                        instance = AdaPaxosInstance.copy(instance);
                        instance.status = InstanceStatus.COMMITTED;
                        instance.lmu.refresh(AdaAgents.newToken(), serverId);

                        return instance;
                    });

                    GenericPaxosMessage.Commit sendOut = new GenericPaxosMessage.Commit(ackAccept.inst_no, serverId, inst.crtInstBallot, inst.requests);
                    logger.logCommit(ackAccept.inst_no, sendOut, "settled");

                    if (!forceFsync.get()) {
                        sender.broadcastPeerMessage(sendOut);
                    }
                    else {
                        broadcastOnDisks(inst.lmu.token, ackAccept.inst_no, inst, serverId, peerSize, remoteStore);
                    }
                }
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
    public void handleCommit(GenericPaxosMessage.Commit commit) {
        AdaPaxosInstance inst = instanceSpace.get(commit.inst_no);
        if (inst == null){     // back-online case: catch up with current situation
            inst = AdaPaxosInstance.subInst(commit.leaderId, commit.inst_ballot, InstanceStatus.COMMITTED, commit.cmds);
            instanceSpace.set(commit.inst_no, inst);
            logger.logCommit(commit.inst_no, commit, "settled");
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
                }

                /* otherwise, drop the message, which is expired */
            }
            else if (inst.crtLeaderId < commit.leaderId){
                // TODO: 2019/3/15 assertion
            }

            /* otherwise, drop the message, which is expired */
        }
    }
}
