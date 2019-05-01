package agent.acceptor;

import client.ClientRequest;
import instance.AdaPaxosInstance;
import instance.InstanceStatus;
import instance.StaticPaxosInstance;
import instance.store.RemoteInstanceStore;
import logger.PaxosLogger;
import network.message.protocols.GenericPaxosMessage;
import network.service.sender.PeerMessageSender;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.UnaryOperator;

/**
 * @author : Swimiltylers
 * @version : 2019/3/15 19:11
 */
public class AdaAcceptor implements Acceptor{
    private final int serverId;
    private final int peerSize;

    private final PeerMessageSender sender;
    private final RemoteInstanceStore remoteStore;

    private final AtomicReferenceArray<AdaPaxosInstance> instanceSpace;
    private final Queue<ClientRequest> restoreRequests;

    private final AtomicBoolean forceFsync;

    private final PaxosLogger logger;

    public AdaAcceptor(int serverId, int peerSize,
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

    private boolean fitRestoreCase(AdaPaxosInstance inst){
        return false;
    }

    private boolean fitRecoveryCase(AdaPaxosInstance inst){
        return false;
    }

    @Override
    public void handlePrepare(GenericPaxosMessage.Prepare prepare) {
        AdaPaxosInstance inst = instanceSpace.get(prepare.inst_no);
        UnaryOperator<AdaPaxosInstance> updating = instance -> AdaPaxosInstance.subInst(prepare.leaderId, prepare.inst_ballot, InstanceStatus.PREPARING, null);


        if (inst == null){    // normal case
            inst = AdaPaxosInstance.subInst(prepare.leaderId, prepare.inst_ballot, InstanceStatus.PREPARING, null);
            instanceSpace.set(prepare.inst_no, inst);

            sender.sendPeerMessage(
                    prepare.leaderId,
                    new GenericPaxosMessage.ackPrepare(
                            prepare.inst_no,
                            GenericPaxosMessage.ackMessageType.PROCEEDING,
                            prepare.leaderId,
                            prepare.inst_ballot, null
                    )
            );
        }
        else if (inst.crtInstBallot < prepare.inst_ballot){
            if (inst.crtLeaderId != prepare.leaderId){
                if (fitRestoreCase(inst)){      // restore-early case
                    inst = instanceSpace.getAndUpdate(prepare.inst_no, updating);

                    sender.sendPeerMessage(prepare.leaderId, new GenericPaxosMessage.ackPrepare(
                            prepare.inst_no,
                            GenericPaxosMessage.ackMessageType.RESTORE,
                            prepare.leaderId,
                            prepare.inst_ballot,
                            inst
                    ));
                }
                else if (fitRecoveryCase(inst)){   // recovery case
                    inst = instanceSpace.getAndUpdate(prepare.inst_no, instance -> AdaPaxosInstance.subInst(prepare.leaderId, prepare.inst_ballot, InstanceStatus.COMMITTED, null));

                    sender.sendPeerMessage(prepare.leaderId, new GenericPaxosMessage.ackPrepare(
                            prepare.inst_no,
                            GenericPaxosMessage.ackMessageType.RECOVER,
                            prepare.leaderId,
                            prepare.inst_ballot,
                            inst
                    ));
                }
                else{   // overwrite case
                    instanceSpace.updateAndGet(prepare.inst_no, updating);


                    sender.sendPeerMessage(
                            prepare.leaderId,
                            new GenericPaxosMessage.ackPrepare(
                                    prepare.inst_no,
                                    GenericPaxosMessage.ackMessageType.PROCEEDING,
                                    prepare.leaderId,
                                    prepare.inst_ballot, null
                            )
                    );
                }
            }
            else {      // overwrite case
                instanceSpace.updateAndGet(prepare.inst_no, updating);

                sender.sendPeerMessage(
                        prepare.leaderId,
                        new GenericPaxosMessage.ackPrepare(
                                prepare.inst_no,
                                GenericPaxosMessage.ackMessageType.PROCEEDING,
                                prepare.leaderId,
                                prepare.inst_ballot, null
                        )
                );
            }
        } else {
            inst = instanceSpace.getAndUpdate(prepare.inst_no, updating);
            sender.sendPeerMessage(prepare.leaderId, new GenericPaxosMessage.ackPrepare(
                    prepare.inst_no,
                    GenericPaxosMessage.ackMessageType.ABORT,
                    prepare.leaderId,
                    prepare.inst_ballot,
                    inst
            ));
        }
    }

    @Override
    public void handleAccept(GenericPaxosMessage.Accept accept) {
        AdaPaxosInstance inst = instanceSpace.get(accept.inst_no);

        UnaryOperator<AdaPaxosInstance> updating = instance -> AdaPaxosInstance.subInst(accept.leaderId, accept.inst_ballot, InstanceStatus.ACCEPTED, accept.cmds);

        if (inst == null){     // back-online case: catch up with current situation
            handleAccept_replaceInternal(accept, updating);
        }
        else if (inst.crtLeaderId == accept.leaderId){
            if (inst.crtInstBallot == accept.inst_ballot && inst.status == InstanceStatus.PREPARING){  // normal case
                inst = instanceSpace.updateAndGet(accept.inst_no, instance -> {
                    instance = AdaPaxosInstance.copy(instance);
                    instance.requests = accept.cmds;
                    instance.status = InstanceStatus.ACCEPTED;
                    return instance;
                });

                sender.sendPeerMessage(
                        accept.leaderId,
                        new GenericPaxosMessage.ackAccept(
                                accept.inst_no,
                                GenericPaxosMessage.ackMessageType.PROCEEDING,
                                accept.leaderId,
                                accept.inst_ballot, null,
                                inst.requests
                        )
                );
            }
            else if (inst.crtInstBallot < accept.inst_ballot){  // back-online case: catch up with current situation
                handleAccept_replaceInternal(accept, updating);
            }

            /* otherwise, drop the message, which is expired */
        }
        else if (inst.crtInstBallot < accept.inst_ballot){
            if (fitRestoreCase(inst)){ // restore-late case
                inst = instanceSpace.getAndUpdate(accept.inst_no, updating);

                sender.sendPeerMessage(accept.leaderId, new GenericPaxosMessage.ackAccept(
                        accept.inst_no,
                        GenericPaxosMessage.ackMessageType.RESTORE,
                        accept.leaderId,
                        accept.inst_ballot,
                        inst,
                        accept.cmds
                ));
            }
            else if (fitRecoveryCase(inst)){  // recovery case

                /* feedback is not necessary.
                 * COMMITTED means there are more than n/2 of [ACCEPTED/COMMITTED],
                 * which must be detected in the first run */
                instanceSpace.updateAndGet(accept.inst_no, instance -> AdaPaxosInstance.subInst(accept.leaderId, accept.inst_ballot, InstanceStatus.COMMITTED, accept.cmds));
            }
            else{   // overwrite case
                handleAccept_replaceInternal(accept, updating);
            }
        }
        else {  // abort case
            inst = instanceSpace.getAndUpdate(accept.inst_no, updating);
            sender.sendPeerMessage(accept.leaderId, new GenericPaxosMessage.ackPrepare(
                    accept.inst_no,
                    GenericPaxosMessage.ackMessageType.ABORT,
                    accept.leaderId,
                    accept.inst_ballot,
                    inst
            ));
        }
    }

    private void handleAccept_replaceInternal(GenericPaxosMessage.Accept accept, UnaryOperator<AdaPaxosInstance> updating) {
        AdaPaxosInstance inst = instanceSpace.updateAndGet(accept.inst_no, updating);
        sender.sendPeerMessage(
                accept.leaderId,
                new GenericPaxosMessage.ackAccept(
                        accept.inst_no,
                        GenericPaxosMessage.ackMessageType.PROCEEDING,
                        accept.leaderId,
                        accept.inst_ballot, null,
                        inst.requests)
        );
    }
}
