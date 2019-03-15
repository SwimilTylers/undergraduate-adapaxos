package agent.acceptor;

import instance.StaticPaxosInstance;
import network.message.protocols.GenericPaxosMessage;
import instance.InstanceStatus;
import network.service.sender.PeerMessageSender;

/**
 * @author : Swimiltylers
 * @version : 2019/2/14 19:08
 */
public class GenericAcceptor implements Acceptor {
    private PeerMessageSender net;
    private StaticPaxosInstance[] instanceSpace;


    public GenericAcceptor(StaticPaxosInstance[] instanceSpace, PeerMessageSender net) {
        this.instanceSpace = instanceSpace;
        this.net = net;
    }

    private boolean fitRestoreCase(StaticPaxosInstance inst){
        if (inst.status == InstanceStatus.PREPARING || inst.status == InstanceStatus.PREPARED)
            return inst.leaderMaintenanceUnit != null;      // former leader
        else return inst.status == InstanceStatus.ACCEPTED;
    }

    private boolean fitRecoveryCase(StaticPaxosInstance inst){
        return inst.status == InstanceStatus.COMMITTED;
    }

    @Override
    public void handlePrepare(GenericPaxosMessage.Prepare prepare) {
        if (instanceSpace[prepare.inst_no] == null){    // normal case
            StaticPaxosInstance inst = new StaticPaxosInstance();
            inst.crtLeaderId = prepare.leaderId;
            inst.crtInstBallot = prepare.inst_ballot;
            inst.status = InstanceStatus.PREPARING;

            instanceSpace[prepare.inst_no] = inst;

            net.sendPeerMessage(
                    prepare.leaderId,
                    new GenericPaxosMessage.ackPrepare(
                            prepare.inst_no,
                            GenericPaxosMessage.ackMessageType.PROCEEDING,
                            prepare.leaderId,
                            prepare.inst_ballot, null
                    )
            );
        }
        else if (instanceSpace[prepare.inst_no].crtLeaderId < prepare.leaderId){
            StaticPaxosInstance inst = instanceSpace[prepare.inst_no];
            if (fitRestoreCase(inst)){      // restore-early case
                GenericPaxosMessage.ackPrepare reply = new GenericPaxosMessage.ackPrepare(
                        prepare.inst_no,
                        GenericPaxosMessage.ackMessageType.RESTORE,
                        prepare.leaderId,
                        prepare.inst_ballot,
                        inst.copyOf()
                );

                inst.crtLeaderId = prepare.leaderId;
                inst.crtInstBallot = prepare.inst_ballot;
                inst.status = InstanceStatus.PREPARING;
                inst.requests = null;
                inst.leaderMaintenanceUnit = null;

                net.sendPeerMessage(prepare.leaderId, reply);
            }
            else if (fitRecoveryCase(inst)){   // recovery case
                GenericPaxosMessage.ackPrepare reply = new GenericPaxosMessage.ackPrepare(
                        prepare.inst_no,
                        GenericPaxosMessage.ackMessageType.RECOVER,
                        prepare.leaderId,
                        prepare.inst_ballot,
                        inst.copyOf()
                );

                inst.crtLeaderId = prepare.leaderId;
                inst.crtInstBallot = prepare.inst_ballot;
                inst.status = InstanceStatus.COMMITTED;
                inst.leaderMaintenanceUnit = null;

                net.sendPeerMessage(prepare.leaderId, reply);
            }
            else{   // overwrite case
                inst.crtLeaderId = prepare.leaderId;
                inst.crtInstBallot = prepare.inst_ballot;
                inst.status = InstanceStatus.PREPARING;

                inst.leaderMaintenanceUnit = null;
                inst.requests = null;

                net.sendPeerMessage(
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
        else if (instanceSpace[prepare.inst_no].crtLeaderId == prepare.leaderId){
            StaticPaxosInstance inst = instanceSpace[prepare.inst_no];
            if (inst.crtInstBallot < prepare.inst_ballot){
                if (fitRestoreCase(inst)){  // restore-back-online case: catch up with current situation
                    inst.crtLeaderId = prepare.leaderId;
                    inst.crtInstBallot = prepare.inst_ballot;
                    inst.status = InstanceStatus.PREPARING;
                    inst.requests = null;

                    net.sendPeerMessage(
                            prepare.leaderId,
                            new GenericPaxosMessage.ackPrepare(
                                    prepare.inst_no,
                                    GenericPaxosMessage.ackMessageType.PROCEEDING,
                                    prepare.leaderId,
                                    prepare.inst_ballot, null
                            )
                    );
                }
                else if (fitRecoveryCase(inst)){     // recovery case
                    GenericPaxosMessage.ackPrepare reply = new GenericPaxosMessage.ackPrepare(
                            prepare.inst_no,
                            GenericPaxosMessage.ackMessageType.RECOVER,
                            prepare.leaderId,
                            prepare.inst_ballot,
                            inst.copyOf()
                    );

                    inst.crtInstBallot = prepare.inst_ballot;
                    inst.status = InstanceStatus.COMMITTED;
                    inst.leaderMaintenanceUnit = null;

                    net.sendPeerMessage(prepare.leaderId, reply);
                }
                else{   // overwrite case
                    inst.requests = null;
                    inst.crtInstBallot = prepare.inst_ballot;
                    inst.status = InstanceStatus.PREPARING;

                    inst.leaderMaintenanceUnit = null;

                    net.sendPeerMessage(
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

            /* otherwise, drop the message, which is expired */
        }
        else{   // abort case
            StaticPaxosInstance sendOut = instanceSpace[prepare.inst_no].copyOf();
            sendOut.leaderMaintenanceUnit = null;
            net.sendPeerMessage(prepare.leaderId, sendOut);
        }
    }

    @Override
    public void handleAccept(GenericPaxosMessage.Accept accept) {
        if (instanceSpace[accept.inst_no] == null){     // back-online case: catch up with current situation
            StaticPaxosInstance inst = new StaticPaxosInstance();
            inst.crtLeaderId = accept.leaderId;
            inst.crtInstBallot = accept.inst_ballot;

            inst.requests = accept.cmds;
            inst.status = InstanceStatus.ACCEPTED;

            instanceSpace[accept.inst_no] = inst;

            net.sendPeerMessage(
                    accept.leaderId,
                    new GenericPaxosMessage.ackAccept(
                            accept.inst_no,
                            GenericPaxosMessage.ackMessageType.PROCEEDING,
                            accept.leaderId,
                            accept.inst_ballot, null,
                            inst.requests)
            );
        }
        else if (instanceSpace[accept.inst_no].crtLeaderId == accept.leaderId){
            StaticPaxosInstance inst = instanceSpace[accept.inst_no];
            if (inst.crtInstBallot == accept.inst_ballot && inst.status == InstanceStatus.PREPARING){  // normal case
                inst.requests = accept.cmds;
                inst.status = InstanceStatus.ACCEPTED;

                net.sendPeerMessage(
                        accept.leaderId,
                        new GenericPaxosMessage.ackAccept(
                                accept.inst_no,
                                GenericPaxosMessage.ackMessageType.PROCEEDING,
                                accept.leaderId,
                                accept.inst_ballot, null,
                                inst.requests)
                );
            }
            else if (inst.crtInstBallot < accept.inst_ballot){  // back-online case: catch up with current situation
                inst.crtLeaderId = accept.leaderId;
                inst.crtInstBallot = accept.inst_ballot;
                inst.status = InstanceStatus.ACCEPTED;
                inst.requests = accept.cmds;

                net.sendPeerMessage(
                        accept.leaderId,
                        new GenericPaxosMessage.ackAccept(
                                accept.inst_no,
                                GenericPaxosMessage.ackMessageType.PROCEEDING,
                                accept.leaderId,
                                accept.inst_ballot, null,
                                inst.requests)
                );
            }

            /* otherwise, drop the message, which is expired */
        }
        else if (instanceSpace[accept.inst_no].crtLeaderId < accept.leaderId){
            StaticPaxosInstance inst = instanceSpace[accept.inst_no];
            if (fitRestoreCase(inst)){ // restore-late case
                GenericPaxosMessage.ackAccept reply = new GenericPaxosMessage.ackAccept(
                        accept.inst_no,
                        GenericPaxosMessage.ackMessageType.RESTORE,
                        accept.leaderId,
                        accept.inst_ballot,
                        inst.copyOf(),
                        accept.cmds
                );

                inst.crtLeaderId = accept.leaderId;
                inst.crtInstBallot = accept.inst_ballot;
                inst.status = InstanceStatus.ACCEPTED;
                inst.requests = accept.cmds;
                inst.leaderMaintenanceUnit = null;

                net.sendPeerMessage(accept.leaderId, reply);
            }
            else if (fitRecoveryCase(inst)){  // recovery case

                /* feedback is not necessary.
                 * COMMITTED means there are more than n/2 of [ACCEPTED/COMMITTED],
                 * which must be detected in the first run */

                inst.crtLeaderId = accept.leaderId;
                inst.crtInstBallot = accept.inst_ballot;
                inst.status = InstanceStatus.COMMITTED;
                inst.requests = accept.cmds;
                inst.leaderMaintenanceUnit = null;
            }
            else{   // overwrite case
                inst.crtLeaderId = accept.leaderId;
                inst.crtInstBallot = accept.inst_ballot;
                inst.status = InstanceStatus.PREPARING;

                inst.leaderMaintenanceUnit = null;
                inst.requests = accept.cmds;

                net.sendPeerMessage(
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
        }
        else {  // abort case
            StaticPaxosInstance sendOut = instanceSpace[accept.inst_no].copyOf();
            sendOut.leaderMaintenanceUnit = null;
            net.sendPeerMessage(accept.leaderId, sendOut);
        }
    }
}
