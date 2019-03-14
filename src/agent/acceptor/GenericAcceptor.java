package agent.acceptor;

import network.message.protocols.GenericPaxosMessage;
import instance.InstanceStatus;
import instance.PaxosInstance;
import network.service.sender.PeerMessageSender;

/**
 * @author : Swimiltylers
 * @version : 2019/2/14 19:08
 */
public class GenericAcceptor implements Acceptor {
    private PeerMessageSender net;
    private PaxosInstance[] instanceSpace;


    public GenericAcceptor(PaxosInstance[] instanceSpace, PeerMessageSender net) {
        this.instanceSpace = instanceSpace;
        this.net = net;
    }

    private boolean fitRestoreCase(PaxosInstance inst){
        if (inst.status == InstanceStatus.PREPARING || inst.status == InstanceStatus.PREPARED)
            return inst.leaderMaintenanceUnit != null;      // former leader
        else return inst.status == InstanceStatus.ACCEPTED;
    }

    private boolean fitRecoveryCase(PaxosInstance inst){
        return inst.status == InstanceStatus.COMMITTED;
    }

    @Override
    public void handlePrepare(GenericPaxosMessage.Prepare prepare) {
        if (instanceSpace[prepare.inst_no] == null){    // normal case
            PaxosInstance inst = new PaxosInstance();
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
            PaxosInstance inst = instanceSpace[prepare.inst_no];
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
                inst.cmds = null;
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
                inst.cmds = null;

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
            PaxosInstance inst = instanceSpace[prepare.inst_no];
            if (inst.crtInstBallot < prepare.inst_ballot){
                if (fitRestoreCase(inst)){  // restore-back-online case: catch up with current situation
                    inst.crtLeaderId = prepare.leaderId;
                    inst.crtInstBallot = prepare.inst_ballot;
                    inst.status = InstanceStatus.PREPARING;
                    inst.cmds = null;

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
                    inst.cmds = null;
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
            PaxosInstance sendOut = instanceSpace[prepare.inst_no].copyOf();
            sendOut.leaderMaintenanceUnit = null;
            net.sendPeerMessage(prepare.leaderId, sendOut);
        }
    }

    @Override
    public void handleAccept(GenericPaxosMessage.Accept accept) {
        if (instanceSpace[accept.inst_no] == null){     // back-online case: catch up with current situation
            PaxosInstance inst = new PaxosInstance();
            inst.crtLeaderId = accept.leaderId;
            inst.crtInstBallot = accept.inst_ballot;

            inst.cmds = accept.cmds;
            inst.status = InstanceStatus.ACCEPTED;

            instanceSpace[accept.inst_no] = inst;

            net.sendPeerMessage(
                    accept.leaderId,
                    new GenericPaxosMessage.ackAccept(
                            accept.inst_no,
                            GenericPaxosMessage.ackMessageType.PROCEEDING,
                            accept.leaderId,
                            accept.inst_ballot, null,
                            inst.cmds)
            );
        }
        else if (instanceSpace[accept.inst_no].crtLeaderId == accept.leaderId){
            PaxosInstance inst = instanceSpace[accept.inst_no];
            if (inst.crtInstBallot == accept.inst_ballot && inst.status == InstanceStatus.PREPARING){  // normal case
                inst.cmds = accept.cmds;
                inst.status = InstanceStatus.ACCEPTED;

                net.sendPeerMessage(
                        accept.leaderId,
                        new GenericPaxosMessage.ackAccept(
                                accept.inst_no,
                                GenericPaxosMessage.ackMessageType.PROCEEDING,
                                accept.leaderId,
                                accept.inst_ballot, null,
                                inst.cmds)
                );
            }
            else if (inst.crtInstBallot < accept.inst_ballot){  // back-online case: catch up with current situation
                inst.crtLeaderId = accept.leaderId;
                inst.crtInstBallot = accept.inst_ballot;
                inst.status = InstanceStatus.ACCEPTED;
                inst.cmds = accept.cmds;

                net.sendPeerMessage(
                        accept.leaderId,
                        new GenericPaxosMessage.ackAccept(
                                accept.inst_no,
                                GenericPaxosMessage.ackMessageType.PROCEEDING,
                                accept.leaderId,
                                accept.inst_ballot, null,
                                inst.cmds)
                );
            }

            /* otherwise, drop the message, which is expired */
        }
        else if (instanceSpace[accept.inst_no].crtLeaderId < accept.leaderId){
            PaxosInstance inst = instanceSpace[accept.inst_no];
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
                inst.cmds = accept.cmds;
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
                inst.cmds = accept.cmds;
                inst.leaderMaintenanceUnit = null;
            }
            else{   // overwrite case
                inst.crtLeaderId = accept.leaderId;
                inst.crtInstBallot = accept.inst_ballot;
                inst.status = InstanceStatus.PREPARING;

                inst.leaderMaintenanceUnit = null;
                inst.cmds = accept.cmds;

                net.sendPeerMessage(
                        accept.leaderId,
                        new GenericPaxosMessage.ackAccept(
                                accept.inst_no,
                                GenericPaxosMessage.ackMessageType.PROCEEDING,
                                accept.leaderId,
                                accept.inst_ballot, null,
                                inst.cmds
                        )
                );
            }
        }
        else {  // abort case
            PaxosInstance sendOut = instanceSpace[accept.inst_no].copyOf();
            sendOut.leaderMaintenanceUnit = null;
            net.sendPeerMessage(accept.leaderId, sendOut);
        }
    }
}
