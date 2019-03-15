package agent.learner;

import client.ClientRequest;
import com.sun.istack.internal.NotNull;
import instance.StaticPaxosInstance;
import logger.PaxosLogger;
import network.message.protocols.GenericPaxosMessage;
import instance.InstanceStatus;
import instance.maintenance.HistoryMaintenance;
import network.service.sender.PeerMessageSender;

import java.util.Queue;

/**
 * @author : Swimiltylers
 * @version : 2019/2/14 19:09
 */
public class GenericLearner implements Learner {
    private PeerMessageSender net;

    private int serverId;
    private int peerSize;

    private StaticPaxosInstance[] instanceSpace;

    private Queue<ClientRequest> restoredRequestList;

    private PaxosLogger logger;

    public GenericLearner(int serverId, int peerSize,
                          @NotNull StaticPaxosInstance[] instanceSpace,
                          @NotNull PeerMessageSender net,
                          @NotNull Queue<ClientRequest> restoredRequestList,
                          @NotNull PaxosLogger logger) {
        this.serverId = serverId;
        this.peerSize = peerSize;
        this.instanceSpace = instanceSpace;
        this.net = net;
        this.restoredRequestList = restoredRequestList;
        this.logger = logger;
    }

    @Override
    public void handleAckAccept(GenericPaxosMessage.ackAccept ackAccept) {
        if (instanceSpace[ackAccept.inst_no] != null
                && instanceSpace[ackAccept.inst_no].crtLeaderId == serverId){         // on this client, local server works as a leader

            StaticPaxosInstance inst = instanceSpace[ackAccept.inst_no];

            if (ackAccept.type == GenericPaxosMessage.ackMessageType.PROCEEDING || ackAccept.type == GenericPaxosMessage.ackMessageType.RESTORE){
                if (ackAccept.type == GenericPaxosMessage.ackMessageType.PROCEEDING
                        && ackAccept.ack_leaderId == serverId
                        && ackAccept.inst_ballot == inst.crtInstBallot){  // normal case

                    ++inst.leaderMaintenanceUnit.acceptResponse;
                }
                else if (ackAccept.type == GenericPaxosMessage.ackMessageType.RESTORE
                        && ackAccept.ack_leaderId == serverId
                        && ackAccept.inst_ballot == inst.crtInstBallot){  // restore-last case

                    ++inst.leaderMaintenanceUnit.acceptResponse;

                    if (ackAccept.load != null){     // a meaningful restoration request
                        inst.leaderMaintenanceUnit.historyMaintenanceUnit = HistoryMaintenance.restoreHelper(
                                inst.leaderMaintenanceUnit.historyMaintenanceUnit,
                                HistoryMaintenance.RESTORE_TYPE.LATE,
                                restoredRequestList,
                                ackAccept.load.crtLeaderId,
                                ackAccept.load.crtInstBallot,
                                ackAccept.load.requests
                        );
                    }
                }

                if (inst.status == InstanceStatus.PREPARED
                        && inst.leaderMaintenanceUnit.acceptResponse > peerSize/2){
                    inst.status = InstanceStatus.COMMITTED;

                    GenericPaxosMessage.Commit sendOut = new GenericPaxosMessage.Commit(ackAccept.inst_no, serverId, inst.crtInstBallot, inst.requests);
                    logger.logCommit(ackAccept.inst_no, sendOut, "settled");
                    net.broadcastPeerMessage(sendOut);
                }
            }
            else if (ackAccept.type == GenericPaxosMessage.ackMessageType.RECOVER){ // recovery case
                /* vacant, due to the property mentioned in handleAccept.[recovery case] */
            }
            else if (ackAccept.type == GenericPaxosMessage.ackMessageType.ABORT){   // abort case
                net.sendPeerMessage(ackAccept.load.crtLeaderId, new GenericPaxosMessage.Restore(ackAccept.inst_no, inst));  // apply for restoration

                instanceSpace[ackAccept.inst_no] = (StaticPaxosInstance) ackAccept.load;

                /* after this point, this server will no longer play the role of leader in this client.
                 * ABORT msg will only react once, since control flow will not reach here again.
                 * There must be only ONE leader in the network ! */
            }
        }
    }

    @Override
    public void handleCommit(GenericPaxosMessage.Commit commit) {
        if (instanceSpace[commit.inst_no] == null){     // back-online case: catch up with current situation
            StaticPaxosInstance inst = new StaticPaxosInstance();
            inst.crtLeaderId = commit.leaderId;
            inst.crtInstBallot = commit.inst_ballot;

            inst.requests = commit.cmds;
            inst.status = InstanceStatus.COMMITTED;

            instanceSpace[commit.inst_no] = inst;
            //System.out.println("successfully committed");
            logger.logCommit(commit.inst_no, commit, "settled");
        }
        else{
            StaticPaxosInstance inst = instanceSpace[commit.inst_no];
            if (inst.crtLeaderId == commit.leaderId){      // normal case: whatever the status is, COMMIT demands comply
                if (inst.crtInstBallot <= commit.inst_ballot){
                    inst.crtInstBallot = commit.inst_ballot;
                    inst.requests = commit.cmds;
                    inst.status = InstanceStatus.COMMITTED;

                    //System.out.println("successfully committed");
                    logger.logCommit(commit.inst_no, commit, "settled");
                }

                /* otherwise, drop the message, which is expired */
            }
            else if (inst.crtLeaderId < commit.leaderId){
                GenericPaxosMessage.Restore reply = new GenericPaxosMessage.Restore(commit.inst_no, inst.copyOf());

                inst.crtLeaderId = commit.leaderId;
                inst.crtInstBallot = commit.inst_ballot;
                inst.requests = commit.cmds;
                inst.status = InstanceStatus.COMMITTED;
                inst.leaderMaintenanceUnit = null;

                net.sendPeerMessage(commit.leaderId, reply);
                //System.out.println("successfully committed");
                logger.logCommit(commit.inst_no, commit, "settled");
            }

            /* otherwise, drop the message, which is expired */
        }
    }
}
