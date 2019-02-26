package agent.learner;

import client.ClientRequest;
import com.sun.istack.internal.NotNull;
import instance.InstanceStatus;
import instance.PaxosInstance;
import instance.maintenance.DiskLeaderMaintenance;
import logger.PaxosLogger;
import network.message.protocols.DiskPaxosMessage;
import network.message.protocols.GenericPaxosMessage;
import network.service.peer.PeerMessageSender;

import java.util.List;

/**
 * @author : Swimiltylers
 * @version : 2019/2/15 18:21
 */
public class DiskLearner {
    private PeerMessageSender net;

    private int serverId;
    private int peerSize;

    private PaxosInstance[] instanceSpace;

    private List<ClientRequest> restoredRequestList;

    private PaxosLogger logger;

    public DiskLearner(int serverId, int peerSize,
                          @NotNull PaxosInstance[] instanceSpace,
                          @NotNull PeerMessageSender net,
                          @NotNull List<ClientRequest> restoredRequestList,
                          @NotNull PaxosLogger logger) {
        this.serverId = serverId;
        this.peerSize = peerSize;
        this.instanceSpace = instanceSpace;
        this.net = net;
        this.restoredRequestList = restoredRequestList;
        this.logger = logger;
    }

    private boolean isActivate(int inst_no){
        return instanceSpace[inst_no].status == InstanceStatus.PREPARED;
    }

    public boolean handlePacked(DiskPaxosMessage.PackedMessage packedMessage){
        if (packedMessage.desc.equals(DiskPaxosMessage.IRW_ACK_HEADER)) {
            DiskPaxosMessage.ackWrite ackWrite = (DiskPaxosMessage.ackWrite) packedMessage.packages[0];

            if (ackWrite.status == DiskPaxosMessage.DiskStatus.WRITE_SUCCESS) {
                DiskPaxosMessage.ackRead[] ackReads = (DiskPaxosMessage.ackRead[]) ((DiskPaxosMessage.PackedMessage) packedMessage.packages[1]).packages;
                return handle(packedMessage.inst_no, packedMessage.leaderId, packedMessage.inst_ballot, packedMessage.dialog_no, ackReads);
            }
        }
        return false;
    }

    private boolean handle(int inst_no, int ack_leaderId, int inst_ballot, long dialogue_no, DiskPaxosMessage.ackRead[] ackReads) {
        PaxosInstance inst = instanceSpace[inst_no];

        /* only when it is a DiskLeaderMaintenance can it proceed disk-paxos procedure */

        if (inst.crtLeaderId == serverId
                && inst.leaderMaintenanceUnit instanceof DiskLeaderMaintenance) {
            DiskLeaderMaintenance diskUnit = (DiskLeaderMaintenance) inst.leaderMaintenanceUnit;

            /* In the following procedure,
             * both inst.status & diskUnit.crtDialogue is checked, due to:
             *   - inst.status == PREPARED assures Learner's activation
             *   - <leaderId, ballot> & crtDialogue assures ignorance of delayed response */

            if (isActivate(inst_no)
                    && inst.crtLeaderId == ack_leaderId
                    && inst.crtInstBallot == inst_ballot
                    && diskUnit.crtDialogue == dialogue_no) {

                if (ackReads == null) {   // normal case
                    ++diskUnit.acceptResponse;
                }
                else {
                    for (DiskPaxosMessage.ackRead ack : ackReads) {
                        if (ack.status == DiskPaxosMessage.DiskStatus.READ_SUCCESS){
                            PaxosInstance last_inst = ack.load;
                            if (last_inst.crtLeaderId > serverId){      // abort case
                                net.sendPeerMessage(last_inst.crtLeaderId, new GenericPaxosMessage.Restore(inst_no, inst));  // apply for restoration
                                last_inst.leaderMaintenanceUnit = null;
                                instanceSpace[inst_no] = last_inst;

                                /* after this point, this server will no longer play the role of leader in this client.
                                 * ABORT msg will only react once, since control flow will not reach here again.
                                 * There must be only ONE leader in the network ! */

                                return true;
                            }
                            else {
                                /* disk-paxos do not support restore-late-case */
                            }
                        }
                    }

                    ++diskUnit.acceptResponse;
                }

                if (inst.leaderMaintenanceUnit.acceptResponse > peerSize / 2){

                    inst.status = InstanceStatus.COMMITTED;

                    GenericPaxosMessage.Commit sendOut = new GenericPaxosMessage.Commit(inst_no, serverId, inst.crtInstBallot, inst.cmds);
                    logger.logCommit(inst_no, sendOut, "settled");
                    net.broadcastPeerMessage(sendOut);
                }
                return true;
            }
            else
                return false;
        }
        else
            return false;
    }

    public void handleCommit(GenericPaxosMessage.Commit commit) {
        if (instanceSpace[commit.inst_no] == null){     // back-online case: catch up with current situation
            PaxosInstance inst = new PaxosInstance();
            inst.crtLeaderId = commit.leaderId;
            inst.crtInstBallot = commit.inst_ballot;

            inst.cmds = commit.cmds;
            inst.status = InstanceStatus.COMMITTED;

            instanceSpace[commit.inst_no] = inst;
            System.out.println("successfully committed");
            logger.logCommit(commit.inst_no, commit, "settled");
        }
        else{
            PaxosInstance inst = instanceSpace[commit.inst_no];
            if (inst.crtLeaderId == commit.leaderId){      // normal case: whatever the status is, COMMIT demands comply
                if (inst.crtInstBallot <= commit.inst_ballot){
                    inst.crtInstBallot = commit.inst_ballot;
                    inst.cmds = commit.cmds;
                    inst.status = InstanceStatus.COMMITTED;

                    System.out.println("successfully committed");
                    logger.logCommit(commit.inst_no, commit, "settled");
                }

                /* otherwise, drop the message, which is expired */
            }
            else if (inst.crtLeaderId < commit.leaderId){
                GenericPaxosMessage.Restore reply = new GenericPaxosMessage.Restore(commit.inst_no, inst.copyOf());

                inst.crtLeaderId = commit.leaderId;
                inst.crtInstBallot = commit.inst_ballot;
                inst.cmds = commit.cmds;
                inst.status = InstanceStatus.COMMITTED;
                inst.leaderMaintenanceUnit = null;

                net.sendPeerMessage(commit.leaderId, reply);
                System.out.println("successfully committed");
                logger.logCommit(commit.inst_no, commit, "settled");
            }

            /* otherwise, drop the message, which is expired */
        }
    }
}
