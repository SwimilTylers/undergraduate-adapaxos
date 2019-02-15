package agent.learner;

import client.ClientRequest;
import com.sun.istack.internal.NotNull;
import instance.InstanceStatus;
import instance.PaxosInstance;
import instance.maintenance.DiskLeaderMaintenance;
import logger.PaxosLogger;
import network.message.protocols.DiskPaxosMessage;
import network.message.protocols.GenericPaxosMessage;
import network.service.GenericNetService;

import java.util.List;

/**
 * @author : Swimiltylers
 * @version : 2019/2/15 18:21
 */
public class DiskLearner {
    private GenericNetService net;

    private int serverId;
    private int peerSize;

    private PaxosInstance[] instanceSpace;

    private List<ClientRequest> restoredRequestList;

    private PaxosLogger logger;

    public void handle(@NotNull DiskPaxosMessage.ackWrite ackWrite, DiskPaxosMessage.ackRead[] ackReads) {
        PaxosInstance inst = instanceSpace[ackWrite.inst_no];
        if (inst.leaderMaintenanceUnit instanceof DiskLeaderMaintenance) {
            DiskLeaderMaintenance diskUnit = (DiskLeaderMaintenance) inst.leaderMaintenanceUnit;

            if (inst.status == InstanceStatus.PREPARED && diskUnit.crtDialogue == ackWrite.DIALOGUE_NO) {
                if (ackReads == null) {   // normal case
                    if (!diskUnit.dialogueRecord.containsKey(ackWrite.disk_no))
                        diskUnit.dialogueRecord.put(ackWrite.disk_no, diskUnit.totalDisk);
                    else
                        diskUnit.dialogueRecord.replace(ackWrite.disk_no, diskUnit.totalDisk);

                    ++diskUnit.acceptResponse;     // if dialogueRecord.get(ackWrite.disk_no) > totalDisk/2, then increment diskUnit.prepareResponse
                }

                if (inst.status == InstanceStatus.PREPARED
                        && inst.leaderMaintenanceUnit.acceptResponse > peerSize/2){
                    inst.status = InstanceStatus.COMMITTED;

                    // TODO: failure protocols

                    GenericPaxosMessage.Commit sendOut = new GenericPaxosMessage.Commit(ackWrite.inst_no, serverId, inst.crtInstBallot, inst.cmds);
                    logger.logCommit(ackWrite.inst_no, sendOut, "settled");
                    net.broadcastPeerMessage(sendOut);
                }
            }
        }
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
