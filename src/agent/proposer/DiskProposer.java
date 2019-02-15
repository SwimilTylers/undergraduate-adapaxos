package agent.proposer;

import client.ClientRequest;
import com.sun.istack.internal.NotNull;
import instance.InstanceStatus;
import instance.PaxosInstance;
import instance.maintenance.DiskLeaderMaintenance;
import network.message.protocols.DiskPaxosMessage;
import network.service.GenericNetService;

import java.util.List;

/**
 * @author : Swimiltylers
 * @version : 2019/2/15 14:13
 */
public class DiskProposer {
    private GenericNetService net;
    private List<ClientRequest> restoredRequestList;

    private int serverId;
    private int peerSize;

    private PaxosInstance[] instanceSpace;
    private int crtInstance = 0;

    private int crtBallot = 0;

    public DiskProposer(int serverId, int peerSize,
                           @NotNull PaxosInstance[] instanceSpace,
                           @NotNull GenericNetService net,
                           @NotNull List<ClientRequest> restoredRequestList) {
        this.serverId = serverId;
        this.peerSize = peerSize;
        this.instanceSpace = instanceSpace;
        this.net = net;
        this.restoredRequestList = restoredRequestList;
    }

    public void handleRequests(@NotNull ClientRequest[] requests){
        PaxosInstance inst = new PaxosInstance();

        inst.crtLeaderId = serverId;
        inst.crtInstBallot = ++crtBallot;
        inst.cmds = requests;
        inst.status = InstanceStatus.PREPARING;
        long initDialogue = System.currentTimeMillis();
        inst.leaderMaintenanceUnit = new DiskLeaderMaintenance(peerSize, initDialogue);

        instanceSpace[crtInstance] = inst;

        net.broadcastPeerMessage(DiskPaxosMessage.integratedWriteAndRead(initDialogue, crtInstance, serverId, peerSize, inst));

        ++crtInstance;
    }

    public void handle(@NotNull DiskPaxosMessage.ackWrite ackWrite, DiskPaxosMessage.ackRead[] ackReads){
        PaxosInstance inst = instanceSpace[ackWrite.inst_no];
        if (inst.leaderMaintenanceUnit instanceof DiskLeaderMaintenance) {
            DiskLeaderMaintenance diskUnit = (DiskLeaderMaintenance) inst.leaderMaintenanceUnit;

            if (inst.status == InstanceStatus.PREPARING && diskUnit.crtDialogue == ackWrite.DIALOGUE_NO) {
                if (ackReads == null) {   // normal case
                    if (!diskUnit.dialogueRecord.containsKey(ackWrite.disk_no))
                        diskUnit.dialogueRecord.put(ackWrite.disk_no, diskUnit.totalDisk);
                    else
                        diskUnit.dialogueRecord.replace(ackWrite.disk_no, diskUnit.totalDisk);

                    ++diskUnit.prepareResponse;     // if dialogueRecord.get(ackWrite.disk_no) > totalDisk/2, then increment diskUnit.prepareResponse
                }

                /* accumulating until reach Paxos threshold
                 * BROADCASTING_2ndIntegrateWriteAndRead activated only once in each Paxos period (only in PREPARING status) */

                if (inst.status == InstanceStatus.PREPARING     // check status to avoid broadcasting duplicated ack
                        && inst.leaderMaintenanceUnit.prepareResponse > peerSize / 2) {
                    /*
                    if (inst.leaderMaintenanceUnit.historyMaintenanceUnit != null
                            && inst.leaderMaintenanceUnit.historyMaintenanceUnit.HOST_RESTORE) { // restore-early case: exists formal paxos conversation
                        restoredRequestList.addAll(Arrays.asList(inst.cmds));   // restore local cmds

                        inst.cmds = inst.leaderMaintenanceUnit.historyMaintenanceUnit.reservedCmds;
                    }
                    */

                    long newDialogue = System.currentTimeMillis();

                    inst.status = InstanceStatus.PREPARED;

                    diskUnit.crtDialogue = newDialogue;     // open a new dialogue
                    diskUnit.dialogueRecord.clear();    // disk dialogue is erased, since dialogue_no has changed

                    net.broadcastPeerMessage(DiskPaxosMessage.integratedWriteAndRead(newDialogue, ackWrite.inst_no, serverId, peerSize, inst));
                }
            }
        }
    }
}
