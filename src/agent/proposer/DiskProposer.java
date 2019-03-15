package agent.proposer;

import client.ClientRequest;
import com.sun.istack.internal.NotNull;
import instance.InstanceStatus;
import instance.StaticPaxosInstance;
import instance.maintenance.DiskLeaderMaintenance;
import instance.maintenance.HistoryMaintenance;
import network.message.protocols.DiskPaxosMessage;
import network.message.protocols.GenericPaxosMessage;
import network.service.sender.PeerMessageSender;

import java.util.Arrays;
import java.util.Queue;

/**
 * @author : Swimiltylers
 * @version : 2019/2/15 14:13
 */
public class DiskProposer {
    private PeerMessageSender net;
    private Queue<ClientRequest> restoredRequestList;

    private int serverId;
    private int peerSize;

    private StaticPaxosInstance[] instanceSpace;

    public DiskProposer(int serverId, int peerSize,
                        @NotNull StaticPaxosInstance[] instanceSpace,
                        @NotNull PeerMessageSender net,
                        @NotNull Queue<ClientRequest> restoredRequestList) {
        this.serverId = serverId;
        this.peerSize = peerSize;
        this.instanceSpace = instanceSpace;
        this.net = net;
        this.restoredRequestList = restoredRequestList;
    }

    public void handleRequests(int inst_no, int ballot, @NotNull ClientRequest[] requests){
        StaticPaxosInstance inst = new StaticPaxosInstance();

        inst.crtLeaderId = serverId;
        inst.crtInstBallot = ballot;
        inst.requests = requests;
        inst.status = InstanceStatus.PREPARING;
        long initDialogue = System.currentTimeMillis();
        inst.leaderMaintenanceUnit = new DiskLeaderMaintenance(peerSize);

        ((DiskLeaderMaintenance) inst.leaderMaintenanceUnit).crtDialogue = initDialogue;
        instanceSpace[inst_no] = inst;

        net.broadcastPeerMessage(DiskPaxosMessage.IRW(inst_no, inst.crtLeaderId, inst.crtInstBallot, initDialogue, peerSize, inst));
    }

    private boolean isSuspend(int inst_no){
        return false;
    }

    private boolean isActivate(int inst_no){
        return instanceSpace[inst_no].status == InstanceStatus.PREPARING;
    }

    public boolean handlePacked(DiskPaxosMessage.PackedMessage packedMessage){
        if (packedMessage.desc.equals(DiskPaxosMessage.IRW_ACK_HEADER)) {
            DiskPaxosMessage.ackWrite ackWrite = (DiskPaxosMessage.ackWrite) packedMessage.packages[0];

            if (ackWrite.status == DiskPaxosMessage.DiskStatus.WRITE_SUCCESS && !isSuspend(packedMessage.inst_no)) {
                DiskPaxosMessage.ackRead[] ackReads = (DiskPaxosMessage.ackRead[]) ((DiskPaxosMessage.PackedMessage) packedMessage.packages[1]).packages;
                return handle(packedMessage.inst_no, packedMessage.leaderId, packedMessage.inst_ballot, packedMessage.dialog_no, ackReads);
            }
        }
        else if (packedMessage.desc.equals(DiskPaxosMessage.IR_ACK_HEADER) && isSuspend(packedMessage.inst_no)){
            return true;
        }
        return false;
    }

    private boolean handle(int inst_no, int ack_leaderId, int inst_ballot, long dialogue_no, DiskPaxosMessage.ackRead[] ackReads){
        StaticPaxosInstance inst = instanceSpace[inst_no];

        /* only when it is a DiskLeaderMaintenance can it proceed disk-paxos procedure */
        //System.out.println("fifi["+(inst.crtLeaderId == serverId)+","+(inst.leaderMaintenanceUnit instanceof DiskLeaderMaintenance)+"]");
        if (inst.crtLeaderId == serverId
                && inst.leaderMaintenanceUnit instanceof DiskLeaderMaintenance) {
            DiskLeaderMaintenance diskUnit = (DiskLeaderMaintenance) inst.leaderMaintenanceUnit;

            /* In the following procedure,
            * both inst.status & diskUnit.crtDialogue is checked, due to:
            *   - inst.status == PREPARING assures Proposer's activation
            *   - <leaderId, ballot> & crtDialogue assures ignorance of delayed response */

            if (isActivate(inst_no)
                    && inst.crtLeaderId == ack_leaderId
                    && inst.crtInstBallot == inst_ballot
                    && diskUnit.crtDialogue == dialogue_no) {

                if (ackReads == null) {   // normal case
                    ++diskUnit.prepareResponse;
                }
                else{
                    for (DiskPaxosMessage.ackRead ack : ackReads) {
                        StaticPaxosInstance last_inst = (StaticPaxosInstance) ack.load;
                        if (last_inst.crtLeaderId <= serverId){     // restore case
                            if (last_inst.requests != null){     // a meaningful restoration request
                                diskUnit.historyMaintenanceUnit = HistoryMaintenance.restoreHelper(
                                        diskUnit.historyMaintenanceUnit,
                                        HistoryMaintenance.RESTORE_TYPE.EARLY,
                                        restoredRequestList,
                                        last_inst.crtLeaderId,
                                        last_inst.crtInstBallot,
                                        last_inst.requests
                                );
                            }
                        }
                        else {      // abort case
                            net.sendPeerMessage(last_inst.crtLeaderId, new GenericPaxosMessage.Restore(inst_no, inst));  // apply for restoration
                            last_inst.leaderMaintenanceUnit = null;
                            instanceSpace[inst_no] = last_inst;

                            /* after this point, this server will no longer play the role of leader in this client.
                             * ABORT msg will only react once, since control flow will not reach here again.
                             * There must be only ONE leader in the network ! */
                            return true;
                        }
                    }

                    ++diskUnit.prepareResponse;
                }

                /* accumulating until reach Paxos threshold
                 * BROADCASTING_2ndIntegrateWriteAndRead activated only once in each Paxos period (only in PREPARING status) */

                if (inst.leaderMaintenanceUnit.prepareResponse > peerSize / 2) {

                    if (diskUnit.historyMaintenanceUnit != null
                            && diskUnit.historyMaintenanceUnit.HOST_RESTORE) { // restore case: exists formal paxos conversation
                        restoredRequestList.addAll(Arrays.asList(inst.requests));   // restore local requests

                        inst.requests = diskUnit.historyMaintenanceUnit.reservedCmds;
                    }


                    long newDialogue = System.currentTimeMillis();

                    diskUnit.crtDialogue = newDialogue;
                    inst.status = InstanceStatus.PREPARED;

                    net.broadcastPeerMessage(DiskPaxosMessage.IRW(inst_no, serverId, inst_ballot, newDialogue, peerSize, inst));
                }
                return true;
            }
            else
                return false;
        }
        else
            return false;
    }
}
