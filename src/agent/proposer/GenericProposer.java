package agent.proposer;

import client.ClientRequest;
import com.sun.istack.internal.NotNull;
import instance.StaticPaxosInstance;
import network.message.protocols.GenericPaxosMessage;
import instance.InstanceStatus;
import instance.maintenance.HistoryMaintenance;
import instance.maintenance.LeaderMaintenance;
import network.service.sender.PeerMessageSender;

import java.util.Arrays;
import java.util.Queue;

/**
 * @author : Swimiltylers
 * @version : 2019/2/14 18:57
 */
public class GenericProposer implements Proposer {
    private PeerMessageSender net;
    private Queue<ClientRequest> restoredRequestList;

    private int serverId;
    private int peerSize;

    private StaticPaxosInstance[] instanceSpace;

    public GenericProposer(int serverId, int peerSize,
                           @NotNull StaticPaxosInstance[] instanceSpace,
                           @NotNull PeerMessageSender net,
                           @NotNull Queue<ClientRequest> restoredRequestList) {
        this.serverId = serverId;
        this.peerSize = peerSize;
        this.instanceSpace = instanceSpace;
        this.net = net;
        this.restoredRequestList = restoredRequestList;
    }

    @Override
    public void handleRequests(int inst_no, int ballot, ClientRequest[] requests) {
        StaticPaxosInstance inst = new StaticPaxosInstance();

        inst.crtLeaderId = serverId;
        inst.crtInstBallot = ballot;
        inst.requests = requests;
        inst.status = InstanceStatus.PREPARING;
        inst.leaderMaintenanceUnit = new LeaderMaintenance();

        instanceSpace[inst_no] = inst;
        //System.out.println("eueu="+Arrays.toString(inst.requests));

        net.broadcastPeerMessage(new GenericPaxosMessage.Prepare(inst_no, inst.crtLeaderId, inst.crtInstBallot));
    }

    @Override
    public void handleAckPrepare(GenericPaxosMessage.ackPrepare ackPrepare) {
        if (instanceSpace[ackPrepare.inst_no] != null
                && instanceSpace[ackPrepare.inst_no].crtLeaderId == serverId){   // on this instance, local server works as a leader

            StaticPaxosInstance inst = instanceSpace[ackPrepare.inst_no];

            if (ackPrepare.type == GenericPaxosMessage.ackMessageType.PROCEEDING || ackPrepare.type == GenericPaxosMessage.ackMessageType.RESTORE){
                if (ackPrepare.type == GenericPaxosMessage.ackMessageType.PROCEEDING
                        && ackPrepare.ack_leaderId == serverId
                        && ackPrepare.inst_ballot == inst.crtInstBallot){  // normal case

                    ++inst.leaderMaintenanceUnit.prepareResponse;
                }
                else if (ackPrepare.type == GenericPaxosMessage.ackMessageType.RESTORE
                        && ackPrepare.ack_leaderId == serverId
                        && ackPrepare.inst_ballot == inst.crtInstBallot){  // restore-early case

                    ++inst.leaderMaintenanceUnit.prepareResponse;

                    if (ackPrepare.load != null){     // a meaningful restoration request
                        inst.leaderMaintenanceUnit.historyMaintenanceUnit = HistoryMaintenance.restoreHelper(
                                inst.leaderMaintenanceUnit.historyMaintenanceUnit,
                                HistoryMaintenance.RESTORE_TYPE.EARLY,
                                restoredRequestList,
                                ackPrepare.load.crtLeaderId,
                                ackPrepare.load.crtInstBallot,
                                ackPrepare.load.requests
                        );
                    }
                }

                /* accumulating until reach Paxos threshold
                 * BROADCASTING_ACCEPT activated only once in each Paxos period (only in PREPARING status) */

                if (inst.status == InstanceStatus.PREPARING     // check status to avoid broadcasting duplicated ACCEPT
                        && inst.leaderMaintenanceUnit.prepareResponse > peerSize/2){
                    if (inst.leaderMaintenanceUnit.historyMaintenanceUnit != null
                            && inst.leaderMaintenanceUnit.historyMaintenanceUnit.HOST_RESTORE){ // restore-early case: exists formal paxos conversation
                        restoredRequestList.addAll(Arrays.asList(inst.requests));   // restore local requests

                        inst.requests = inst.leaderMaintenanceUnit.historyMaintenanceUnit.reservedCmds;
                    }
                    inst.status = InstanceStatus.PREPARED;
                    net.broadcastPeerMessage(new GenericPaxosMessage.Accept(ackPrepare.inst_no, serverId, inst.crtInstBallot, inst.requests));
                }
            }
            else if (ackPrepare.type == GenericPaxosMessage.ackMessageType.RECOVER){
                if (inst.status == InstanceStatus.PREPARING){   // recovery case: check status to avoid broadcasting duplicated COMMIT
                    restoredRequestList.addAll(Arrays.asList(inst.requests));

                    inst.requests = ackPrepare.load.requests;
                    inst.status = InstanceStatus.COMMITTED;

                    net.broadcastPeerMessage(new GenericPaxosMessage.Commit(ackPrepare.inst_no, serverId, inst.crtInstBallot, inst.requests));
                }
            }
            else if (ackPrepare.type == GenericPaxosMessage.ackMessageType.ABORT){  // abort case
                net.sendPeerMessage(ackPrepare.load.crtLeaderId, new GenericPaxosMessage.Restore(ackPrepare.inst_no, inst));  // apply for restoration
                ((StaticPaxosInstance)ackPrepare.load).leaderMaintenanceUnit = null;
                instanceSpace[ackPrepare.inst_no] = (StaticPaxosInstance) ackPrepare.load;

                /* after this point, this server will no longer play the role of leader in this client.
                 * ABORT msg will only react once, since control flow will not reach here again.
                 * There must be only ONE leader in the network ! */
            }
        }
    }
}
