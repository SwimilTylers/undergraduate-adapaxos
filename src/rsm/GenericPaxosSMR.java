package rsm;

import com.sun.istack.internal.NotNull;
import instance.ClientRequest;
import javafx.util.Pair;
import network.message.protocols.GenericPaxosMessage;
import network.service.GenericNetService;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * @author : Swimiltylers
 * @version : 2019/1/29 11:49
 */
public class GenericPaxosSMR implements Runnable{
    public static final int DEFAULT_INSTANCE_SIZE = 1024;
    public static final int DEFAULT_MESSAGE_SIZE = 32;

    private GenericNetService net;
    private String[] peerAddr;
    private int[] peerPort;
    private BlockingQueue<ClientRequest> cMessages;
    private BlockingQueue<GenericPaxosMessage> pMessage;

    private int serverId;
    private int peerSize;
    private PaxosInstance[] instanceSpace = new PaxosInstance[DEFAULT_INSTANCE_SIZE];
    private int crtInstance = 0;

    private List<ClientRequest> restoredRequestList;

    public GenericPaxosSMR(int id, @NotNull String[] addr, int[] port){
        assert addr.length == port.length;

        peerAddr = addr;
        peerPort = port;
        peerSize = addr.length;
        serverId = id;

        cMessages = new ArrayBlockingQueue<>(DEFAULT_MESSAGE_SIZE);
        pMessage = new ArrayBlockingQueue<>(DEFAULT_MESSAGE_SIZE);

        net = new GenericNetService(id, cMessages, pMessage);

        restoredRequestList = new ArrayList<>();
    }

    public enum InstanceStatus{
        PREPARING, PREPARED, ACCEPTED, COMMITTED
    }

    public static class HistoryMaintenance{
        int maxRecvLeaderId;
        int maxRecvInstBallot;
        ClientRequest[] reservedCmds;
        Set<Pair<Integer, Integer>> received;

        HistoryMaintenance(int initLeaderId, int initInstBallot, ClientRequest[] initCmds){
            maxRecvLeaderId = initLeaderId;
            maxRecvInstBallot = initInstBallot;
            reservedCmds = initCmds;

            received = new HashSet<>();
            received.add(new Pair<>(initLeaderId, initInstBallot));
        }

        /* this initiator is designed for restore-late case */
        HistoryMaintenance(List<ClientRequest> restoredProposals, int initLeaderId, int initInstBallot, ClientRequest[] initCmds){
            maxRecvLeaderId = initLeaderId;
            maxRecvInstBallot = initInstBallot;
            reservedCmds = initCmds;

            if (initCmds != null)
                restoredProposals.addAll(Arrays.asList(reservedCmds));

            received = new HashSet<>();
            received.add(new Pair<>(initLeaderId, initInstBallot));
        }

        void record(List<ClientRequest> restoredProposals, int leaderId, int instBallot, ClientRequest[] cmds){
            if (!received.contains(new Pair<>(leaderId, instBallot))){
                received.add(new Pair<>(leaderId, instBallot));

                if (leaderId > maxRecvLeaderId
                        || (leaderId == maxRecvLeaderId && instBallot > maxRecvInstBallot)){
                    if (reservedCmds != null)
                        restoredProposals.addAll(Arrays.asList(reservedCmds));

                    maxRecvLeaderId = leaderId;
                    maxRecvInstBallot = instBallot;
                    reservedCmds = cmds;

                }
                else if (cmds != null && !received.contains(new Pair<>(leaderId, instBallot)))
                    restoredProposals.addAll(Arrays.asList(cmds));
            }

        }
    }

    public static class LeaderMaintenance {
        HistoryMaintenance historyMaintenanceUnit = null;
        int prepareResponse = 0;
        int acceptResponse = 0;
        int nack = 0;
    }

    public static class PaxosInstance {
        int crtLeaderId;
        InstanceStatus status;
        int crtInstBallot;
        ClientRequest[] cmds;
        LeaderMaintenance leaderMaintenanceUnit;

        PaxosInstance copyOf() {
            PaxosInstance ret = new PaxosInstance();

            ret.crtLeaderId = crtLeaderId;
            ret.status = status;
            ret.crtInstBallot = crtInstBallot;
            ret.cmds = cmds;
            ret.leaderMaintenanceUnit = leaderMaintenanceUnit;

            return ret;
        }

        PaxosInstance() {}
    }

    @Override
    public void run() {
        try {
            net.connect(peerAddr, peerPort);
        } catch (InterruptedException e) {
            System.out.println("Net Connection is interrupted: "+e.getMessage());
            return;
        }

        while (true){
            GenericPaxosMessage msg;
            try {
                msg = pMessage.take();
            } catch (InterruptedException e) {
                System.out.println("Unsuccessfully message taking");
                break;
            }

            if (msg instanceof GenericPaxosMessage.Prepare){
                System.out.println("Receive a Prepare");
                handlePrepare((GenericPaxosMessage.Prepare) msg);
            }
            else if (msg instanceof GenericPaxosMessage.ackPrepare){
                System.out.println("Receive a ackPrepare");
                handleAckPrepare((GenericPaxosMessage.ackPrepare) msg);
            }
            else if (msg instanceof GenericPaxosMessage.Accept){
                System.out.println("Receive a Accept");
                handleAccept((GenericPaxosMessage.Accept) msg);
            }
            else if (msg instanceof GenericPaxosMessage.ackAccept){
                System.out.println("Receive a ackAccept");
                handleAckAccept((GenericPaxosMessage.ackAccept) msg);
            }
            else if (msg instanceof GenericPaxosMessage.Commit){
                System.out.println("Receive a Commit");
                handleCommit((GenericPaxosMessage.Commit) msg);
            }
            else if (msg instanceof GenericPaxosMessage.Restore){
                System.out.println("Receive a Restore");
                handleRestore((GenericPaxosMessage.Restore) msg);
            }
        }
    }

    public void handleRequest(ClientRequest request){

    }

    private void handlePrepare(GenericPaxosMessage.Prepare prepare){
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
            if (inst.status != InstanceStatus.COMMITTED){      // restore-early case
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
            else{   // recovery case

            }
        }
        else if (instanceSpace[prepare.inst_no].crtLeaderId == prepare.leaderId){
            PaxosInstance inst = instanceSpace[prepare.inst_no];
            if (inst.crtInstBallot < prepare.inst_ballot){  // back-online case: catch up with current situation
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

            /* otherwise, drop the message, which is expired */
        }
        else{   // abort case

        }
    }

    private void handleAckPrepare(GenericPaxosMessage.ackPrepare ackPrepare){
        if (instanceSpace[ackPrepare.inst_no] != null
                && instanceSpace[ackPrepare.inst_no].crtLeaderId == serverId){   // on this instance, local server works as a leader

            PaxosInstance inst = instanceSpace[ackPrepare.inst_no];

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
                    if (inst.leaderMaintenanceUnit.historyMaintenanceUnit == null)
                        inst.leaderMaintenanceUnit.historyMaintenanceUnit = new HistoryMaintenance(
                                ackPrepare.load.crtLeaderId,
                                ackPrepare.load.crtInstBallot,
                                ackPrepare.load.cmds
                        );
                    else
                        inst.leaderMaintenanceUnit.historyMaintenanceUnit.record(
                                restoredRequestList,
                                ackPrepare.load.crtLeaderId,
                                ackPrepare.load.crtInstBallot,
                                ackPrepare.load.cmds
                        );
                }
            }

            /* accumulating until reach Paxos threshold
             * BROADCASTING_ACCEPT activated only once in each Paxos period (only in PREPARING status) */

            if (inst.status == InstanceStatus.PREPARING
                    && inst.leaderMaintenanceUnit.prepareResponse > peerSize/2){
                if (inst.leaderMaintenanceUnit.historyMaintenanceUnit != null){ // restore-early case: exists formal paxos conversation
                    restoredRequestList.addAll(Arrays.asList(inst.cmds));   // restore local cmds

                    inst.cmds = inst.leaderMaintenanceUnit.historyMaintenanceUnit.reservedCmds;
                }
                inst.status = InstanceStatus.PREPARED;
                net.broadcastPeerMessage(new GenericPaxosMessage.Accept(ackPrepare.inst_no, serverId, inst.crtInstBallot, inst.cmds));
            }
        }
    }

    private void handleAccept(GenericPaxosMessage.Accept accept){
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
            if (inst.status != InstanceStatus.COMMITTED){
                GenericPaxosMessage.ackAccept reply = new GenericPaxosMessage.ackAccept(    // restore-late case
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
            else {  // recovery case

            }
        }
        else {  // abort case

        }
    }

    private void handleAckAccept(GenericPaxosMessage.ackAccept ackAccept){
        if (instanceSpace[ackAccept.inst_no] != null
                && instanceSpace[ackAccept.inst_no].crtLeaderId == serverId){         // on this instance, local server works as a leader

            PaxosInstance inst = instanceSpace[ackAccept.inst_no];

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
                    if (inst.leaderMaintenanceUnit.historyMaintenanceUnit == null)
                        inst.leaderMaintenanceUnit.historyMaintenanceUnit = new HistoryMaintenance(
                                restoredRequestList,
                                ackAccept.load.crtLeaderId,
                                ackAccept.load.crtInstBallot,
                                ackAccept.load.cmds
                        );
                    else
                        inst.leaderMaintenanceUnit.historyMaintenanceUnit.record(
                                restoredRequestList,
                                ackAccept.load.crtLeaderId,
                                ackAccept.load.crtInstBallot,
                                ackAccept.load.cmds
                        );
                }
            }

            if (inst.status == InstanceStatus.PREPARED
                    && inst.leaderMaintenanceUnit.acceptResponse > peerSize/2){
                inst.status = InstanceStatus.COMMITTED;
                net.broadcastPeerMessage(new GenericPaxosMessage.Commit(ackAccept.inst_no, serverId, inst.crtInstBallot, inst.cmds));
            }
        }
    }

    private void handleCommit(GenericPaxosMessage.Commit commit){
        if (instanceSpace[commit.inst_no] == null){     // back-online case: catch up with current situation
            PaxosInstance inst = new PaxosInstance();
            inst.crtLeaderId = commit.leaderId;
            inst.crtInstBallot = commit.inst_ballot;

            inst.cmds = commit.cmds;
            inst.status = InstanceStatus.COMMITTED;

            instanceSpace[commit.inst_no] = inst;
            System.out.println("successfully committed");
        }
        else{
            PaxosInstance inst = instanceSpace[commit.inst_no];
            if (inst.crtLeaderId == commit.leaderId){      // normal case: whatever the status is, COMMIT msg must follow
                if (inst.crtInstBallot <= commit.inst_ballot){
                    inst.crtInstBallot = commit.inst_ballot;
                    inst.cmds = commit.cmds;
                    inst.status = InstanceStatus.COMMITTED;

                    System.out.println("successfully committed");
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
            }

            /* otherwise, drop the message, which is expired */
        }

    }

    private void handleRestore(GenericPaxosMessage.Restore restore){
        PaxosInstance inst = instanceSpace[restore.inst_no];
        if (inst.leaderMaintenanceUnit != null && restore.load != null){
            if (inst.leaderMaintenanceUnit.historyMaintenanceUnit == null)
                inst.leaderMaintenanceUnit.historyMaintenanceUnit = new HistoryMaintenance(
                        restoredRequestList,
                        restore.load.crtLeaderId,
                        restore.load.crtInstBallot,
                        restore.load.cmds
                );
            else
                inst.leaderMaintenanceUnit.historyMaintenanceUnit.record(
                        restoredRequestList,
                        restore.load.crtLeaderId,
                        restore.load.crtInstBallot,
                        restore.load.cmds
                );
        }
    }
}
