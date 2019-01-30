package rsm;

import com.sun.istack.internal.NotNull;
import instance.ClientRequest;
import javafx.util.Pair;
import network.message.protocols.GenericClientMessage;
import network.message.protocols.GenericPaxosMessage;
import network.service.GenericNetService;

import java.util.HashSet;
import java.util.Set;
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
    private BlockingQueue<GenericClientMessage> cMessages;
    private BlockingQueue<GenericPaxosMessage> pMessage;

    private int serverId;
    private int peerSize;
    private PaxosInstance[] instanceSpace = new PaxosInstance[DEFAULT_INSTANCE_SIZE];
    private int crtInstance = 0;

    public GenericPaxosSMR(int id, @NotNull String[] addr, int[] port){
        assert addr.length == port.length;

        peerAddr = addr;
        peerPort = port;
        peerSize = addr.length;
        serverId = id;

        cMessages = new ArrayBlockingQueue<>(DEFAULT_MESSAGE_SIZE);
        pMessage = new ArrayBlockingQueue<>(DEFAULT_MESSAGE_SIZE);

        net = new GenericNetService(id, cMessages, pMessage);
    }

    public enum InstanceStatus{
        PREPARING, PREPARED, ACCEPTED, COMMITTED
    }

    public static class LeaderMaintenance {
        GenericClientMessage.Propose[] proposes;
        Set<Pair<Integer, Integer>> restoringHistory = new HashSet<>();
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

    public void handlePropose(GenericClientMessage.Propose propose){

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
    }

    private void handleAckPrepare(GenericPaxosMessage.ackPrepare ackPrepare){
        if (instanceSpace[ackPrepare.inst_no] != null
                && instanceSpace[ackPrepare.inst_no].crtLeaderId == serverId){         // on this instance, local server works as a leader
            if (ackPrepare.type == GenericPaxosMessage.ackMessageType.PROCEEDING
                    && ackPrepare.ack_leaderId == serverId
                    && ackPrepare.inst_ballot == instanceSpace[ackPrepare.inst_no].crtInstBallot){  // normal case

                PaxosInstance inst = instanceSpace[ackPrepare.inst_no];
                ++inst.leaderMaintenanceUnit.prepareResponse;

                if (inst.status == InstanceStatus.PREPARING
                        && inst.leaderMaintenanceUnit.prepareResponse > peerSize/2){
                    inst.status = InstanceStatus.PREPARED;
                    net.broadcastPeerMessage(new GenericPaxosMessage.Accept(ackPrepare.inst_no, serverId, inst.crtInstBallot, inst.cmds));
                }
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
            if (inst.crtInstBallot == accept.inst_ballot && inst.status == InstanceStatus.PREPARED){  // normal case
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

        }
    }

    private void handleAckAccept(GenericPaxosMessage.ackAccept ackAccept){
        if (instanceSpace[ackAccept.inst_no] != null
                && instanceSpace[ackAccept.inst_no].crtLeaderId == serverId){         // on this instance, local server works as a leader
            if (ackAccept.type == GenericPaxosMessage.ackMessageType.PROCEEDING
                    && ackAccept.ack_leaderId == serverId
                    && ackAccept.inst_ballot == instanceSpace[ackAccept.inst_no].crtInstBallot){  // normal case

                PaxosInstance inst = instanceSpace[ackAccept.inst_no];
                ++inst.leaderMaintenanceUnit.acceptResponse;

                if (inst.status == InstanceStatus.PREPARED
                        && inst.leaderMaintenanceUnit.acceptResponse > peerSize/2){
                    inst.status = InstanceStatus.COMMITTED;
                    net.broadcastPeerMessage(new GenericPaxosMessage.Commit(ackAccept.inst_no, serverId, inst.crtInstBallot, inst.cmds));
                }
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
        else if (instanceSpace[commit.inst_no].crtLeaderId == commit.leaderId){
            PaxosInstance inst = instanceSpace[commit.inst_no];
            if (inst.crtInstBallot == commit.inst_ballot && inst.status == InstanceStatus.ACCEPTED){
                inst.cmds = commit.cmds;
                inst.status = InstanceStatus.COMMITTED;

                System.out.println("successfully committed");
            }
        }
    }

    private void handleRestore(GenericPaxosMessage.Restore restore){

    }
}
