package rsm;

import com.sun.istack.internal.NotNull;
import client.ClientRequest;
import javafx.util.Pair;
import logger.NaiveLogger;
import logger.PaxosLogger;
import network.message.protocols.GenericPaxosMessage;
import network.service.GenericNetService;
import rsm.agent.Acceptor;
import rsm.agent.Learner;
import rsm.agent.Proposer;

import java.util.*;
import java.util.concurrent.*;

/**
 * @author : Swimiltylers
 * @version : 2019/1/29 11:49
 */
public class GenericPaxosSMR implements Runnable{
    public static final int DEFAULT_INSTANCE_SIZE = 1024;
    public static final int DEFAULT_MESSAGE_SIZE = 32;
    public static final int DEFAULT_REQUEST_COMPACTING_SIZE = 48;
    public static final int DEFAULT_COMPACT_INTERVAL = 5000;
    public static final int DEFAULT_CLIENT_COM_WAITING = 50;
    public static final int DEFAULT_PEER_COM_WAITING = 50;

    private GenericNetService net;
    private String[] peerAddr;
    private int[] peerPort;

    private BlockingQueue<ClientRequest[]> compactChan;
    private int compactInterval;

    private int clientComWaiting;
    private int peerComWaiting;

    private BlockingQueue<ClientRequest> cMessages;
    private BlockingQueue<GenericPaxosMessage> pMessage;

    private int serverId;
    private int peerSize;
    private PaxosInstance[] instanceSpace = new PaxosInstance[DEFAULT_INSTANCE_SIZE];
    private int crtInstance = 0;
    private int excInstance = 0;
    private int crtBallot = 0;

    private PaxosLogger logger;

    private Proposer proposer;
    private Acceptor acceptor;
    private Learner learner;

    private List<ClientRequest> restoredRequestList;

    public GenericPaxosSMR(int id, @NotNull String[] addr, int[] port){
        assert addr.length == port.length;

        peerAddr = addr;
        peerPort = port;
        peerSize = addr.length;
        serverId = id;

        compactChan = new ArrayBlockingQueue<>(1);

        compactInterval = DEFAULT_COMPACT_INTERVAL;
        clientComWaiting = DEFAULT_CLIENT_COM_WAITING;
        peerComWaiting = DEFAULT_PEER_COM_WAITING;

        cMessages = new ArrayBlockingQueue<>(DEFAULT_MESSAGE_SIZE);
        pMessage = new ArrayBlockingQueue<>(DEFAULT_MESSAGE_SIZE);

        logger = new NaiveLogger(id);
        net = new GenericNetService(id, GenericNetService.DEFAULT_TO_CLIENT_PORT, cMessages, pMessage, logger);

        restoredRequestList = new ArrayList<>();
    }

    public enum InstanceStatus{
        PREPARING, PREPARED, ACCEPTED, COMMITTED
    }

    public static class HistoryMaintenance{
        int maxRecvLeaderId;
        int maxRecvInstBallot;
        boolean HOST_RESTORE;
        ClientRequest[] reservedCmds;
        Set<Pair<Integer, Integer>> received;

        HistoryMaintenance(int initLeaderId, int initInstBallot, ClientRequest[] initCmds){
            maxRecvLeaderId = initLeaderId;
            maxRecvInstBallot = initInstBallot;
            reservedCmds = initCmds;

            HOST_RESTORE = true;

            received = new HashSet<>();
            received.add(new Pair<>(initLeaderId, initInstBallot));
        }

        /* this initiator is designed for restore-late case */
        HistoryMaintenance(List<ClientRequest> restoredProposals, int initLeaderId, int initInstBallot, ClientRequest[] initCmds){
            maxRecvLeaderId = -1;
            maxRecvInstBallot = -1;
            reservedCmds = null;

            if (initCmds != null)
                restoredProposals.addAll(Arrays.asList(initCmds));

            HOST_RESTORE = false;

            received = new HashSet<>();
            received.add(new Pair<>(initLeaderId, initInstBallot));
        }

        void record(List<ClientRequest> restoredProposals, int leaderId, int instBallot, ClientRequest[] cmds){
            if (!received.contains(new Pair<>(leaderId, instBallot))){
                received.add(new Pair<>(leaderId, instBallot));

                HOST_RESTORE = true;

                if (leaderId > maxRecvLeaderId
                        || (leaderId == maxRecvLeaderId && instBallot > maxRecvInstBallot)){
                    if (reservedCmds != null)
                        restoredProposals.addAll(Arrays.asList(reservedCmds));

                    maxRecvLeaderId = leaderId;
                    maxRecvInstBallot = instBallot;
                    reservedCmds = cmds;

                }
                else if (cmds != null)
                    restoredProposals.addAll(Arrays.asList(cmds));
            }

        }

        void restore(List<ClientRequest> restoredProposals, int leaderId, int instBallot, ClientRequest[] cmds){
            if (!received.contains(new Pair<>(leaderId, instBallot))){
                received.add(new Pair<>(leaderId, instBallot));

                if (cmds != null)
                    restoredProposals.addAll(Arrays.asList(cmds));
            }
        }
    }

    public static class LeaderMaintenance {
        HistoryMaintenance historyMaintenanceUnit = null;
        int prepareResponse = 0;
        int acceptResponse = 0;
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
    }

    private boolean isLeader(int inst_no){
        return serverId == 0;
    }

    @Override
    public void run() {
        try {
            net.connect(peerAddr, peerPort);
        } catch (InterruptedException e) {
            System.out.println("Net Connection is interrupted: "+e.getMessage());
            return;
        }

        proposer = new GenericProposer(serverId, peerSize, instanceSpace, net, restoredRequestList);
        acceptor = new GenericAcceptor(instanceSpace, net);
        learner = new GenericLearner(serverId, peerSize, instanceSpace, net, restoredRequestList, logger);

        ExecutorService service = Executors.newCachedThreadPool();

        if (isLeader(0)) {
            System.out.println("Server-"+serverId+" is watching");
            service.execute(() -> net.watch());
        }

        service.execute(this::compact);
        service.execute(this::paxosRoutine);

        service.shutdown();
    }

    private void compact(){
        while (true){
            int cMessageSize = cMessages.size();

            for (int i = 0; i < cMessageSize; i++) {
                try {
                    restoredRequestList.add(cMessages.take());
                } catch (InterruptedException ignored) {}
            }

            if (!restoredRequestList.isEmpty()) {
                Collections.shuffle(restoredRequestList);
                int compactSize = restoredRequestList.size() < DEFAULT_REQUEST_COMPACTING_SIZE
                        ? restoredRequestList.size()
                        : DEFAULT_INSTANCE_SIZE;

                ClientRequest[] requests = restoredRequestList.subList(0, compactSize).toArray(new ClientRequest[compactSize]);

                if (restoredRequestList.size() == compactSize)
                    restoredRequestList.clear();
                else
                    restoredRequestList = restoredRequestList.subList(compactSize, restoredRequestList.size());

                try {
                    compactChan.put(requests); // slow down if congestion happens

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            try {
                Thread.sleep(compactInterval); // drop the refreshing frequency of 'compact'
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void peerConversation(){
        GenericPaxosMessage msg;
        try {
            msg = pMessage.poll(peerComWaiting, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            System.out.println("Unsuccessfully message taking");
            return;
        }

        if (msg != null) {
            if (msg instanceof GenericPaxosMessage.Prepare) {
                GenericPaxosMessage.Prepare cast = (GenericPaxosMessage.Prepare) msg;
                logger.logPrepare(cast.inst_no, cast, "handle");
                acceptor.handlePrepare(cast);
                logger.logPrepare(cast.inst_no, cast, "exit handle");
            } else if (msg instanceof GenericPaxosMessage.ackPrepare) {
                GenericPaxosMessage.ackPrepare cast = (GenericPaxosMessage.ackPrepare) msg;
                logger.logAckPrepare(cast.inst_no, cast, "handle");
                proposer.handleAckPrepare(cast);
                logger.logAckPrepare(cast.inst_no, cast, "exit handle");
            } else if (msg instanceof GenericPaxosMessage.Accept) {
                GenericPaxosMessage.Accept cast = (GenericPaxosMessage.Accept) msg;
                logger.logAccept(cast.inst_no, cast, "handle");
                acceptor.handleAccept(cast);
                logger.logAccept(cast.inst_no, cast, "exit handle");
            } else if (msg instanceof GenericPaxosMessage.ackAccept) {
                GenericPaxosMessage.ackAccept cast = (GenericPaxosMessage.ackAccept) msg;
                logger.logAckAccept(cast.inst_no, cast, "handle");
                learner.handleAckAccept(cast);
                logger.logAckAccept(cast.inst_no, cast, "exit handle");
            } else if (msg instanceof GenericPaxosMessage.Commit) {
                GenericPaxosMessage.Commit cast = (GenericPaxosMessage.Commit) msg;
                logger.logCommit(cast.inst_no, cast, "handle");
                learner.handleCommit(cast);
                logger.logCommit(cast.inst_no, cast, "exit handle");
            } else if (msg instanceof GenericPaxosMessage.Restore) {
                GenericPaxosMessage.Restore cast = (GenericPaxosMessage.Restore) msg;
                logger.logRestore(cast.inst_no, cast, "handle");
                handleRestore(cast);
                logger.logRestore(cast.inst_no, cast, "exit handle");
            }
        }
    }

    private void paxosRoutine(){
        while (true) {
            try {
                peerConversation();
            } catch (Exception ignored){}

            ClientRequest[] compact = null;
            try {
                compact = compactChan.poll(clientComWaiting, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (compact != null)
                proposer.handleRequests(compact);
        }
    }

    private void handleRestore(GenericPaxosMessage.Restore restore){
        PaxosInstance inst = instanceSpace[restore.inst_no];
        if (inst.leaderMaintenanceUnit != null && restore.load != null){ // a meaningful restoration request
            if (inst.leaderMaintenanceUnit.historyMaintenanceUnit == null)
                /* watch out for the constructor
                * it is a restore-late-style one */
                inst.leaderMaintenanceUnit.historyMaintenanceUnit = new HistoryMaintenance(
                        restoredRequestList,
                        restore.load.crtLeaderId,
                        restore.load.crtInstBallot,
                        restore.load.cmds
                );
            else
                inst.leaderMaintenanceUnit.historyMaintenanceUnit.restore(
                        restoredRequestList,
                        restore.load.crtLeaderId,
                        restore.load.crtInstBallot,
                        restore.load.cmds
                );
        }
    }

    public static class GenericProposer implements Proposer{
        private GenericNetService net;
        private List<ClientRequest> restoredRequestList;

        private int serverId;
        private int peerSize;

        private PaxosInstance[] instanceSpace;
        private int crtInstance = 0;

        private int crtBallot = 0;

        public GenericProposer(int serverId, int peerSize,
                               @NotNull PaxosInstance[] instanceSpace,
                               @NotNull GenericNetService net,
                               @NotNull List<ClientRequest> restoredRequestList) {
            this.serverId = serverId;
            this.peerSize = peerSize;
            this.instanceSpace = instanceSpace;
            this.net = net;
            this.restoredRequestList = restoredRequestList;
        }

        @Override
        public void handleRequests(ClientRequest[] requests) {
            PaxosInstance inst = new PaxosInstance();

            inst.crtLeaderId = serverId;
            inst.crtInstBallot = ++crtBallot;
            inst.cmds = requests;
            inst.status = InstanceStatus.PREPARING;
            inst.leaderMaintenanceUnit = new LeaderMaintenance();

            instanceSpace[crtInstance] = inst;

            net.broadcastPeerMessage(new GenericPaxosMessage.Prepare(crtInstance, inst.crtLeaderId, inst.crtInstBallot));

            ++crtInstance;
        }

        @Override
        public void handleAckPrepare(GenericPaxosMessage.ackPrepare ackPrepare) {
            if (instanceSpace[ackPrepare.inst_no] != null
                    && instanceSpace[ackPrepare.inst_no].crtLeaderId == serverId){   // on this client, local server works as a leader

                PaxosInstance inst = instanceSpace[ackPrepare.inst_no];

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
                            if (inst.leaderMaintenanceUnit.historyMaintenanceUnit == null)
                                /* watch out for the constructor
                                 * it is a restore-early-style one */
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

                    if (inst.status == InstanceStatus.PREPARING     // check status to avoid broadcasting duplicated ACCEPT
                            && inst.leaderMaintenanceUnit.prepareResponse > peerSize/2){
                        if (inst.leaderMaintenanceUnit.historyMaintenanceUnit != null
                                && inst.leaderMaintenanceUnit.historyMaintenanceUnit.HOST_RESTORE){ // restore-early case: exists formal paxos conversation
                            restoredRequestList.addAll(Arrays.asList(inst.cmds));   // restore local cmds

                            inst.cmds = inst.leaderMaintenanceUnit.historyMaintenanceUnit.reservedCmds;
                        }
                        inst.status = InstanceStatus.PREPARED;
                        net.broadcastPeerMessage(new GenericPaxosMessage.Accept(ackPrepare.inst_no, serverId, inst.crtInstBallot, inst.cmds));
                    }
                }
                else if (ackPrepare.type == GenericPaxosMessage.ackMessageType.RECOVER){
                    if (inst.status == InstanceStatus.PREPARING){   // recovery case: check status to avoid broadcasting duplicated COMMIT
                        restoredRequestList.addAll(Arrays.asList(inst.cmds));

                        inst.cmds = ackPrepare.load.cmds;
                        inst.status = InstanceStatus.COMMITTED;

                        net.broadcastPeerMessage(new GenericPaxosMessage.Commit(ackPrepare.inst_no, serverId, inst.crtInstBallot, inst.cmds));
                    }
                }
                else if (ackPrepare.type == GenericPaxosMessage.ackMessageType.ABORT){  // abort case
                    net.sendPeerMessage(ackPrepare.load.crtLeaderId, new GenericPaxosMessage.Restore(ackPrepare.inst_no, inst));  // apply for restoration

                    instanceSpace[ackPrepare.inst_no] = ackPrepare.load;

                    /* after this point, this server will no longer play the role of leader in this client.
                     * ABORT msg will only react once, since control flow will not reach here again.
                     * There must be only ONE leader in the network ! */
                }
            }
        }
    }

    public static class GenericAcceptor implements Acceptor{
        private GenericNetService net;
        private PaxosInstance[] instanceSpace;

        public GenericAcceptor(PaxosInstance[] instanceSpace, GenericNetService net) {
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

    public static class GenericLearner implements Learner{
        private GenericNetService net;

        private int serverId;
        private int peerSize;

        private PaxosInstance[] instanceSpace;

        private List<ClientRequest> restoredRequestList;

        private PaxosLogger logger;

        public GenericLearner(int serverId, int peerSize,
                              @NotNull PaxosInstance[] instanceSpace,
                              @NotNull GenericNetService net,
                              @NotNull List<ClientRequest> restoredRequestList,
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

                PaxosInstance inst = instanceSpace[ackAccept.inst_no];

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
                            if (inst.leaderMaintenanceUnit.historyMaintenanceUnit == null)
                                /* watch out for the constructor
                                 * it is a restore-late-style one */
                                inst.leaderMaintenanceUnit.historyMaintenanceUnit = new HistoryMaintenance(
                                        restoredRequestList,
                                        ackAccept.load.crtLeaderId,
                                        ackAccept.load.crtInstBallot,
                                        ackAccept.load.cmds
                                );
                            else
                                inst.leaderMaintenanceUnit.historyMaintenanceUnit.restore(
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

                        GenericPaxosMessage.Commit sendOut = new GenericPaxosMessage.Commit(ackAccept.inst_no, serverId, inst.crtInstBallot, inst.cmds);
                        logger.logCommit(ackAccept.inst_no, sendOut, "settled");
                        net.broadcastPeerMessage(sendOut);
                    }
                }
                else if (ackAccept.type == GenericPaxosMessage.ackMessageType.RECOVER){ // recovery case
                    /* vacant, due to the property mentioned in handleAccept.[recovery case] */
                }
                else if (ackAccept.type == GenericPaxosMessage.ackMessageType.ABORT){   // abort case
                    net.sendPeerMessage(ackAccept.load.crtLeaderId, new GenericPaxosMessage.Restore(ackAccept.inst_no, inst));  // apply for restoration

                    instanceSpace[ackAccept.inst_no] = ackAccept.load;

                    /* after this point, this server will no longer play the role of leader in this client.
                     * ABORT msg will only react once, since control flow will not reach here again.
                     * There must be only ONE leader in the network ! */
                }
            }
        }

        @Override
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
}
