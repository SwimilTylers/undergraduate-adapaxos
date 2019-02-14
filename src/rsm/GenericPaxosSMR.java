package rsm;

import com.sun.istack.internal.NotNull;
import client.ClientRequest;
import logger.NaiveLogger;
import logger.PaxosLogger;
import network.message.protocols.GenericPaxosMessage;
import network.service.GenericNetService;
import agent.acceptor.Acceptor;
import agent.learner.Learner;
import agent.acceptor.GenericAcceptor;
import agent.learner.GenericLearner;
import agent.proposer.GenericProposer;
import agent.proposer.Proposer;
import instance.PaxosInstance;
import instance.maintenance.HistoryMaintenance;

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
    private int excInstance = 0;

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
}
