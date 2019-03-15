package rsm;

import com.sun.istack.internal.NotNull;
import client.ClientRequest;
import instance.InstanceStatus;
import instance.StaticPaxosInstance;
import javafx.util.Pair;
import logger.NaiveLogger;
import logger.PaxosLogger;
import network.message.protocols.Distinguishable;
import network.message.protocols.GenericPaxosMessage;
import network.service.GenericNetService;
import instance.maintenance.HistoryMaintenance;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author : Swimiltylers
 * @version : 2019/1/29 11:49
 */
abstract public class GenericPaxosSMR implements Runnable{
    public static final int DEFAULT_INSTANCE_SIZE = 1024;
    public static final int DEFAULT_MESSAGE_SIZE = 32;
    public static final int DEFAULT_REQUEST_COMPACTING_SIZE = 48;
    public static final int DEFAULT_COMPACT_INTERVAL = 5000;
    public static final int DEFAULT_CLIENT_COM_WAITING = 50;
    public static final int DEFAULT_PEER_COM_WAITING = 50;
    public static final int DEFAULT_BATCH_CHAN_SIZE = 1;

    protected GenericNetService net;
    private String[] peerAddr;
    private int[] peerPort;

    protected BlockingQueue<ClientRequest[]> compactChan;
    private int compactInterval;

    protected int clientComWaiting;
    protected int peerComWaiting;

    private BlockingQueue<ClientRequest> cMessages;
    protected BlockingQueue<GenericPaxosMessage> pMessage;

    protected List<Pair<Distinguishable, BlockingQueue>> customizedChannels;
    protected List<Runnable> customizedRoutines;

    protected int serverId;
    protected int peerSize;
    protected StaticPaxosInstance[] instanceSpace = new StaticPaxosInstance[DEFAULT_INSTANCE_SIZE];
    protected AtomicInteger crtBallot = new AtomicInteger(0);

    protected AtomicInteger maxInstance = new AtomicInteger(0);
    protected AtomicInteger consecutiveCommit = new AtomicInteger(0);

    protected PaxosLogger logger;

    protected Queue<ClientRequest> restoredRequestList;

    public GenericPaxosSMR(int id, @NotNull String[] addr, int[] port){
        assert addr.length == port.length;

        peerAddr = addr;
        peerPort = port;
        peerSize = addr.length;
        serverId = id;

        compactChan = new ArrayBlockingQueue<>(DEFAULT_BATCH_CHAN_SIZE);

        compactInterval = DEFAULT_COMPACT_INTERVAL;
        clientComWaiting = DEFAULT_CLIENT_COM_WAITING;
        peerComWaiting = DEFAULT_PEER_COM_WAITING;

        cMessages = new ArrayBlockingQueue<>(DEFAULT_MESSAGE_SIZE);
        pMessage = new ArrayBlockingQueue<>(DEFAULT_MESSAGE_SIZE);

        customizedChannels = new ArrayList<>();
        customizedRoutines = new ArrayList<>();

        logger = new NaiveLogger(id);
        net = new GenericNetService(id, GenericNetService.DEFAULT_TO_CLIENT_PORT, cMessages, pMessage, logger);

        restoredRequestList = new ConcurrentLinkedQueue<>();
    }

    public GenericPaxosSMR(int id, @NotNull String[] addr, int[] port, PaxosLogger logger){
        assert addr.length == port.length;

        peerAddr = addr;
        peerPort = port;
        peerSize = addr.length;
        serverId = id;

        compactChan = new ArrayBlockingQueue<>(DEFAULT_BATCH_CHAN_SIZE);

        compactInterval = DEFAULT_COMPACT_INTERVAL;
        clientComWaiting = DEFAULT_CLIENT_COM_WAITING;
        peerComWaiting = DEFAULT_PEER_COM_WAITING;

        cMessages = new ArrayBlockingQueue<>(DEFAULT_MESSAGE_SIZE);
        pMessage = new ArrayBlockingQueue<>(DEFAULT_MESSAGE_SIZE);

        customizedChannels = new ArrayList<>();

        this.logger = logger;
        net = new GenericNetService(id, GenericNetService.DEFAULT_TO_CLIENT_PORT, cMessages, pMessage, logger);

        restoredRequestList = new ConcurrentLinkedQueue<>();
    }

    protected boolean isLeader(int inst_no){
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

        registerChannels();
        agentDeployment();

        ExecutorService service = Executors.newCachedThreadPool();

        if (isLeader(0)) {
            System.out.println("Server-"+serverId+" is watching");
            service.execute(() -> net.watch());
        }

        service.execute(this::compact);
        service.execute(this::paxosRoutine);
        customizedRoutines.forEach(service::execute);

        service.shutdown();
    }

    private void registerChannels(){
        for (Pair<Distinguishable, BlockingQueue> chan : customizedChannels) {
            net.registerChannel(chan.getKey(), chan.getValue());
        }
    }

    abstract protected void agentDeployment();

    private void compact(){
        while (true){
            int cMessageSize = cMessages.size();
            List<ClientRequest> requestList = new ArrayList<>();

            cMessageSize = Integer.min(cMessageSize, DEFAULT_REQUEST_COMPACTING_SIZE);

            for (int i = 0; i < cMessageSize; i++) {
                try {
                    requestList.add(cMessages.take());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }


            int count = cMessageSize;
            ClientRequest recv = restoredRequestList.poll();
            while(count < DEFAULT_REQUEST_COMPACTING_SIZE && recv != null){
                requestList.add(recv);
                recv = restoredRequestList.poll();
                ++count;
            }


            ClientRequest[] requests = requestList.toArray(new ClientRequest[0]);
            if (requests.length != 0) {
                //logger.log(true, "tete="+requests.length+"\n");
                try {
                    compactChan.put(requests);
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

    private void paxosRoutine(){
        while (true) {
            try {
                peerConversation();
            } catch (Exception ignored){}

            clientConversation();
        }
    }

    abstract protected void peerConversation();

    abstract protected void clientConversation();

    protected void handleRestore(GenericPaxosMessage.Restore restore){
        StaticPaxosInstance inst = instanceSpace[restore.inst_no];
        if (inst.leaderMaintenanceUnit != null && restore.load != null){ // a meaningful restoration request
            inst.leaderMaintenanceUnit.historyMaintenanceUnit = HistoryMaintenance.restoreHelper(
                    inst.leaderMaintenanceUnit.historyMaintenanceUnit,
                    HistoryMaintenance.RESTORE_TYPE.LATE,
                    restoredRequestList,
                    restore.load.crtLeaderId,
                    restore.load.crtInstBallot,
                    restore.load.requests
            );
        }
    }

    protected void updateConsecutiveCommit(){
        int iter = consecutiveCommit.get();
        while (iter < maxInstance.get()
                && instanceSpace[iter] != null
                && instanceSpace[iter].status == InstanceStatus.COMMITTED)
            ++iter;
        consecutiveCommit.set(iter);
    }
}
