package rsm;

import com.sun.istack.internal.NotNull;
import client.ClientRequest;
import javafx.util.Pair;
import logger.NaiveLogger;
import logger.PaxosLogger;
import network.message.protocols.Distinguishable;
import network.message.protocols.GenericPaxosMessage;
import network.service.GenericNetService;
import instance.PaxosInstance;
import instance.maintenance.HistoryMaintenance;

import java.util.*;
import java.util.concurrent.*;

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

    protected int serverId;
    protected int peerSize;
    protected PaxosInstance[] instanceSpace = new PaxosInstance[DEFAULT_INSTANCE_SIZE];
    private int excInstance = 0;

    protected PaxosLogger logger;

    protected List<ClientRequest> restoredRequestList;

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

        customizedChannels = new ArrayList<>();

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

        registerChannels();
        agentDeployment();

        ExecutorService service = Executors.newCachedThreadPool();

        if (isLeader(0)) {
            System.out.println("Server-"+serverId+" is watching");
            service.execute(() -> net.watch());
        }

        service.execute(this::compact);
        service.execute(this::paxosRoutine);

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
        PaxosInstance inst = instanceSpace[restore.inst_no];
        if (inst.leaderMaintenanceUnit != null && restore.load != null){ // a meaningful restoration request
            inst.leaderMaintenanceUnit.historyMaintenanceUnit = HistoryMaintenance.restoreHelper(
                    inst.leaderMaintenanceUnit.historyMaintenanceUnit,
                    HistoryMaintenance.RESTORE_TYPE.LATE,
                    restoredRequestList,
                    restore.load.crtLeaderId,
                    restore.load.crtInstBallot,
                    restore.load.cmds
            );
        }
    }
}
