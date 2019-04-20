package rsm;

import client.ClientRequest;
import instance.InstanceStatus;
import instance.store.InstanceStore;
import instance.store.OffsetIndexStore;
import logger.NaiveLogger;
import logger.PaxosLogger;
import network.message.protocols.AdaPaxosMessage;
import network.service.GenericNetService;
import network.service.module.connection.ConnectionModule;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author : Swimiltylers
 * @version : 2019/3/7 19:17
 */
public class PseudoAdaPaxosRSM extends GenericPaxosSMR{
    public static final int DEFAULT_SYNC_INTERVAL = 500;
    public static final int DEFAULT_CONN_INTERVAL = GenericNetService.DEFAULT_BEACON_INTERVAL;
    private int sync_interval = DEFAULT_SYNC_INTERVAL;
    private int conn_interval = DEFAULT_CONN_INTERVAL;

    public static final int DEFAULT_STABLE_TIMEOUT = DEFAULT_CONN_INTERVAL * 3;
    public static final int DEFAULT_STABLE_THRESHOLD = 3;
    private int stable_timeout = DEFAULT_STABLE_TIMEOUT;
    private int stable_threshold = DEFAULT_STABLE_THRESHOLD;

    private BasicPaxosRSM mProxy;
    private DiskPaxosRSM dProxy;

    private boolean asLeader;

    /* storage-related fields */
    private InstanceStore store;    // storage
    private AtomicBoolean slowMode;       // true when switch to slow-mode
    private volatile int fsyncStart;         // where fsync start persisting
    public static final int DEFAULT_FSYNC_WIN = 20;
    private final int fsyncWinSize = DEFAULT_FSYNC_WIN;      // max number of one fsync
    private boolean[] fsyncSkip;    // true when a COMMITTED-inst has already fsync-ed

    /* connection-related fields */
    private PaxosLogger logger;
    private ConnectionModule conn;
    private int stableCount;

    private BlockingQueue<AdaPaxosMessage> aMessages;

    public PseudoAdaPaxosRSM(int id, String[] addr, int[] port, boolean asLeader) {
        super(id, addr, port);

        this.asLeader = asLeader;

        slowMode = new AtomicBoolean(false);
        store = new OffsetIndexStore("disk-"+id);
        fsyncStart = 0;
        fsyncSkip = new boolean[instanceSpace.length];

        logger = new NaiveLogger(id);

        mProxy = new BasicPaxosRSM(id, addr, port, logger);
        dProxy = new DiskPaxosRSM(id, addr, port, store, logger);

        conn = null;
        aMessages = new ArrayBlockingQueue<>(DEFAULT_MESSAGE_SIZE);
        net.registerChannel(o -> o instanceof AdaPaxosMessage, aMessages);

        customizedRoutines.add(()->{
            while (true){
                fileSynchronize();
                try {
                    Thread.sleep(sync_interval);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }
            }
        });
        if (asLeader){
            conn = net.getConnectionModule();
            customizedRoutines.add(()->{
                while (true){
                    try{
                        connAnalysis();
                        Thread.sleep(conn_interval);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        break;
                    }
                }
            });
        }
    }

    @Override
    protected boolean isLeader(int inst_no) {
        return asLeader;
    }

    private void switchToMemory(){
        slowMode.set(false);
//        net.getPeerMessageSender().broadcastPeerMessage(new AdaPaxosMessage(false, upto));

        // TODO: store FAST_MODE
    }

    private void switchToDisk(){
        slowMode.set(true);
//        net.getPeerMessageSender().broadcastPeerMessage(new AdaPaxosMessage(true, upto));
        fileSynchronize();

        // TODO: store SLOW_MODE
    }

    private void connAnalysis(){
        int[] lost = conn.filter(stable_timeout);
        if (lost.length <= peerSize/2+1){
            stableCount = 0;
            if (!slowMode.get()) {
                switchToDisk();
            }
        }
        else {
            if (slowMode.get() && stableCount > stable_threshold){
                switchToMemory();
            }
            ++stableCount;
        }
    }

    private synchronized void fileSynchronize(){
        int iter = fsyncStart;
        while(iter < consecutiveCommit.get()){
            if (!fsyncSkip[iter]){
                fsyncSkip[iter] = instanceSpace[iter].status == InstanceStatus.COMMITTED;
                store.store(instanceSpace[iter].crtLeaderId, iter, instanceSpace[iter]);
            }
        }
        fsyncStart = iter;
        while (iter < maxInstance.get()){

        }
    }

    @Override
    protected void agentDeployment() {
        mProxy.agentDeployment();
        dProxy.agentDeployment();
    }

    @Override
    protected void peerConversation() {
        adaPaxosMessageHandler();
        dProxy.diskPaxosMessageHandler();
        mProxy.peerConversation();

        if (slowMode.get())
            fileSynchronize();
    }

    protected void adaPaxosMessageHandler(){
        AdaPaxosMessage msg = aMessages.poll();
        if (msg != null){
            if (msg.fsync && !slowMode.get())
                switchToDisk();
            else if (!msg.fsync && slowMode.get())
                switchToMemory();
        }
    }


    @Override
    protected void clientConversation() {
        ClientRequest[] compact = null;
        try {
            compact = compactChan.poll(clientComWaiting, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (compact != null) {
            if (slowMode.get()) {
                mProxy.getProposer().handleRequests(maxInstance.getAndIncrement(), crtBallot.getAndIncrement(), compact);
            } else {
                dProxy.getProposer().handleRequests(maxInstance.getAndIncrement(), crtBallot.getAndIncrement(), compact);
            }
        }
    }
}
