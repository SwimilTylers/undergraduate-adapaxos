package rsm;

import agent.acceptor.Acceptor;
import agent.acceptor.AdaAcceptor;
import agent.learner.AdaLearner;
import agent.learner.Learner;
import agent.proposer.AdaProposer;
import agent.proposer.Proposer;
import client.ClientRequest;
import instance.AdaPaxosInstance;
import instance.InstanceStatus;
import instance.store.InstanceStore;
import instance.store.OffsetIndexStore;
import instance.store.RemoteInstanceStore;
import javafx.util.Pair;
import logger.DummyLogger;
import logger.NaiveLogger;
import logger.PaxosLogger;
import network.message.protocols.AdaPaxosMessage;
import network.message.protocols.DiskPaxosMessage;
import network.message.protocols.Distinguishable;
import network.message.protocols.GenericPaxosMessage;
import network.service.GenericNetService;
import network.service.module.ConnectionModule;
import utils.AdaPaxosConfiguration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * @author : Swimiltylers
 * @version : 2019/3/14 18:19
 */
public class AdaPaxosRSM implements Serializable{
    private static final long serialVersionUID = -1904538218951667113L;

    /* unique identity */
    protected int serverId;
    transient protected AtomicBoolean asLeader;


    /* net and connect */
    protected int peerSize;
    transient protected GenericNetService net;
    transient protected ConnectionModule conn;

    /* channels */
    transient protected Queue<ClientRequest> restoredQueue;

    transient protected BlockingQueue<ClientRequest> cMessages;
    transient protected BlockingQueue<GenericPaxosMessage> pMessages;
    transient protected BlockingQueue<DiskPaxosMessage> dMessages;
    transient protected BlockingQueue<AdaPaxosMessage> aMessages;
    transient protected List<Pair<Distinguishable, BlockingQueue>> customizedChannels;


    /* instance and storage */
    transient protected AtomicReferenceArray<AdaPaxosInstance> instanceSpace;
    protected AtomicInteger crtInstBallot;
    protected AtomicInteger maxReceivedInstance;
    protected AtomicInteger maxSendInstance;

    protected AtomicInteger consecutiveCommit;
    protected int fsyncInitInstance;

    transient protected InstanceStore localStore;
    transient protected RemoteInstanceStore remoteStore;
    protected AtomicBoolean forceFsync;
    transient protected boolean[] fsyncSignature;
    transient protected BlockingQueue<Pair<Integer, AdaPaxosInstance>> fsyncQueue;

    /* agents */
    transient protected AdaProposer proposer;
    transient protected AdaLearner learner;
    transient protected Acceptor acceptor;

    /* misc */
    transient protected PaxosLogger logger;
    transient protected AtomicBoolean routineOnRunning;


    protected AdaPaxosRSM(final int serverId,
                       final boolean initAsLeader,
                       PaxosLogger logger) {

        this.serverId = serverId;
        this.asLeader = new AtomicBoolean(initAsLeader);
        this.logger = logger;

        this.restoredQueue = new ConcurrentLinkedQueue<>();
    }

    /* protected-access build func */

    protected AdaPaxosRSM netConnectionBuild(GenericNetService net, ConnectionModule conn, int peerSize){
        this.peerSize = peerSize;
        this.net = net;
        this.conn = conn;

        net.setLogger(logger);

        return this;
    }

    protected AdaPaxosRSM instanceSpaceBuild(final int sizeInstanceSpace,
                                      final int initInstBallot,
                                      final int initFsyncInstance){

        instanceSpace = new AtomicReferenceArray<>(sizeInstanceSpace);
        crtInstBallot = new AtomicInteger(initInstBallot);
        maxReceivedInstance = new AtomicInteger(0);
        maxSendInstance = new AtomicInteger(0);

        consecutiveCommit = new AtomicInteger(0);
        fsyncInitInstance = initFsyncInstance;

        return this;
    }

    protected AdaPaxosRSM instanceStorageBuild(InstanceStore localStore,
                                        RemoteInstanceStore remoteStore,
                                        boolean initFsync, int waitingQueueLength){

        this.localStore = localStore;
        this.remoteStore = remoteStore;
        remoteStore.setLogger(logger);
        // TODO: 2019/3/29 setLogger
        forceFsync = new AtomicBoolean(initFsync);
        fsyncSignature = new boolean[instanceSpace.length()];
        Arrays.fill(fsyncSignature, false);
        fsyncQueue = new ArrayBlockingQueue<>(waitingQueueLength);

        return this;
    }

    protected AdaPaxosRSM messageChanBuild(final int sizeCMessageChan,
                                 final int sizePMessage,
                                 final int sizeDMessage,
                                 final int sizeAMessage,
                                 Pair<Distinguishable, BlockingQueue>... supplement){

        cMessages = new ArrayBlockingQueue<>(sizeCMessageChan);
        pMessages = new ArrayBlockingQueue<>(sizePMessage);
        dMessages = new ArrayBlockingQueue<>(sizeDMessage);
        aMessages = new ArrayBlockingQueue<>(sizeAMessage);

        customizedChannels = new ArrayList<>();

        if (supplement != null && supplement.length != 0) {
            customizedChannels = new ArrayList<>();
            customizedChannels.addAll(Arrays.asList(supplement));
        }

        return this;
    }

    @SuppressWarnings("unchecked")
    public static AdaPaxosRSM makeInstance(final int id, final int epoch, final int peerSize, InstanceStore localStore, RemoteInstanceStore remoteStore, GenericNetService net, boolean initAsLeader){
        AdaPaxosRSM rsm = new AdaPaxosRSM(id, initAsLeader, new NaiveLogger(id));
        rsm.netConnectionBuild(net, net.getConnectionModule(), peerSize)
                .instanceSpaceBuild(AdaPaxosConfiguration.RSM.DEFAULT_INSTANCE_SIZE, epoch << 16, 0)
                .instanceStorageBuild(localStore, remoteStore, true, AdaPaxosConfiguration.RSM.DEFAULT_INSTANCE_SIZE)
                .messageChanBuild(AdaPaxosConfiguration.RSM.DEFAULT_MESSAGE_SIZE, AdaPaxosConfiguration.RSM.DEFAULT_MESSAGE_SIZE, AdaPaxosConfiguration.RSM.DEFAULT_MESSAGE_SIZE, AdaPaxosConfiguration.RSM.DEFAULT_INSTANCE_SIZE);
        return rsm;
    }

    /* public-access deployment func, including:
    * - link: connection establishment of both net and remote-disk */

    public void link(String[] peerAddr, int[] peerPort, final int clientPort) throws InterruptedException {
        link(peerAddr, peerPort, clientPort, AdaPaxosConfiguration.RSM.DEFAULT_LINK_STABLE_WAITING);
    }

    public void link(String[] peerAddr, int[] peerPort, final int clientPort, int wait) throws InterruptedException {
        assert peerAddr.length == peerPort.length && peerAddr.length == peerSize;

        net.setClientChan(cMessages);
        net.setPaxosChan(pMessages);
        net.registerChannel(o->o instanceof DiskPaxosMessage, dMessages);
        net.registerChannel(o->o instanceof AdaPaxosMessage, aMessages);
        for (Pair<Distinguishable, BlockingQueue> chan : customizedChannels) {
            net.registerChannel(chan.getKey(), chan.getValue());
        }

        net.connect(peerAddr, peerPort);
        conn = net.getConnectionModule();
        net.openClientListener(clientPort);

        remoteStore.connect(dMessages);

        Thread.sleep(wait);

        logger.record(false, "hb", "finish link\n");
    }

    public void agent(){
        proposer = new AdaProposer(serverId, peerSize, forceFsync, net.getPeerMessageSender(), remoteStore, instanceSpace, restoredQueue, logger);
        acceptor = new AdaAcceptor(serverId, peerSize, forceFsync, net.getPeerMessageSender(), remoteStore, instanceSpace, restoredQueue, logger);
        learner = new AdaLearner(serverId, peerSize, forceFsync, net.getPeerMessageSender(), remoteStore, instanceSpace, restoredQueue, logger);
    }

    public void routine(Runnable... supplement){
        routineOnRunning = new AtomicBoolean(true);
        ExecutorService routines = Executors.newCachedThreadPool();
        routines.execute(()-> routine_batch(2000, GenericPaxosSMR.DEFAULT_REQUEST_COMPACTING_SIZE));
        routines.execute(() -> routine_monitor(200, 100, 10));
        routines.execute(this::routine_response);
        routines.execute(() -> routine_backup(5000));
        if (supplement != null && supplement.length != 0) {
            for (Runnable r : supplement)
                routines.execute(r);
        }
        routines.shutdown();
    }

    /* protected-access routine func, including:
    * - batch: batch up requests from both clients or restoredQueue and initiate proposal on batched requests
    * - monitor: check out network status and shift between slow- and fast-mode accordingly
    * - response: response peers due to paxos mechanics
    * - backup: backup on-memory instance if necessary
    * - leadership: working on leader-election */

    protected void routine_batch(final int batchItv, final int batchSize){
        while (routineOnRunning.get()){
            int cMessageSize = cMessages.size();
            List<ClientRequest> requestList = new ArrayList<>();

            cMessageSize = Integer.min(cMessageSize, batchSize);

            for (int i = 0; i < cMessageSize; i++) {
                try {
                    requestList.add(cMessages.take());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    return;
                }
            }

            int count = cMessageSize;
            ClientRequest recv = restoredQueue.poll();
            while(count < batchSize && recv != null){
                requestList.add(recv);
                ++count;
                recv = restoredQueue.poll();
            }

            if (!requestList.isEmpty()) {
                ClientRequest[] cmd = requestList.toArray(new ClientRequest[0]);
                if (asLeader.get()){
                    proposer.handleRequests(maxReceivedInstance.getAndIncrement(), crtInstBallot.get(), cmd);
                    logger.logFormatted(true, "init a proposal");
                }
            }

            try {
                Thread.sleep(batchItv); // drop the refreshing frequency of 'batch'
            } catch (InterruptedException e) {
                e.printStackTrace();
                return;
            }
        }
    }

    protected void routine_monitor(final int monitorItv, final int expire, final int stability){
        final int bare_majority = (peerSize+1)/2;
        final int bare_minority = peerSize - bare_majority;

        int stableConnCount = 0;

        while (routineOnRunning.get()){
            if (!asLeader.get()) {
                AdaPaxosMessage message = aMessages.poll();
                if (message != null) {
                    if (message.fsync) {
                        forceFsync.set(true);
                        fileSynchronize();
                        logger.record(false, "diag","["+System.currentTimeMillis()+"]"+"[FSYNC=true][received]\n");
                    } else {
                        forceFsync.set(false);
                        memorySynchronize();
                        logger.record(false, "diag","["+System.currentTimeMillis()+"]"+"[FSYNC=false][received]\n");
                    }
                }
            }
            else {
                int[] crushed = conn.filter(expire);

                if (crushed != null) {
                    logger.record(false, "diag", "["+System.currentTimeMillis()+"]"+Arrays.toString(crushed) + "\n");
                    if (crushed.length >= bare_minority && !forceFsync.get()){
                        stableConnCount = 0;
                        int ballot = crtInstBallot.incrementAndGet();
                        forceFsync.set(true);
                        //fileSynchronize();
                        logger.record(false, "diag","["+System.currentTimeMillis()+"]"+"[FSYNC=true][new ballot="+ballot+"]\n");
                        net.getPeerMessageSender().broadcastPeerMessage(new AdaPaxosMessage(true));
                    }
                    else if (crushed.length < bare_minority && forceFsync.get()){
                        ++stableConnCount;
                        if (stableConnCount >= stability){
                            int ballot = crtInstBallot.incrementAndGet();
                            forceFsync.set(false);
                            //memorySynchronize();
                            logger.record(false, "diag","["+System.currentTimeMillis()+"]"+"[FSYNC=false][new ballot="+ballot+"]\n");
                            net.getPeerMessageSender().broadcastPeerMessage(new AdaPaxosMessage(false));
                        }
                    }
                }
                else if (forceFsync.get()){ // no crash
                    ++stableConnCount;
                    if (stableConnCount >= stability){
                        int ballot = crtInstBallot.incrementAndGet();
                        forceFsync.set(false);
                        //memorySynchronize();
                        logger.record(false, "diag","["+System.currentTimeMillis()+"]"+"[FSYNC=false][new ballot="+ballot+"]\n");
                        net.getPeerMessageSender().broadcastPeerMessage(new AdaPaxosMessage(false));
                    }
                }

                try {
                    Thread.sleep(monitorItv); // drop the refreshing frequency of 'monitor'
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    return;
                }
            }
        }
    }

    protected void routine_response(){
        try {
            while (routineOnRunning.get()) {
                GenericPaxosMessage msg = pMessages.poll();
                if (msg != null) {
                    logger.log(true, "receive " + msg.toString() + "\n");

                    maxReceivedInstance.updateAndGet(i -> i = Integer.max(i, msg.inst_no));

                    if (msg instanceof GenericPaxosMessage.Prepare) {
                        GenericPaxosMessage.Prepare cast = (GenericPaxosMessage.Prepare) msg;
                        acceptor.handlePrepare(cast);
                    } else if (msg instanceof GenericPaxosMessage.ackPrepare) {
                        GenericPaxosMessage.ackPrepare cast = (GenericPaxosMessage.ackPrepare) msg;
                        proposer.handleAckPrepare(cast);
                    } else if (msg instanceof GenericPaxosMessage.Accept) {
                        GenericPaxosMessage.Accept cast = (GenericPaxosMessage.Accept) msg;
                        acceptor.handleAccept(cast);
                    } else if (msg instanceof GenericPaxosMessage.ackAccept) {
                        GenericPaxosMessage.ackAccept cast = (GenericPaxosMessage.ackAccept) msg;
                        learner.handleAckAccept(cast);
                    } else if (msg instanceof GenericPaxosMessage.Commit) {
                        GenericPaxosMessage.Commit cast = (GenericPaxosMessage.Commit) msg;
                        learner.handleCommit(cast);
                        updateConsecutiveCommit();
                    } else if (msg instanceof GenericPaxosMessage.Restore) {
                        GenericPaxosMessage.Restore cast = (GenericPaxosMessage.Restore) msg;
                        //handleRestore(cast);
                    }

                    if (forceFsync.get())
                        fileSynchronize(msg.inst_no);
                }

                if (asLeader.get()) {
                    DiskPaxosMessage dmsg = dMessages.poll();
                    if (dmsg != null) {
                        logger.log(true, "receive " + dmsg.toString() + "\n");

                        maxReceivedInstance.updateAndGet(i -> i = Integer.max(i, dmsg.inst_no));

                        if (proposer.isValidMessage(dmsg.inst_no, dmsg.dialog_no)) {
                            if (dmsg instanceof DiskPaxosMessage.ackWrite)
                                proposer.respond_ackWrite((DiskPaxosMessage.ackWrite) dmsg);
                            else if (dmsg instanceof DiskPaxosMessage.ackRead)
                                proposer.respond_ackRead((DiskPaxosMessage.ackRead) dmsg);
                        } else if (learner.isValidMessage(dmsg.inst_no, dmsg.dialog_no)) {
                            if (dmsg instanceof DiskPaxosMessage.ackWrite)
                                learner.respond_ackWrite((DiskPaxosMessage.ackWrite) dmsg);
                            else if (dmsg instanceof DiskPaxosMessage.ackRead)
                                learner.respond_ackRead((DiskPaxosMessage.ackRead) dmsg);
                        }

                        if (forceFsync.get())
                            fileSynchronize(dmsg.inst_no);
                    }
                }
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    protected void routine_backup(final int backupItv){
        while (routineOnRunning.get()) {
            try {
                Thread.sleep(backupItv);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        /*
        boolean fastPersisted = false;
        int persistUpperBound = fsyncInitInstance;
        while(routineOnRunning.get()){
            Pair<Integer, AdaPaxosInstance> backup = fsyncQueue.poll();
            if (backup == null){
                try {
                    int len = fileSynchronize_immediate();
                    if (!fastPersisted && !forceFsync.get()){
                        localStore.meta("fsync=false, len="+len);
                        fastPersisted = true;
                    }
                    Thread.sleep(backupItv);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            else {
                int inst_no = backup.getKey();
                AdaPaxosInstance instance = backup.getValue();
                localStore.store(instance.crtLeaderId, inst_no, instance);
                if (instance.status == InstanceStatus.COMMITTED)
                    fsyncSignature[inst_no] = true;
                if (inst_no > persistUpperBound){
                    persistUpperBound = inst_no;
                    fastPersisted = false;
                    localStore.meta("fsync=true, len="+persistUpperBound);
                }
            }
        }
        */
    }

    protected void routine_leadership(final int leadershipItv){

    }

    /* protected-access file2mem-mem2file func */

    protected void fileSynchronize(){
        for (int inst_no = fsyncInitInstance; inst_no < Integer.max(maxSendInstance.get(), maxReceivedInstance.get()); inst_no++) {
            fileSynchronize(inst_no);
        }
    }

    protected int fileSynchronize_immediate(){
        int inst_no = fsyncInitInstance;
        for (; inst_no < consecutiveCommit.get(); inst_no++) {
            if (!fsyncSignature[inst_no]){
                AdaPaxosInstance instance = instanceSpace.get(inst_no);
                if (instance != null) {
                    localStore.store(instance.crtLeaderId, inst_no, instance);
                    fsyncSignature[inst_no] = true;
                }
            }
        }
        fsyncInitInstance = inst_no;
        for (; inst_no < Integer.max(maxSendInstance.get(), maxReceivedInstance.get()); inst_no++) {
            if (!fsyncSignature[inst_no]){
                AdaPaxosInstance instance = instanceSpace.get(inst_no);
                if (instance != null) {
                    localStore.store(instance.crtLeaderId, inst_no, instance);
                    if (instance.status == InstanceStatus.COMMITTED)
                        fsyncSignature[inst_no] = true;
                }
            }
        }
        return inst_no;
    }

    protected void memorySynchronize(){

    }

    protected void fileSynchronize(final int specific){
        if (!fsyncSignature[specific]){
            try {
                AdaPaxosInstance instance = instanceSpace.get(specific);
                if (instance != null)
                    fsyncQueue.put(new Pair<>(specific, instance));
                logger.log(true, "serverId="+serverId+", specific="+specific+", fqsize="+fsyncQueue.size()+"\n");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /* protected-access misc func */

    /*
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

    */

    protected void updateConsecutiveCommit(){
        int iter = consecutiveCommit.get();
        while (iter < Integer.max(maxReceivedInstance.get(), maxSendInstance.get())){
            AdaPaxosInstance inst = instanceSpace.get(iter);
            if (inst == null || inst.status != InstanceStatus.COMMITTED)
                break;
            ++iter;
        }
        consecutiveCommit.set(iter);
    }
}
