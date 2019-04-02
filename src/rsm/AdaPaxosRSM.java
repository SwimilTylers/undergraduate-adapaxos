package rsm;

import agent.acceptor.Acceptor;
import agent.acceptor.AdaAcceptor;
import agent.learner.AdaLearner;
import agent.learner.CommitUpdater;
import agent.proposer.AdaProposer;
import client.ClientRequest;
import instance.AdaPaxosInstance;
import instance.InstanceStatus;
import instance.store.InstanceStore;
import instance.store.RemoteInstanceStore;
import javafx.util.Pair;
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
    //protected AtomicInteger maxSendInstance;

    protected AtomicInteger consecutiveCommit;
    protected AtomicInteger fsyncInitInstance;

    transient protected InstanceStore localStore;
    transient protected RemoteInstanceStore remoteStore;
    protected AtomicBoolean forceFsync;
    protected AtomicBoolean metaFsync;
    transient protected boolean[] fsyncSignature;
    transient protected BlockingQueue<Integer> fsyncQueue;

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
        //maxSendInstance = new AtomicInteger(0);

        consecutiveCommit = new AtomicInteger(0);
        fsyncInitInstance = new AtomicInteger(initFsyncInstance);

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
        metaFsync = new AtomicBoolean(true);
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

    @Override
    protected void finalize() throws Throwable {
        routineOnRunning.set(false);
        super.finalize();
    }

    @SuppressWarnings("unchecked")
    public static AdaPaxosRSM makeInstance(final int id, final int epoch, final int peerSize, InstanceStore localStore, RemoteInstanceStore remoteStore, GenericNetService net, boolean initAsLeader){
        AdaPaxosRSM rsm = new AdaPaxosRSM(id, initAsLeader, new NaiveLogger(id));
        rsm.netConnectionBuild(net, net.getConnectionModule(), peerSize)
                .instanceSpaceBuild(AdaPaxosConfiguration.RSM.DEFAULT_INSTANCE_SIZE, epoch << 16, 0)
                .instanceStorageBuild(localStore, remoteStore, false, AdaPaxosConfiguration.RSM.DEFAULT_INSTANCE_SIZE)
                .messageChanBuild(AdaPaxosConfiguration.RSM.DEFAULT_MESSAGE_SIZE, AdaPaxosConfiguration.RSM.DEFAULT_MESSAGE_SIZE, AdaPaxosConfiguration.RSM.DEFAULT_MESSAGE_SIZE, AdaPaxosConfiguration.RSM.DEFAULT_INSTANCE_SIZE);
        return rsm;
    }

    /* public-access deployment func, including:
    * - link: connection establishment of both net and remote-disk */

    public void link(String[] peerAddr, int[] peerPort, final int clientPort) throws InterruptedException {
        link(peerAddr, peerPort, clientPort, AdaPaxosConfiguration.RSM.DEFAULT_LINK_STABLE_WAITING);
    }

    public void link(String[] peerAddr, int[] peerPort, final int clientPort, int stableWaits) throws InterruptedException {
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

        Thread.sleep(stableWaits);

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
        routines.execute(() -> routine_monitor(160, 80, 10));
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
                    int inst_no = maxReceivedInstance.getAndIncrement();
                    proposer.handleRequests(inst_no, crtInstBallot.get(), cmd);
                    logger.logFormatted(true, "init a proposal");
                    if (forceFsync.get())
                        fileSynchronize(inst_no);
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
                        metaFsync.set(true);
                        logger.record(false, "diag","["+System.currentTimeMillis()+"]"+"[FSYNC=true][received]\n");
                        fileSynchronize();
                    } else {
                        forceFsync.set(false);
                        metaFsync.set(true);
                        logger.record(false, "diag","["+System.currentTimeMillis()+"]"+"[FSYNC=false][received]\n");
                        memorySynchronize(message.upto);
                    }
                }
            }
            else {  // if you are a leader, MSync is not necessary.
                int[] crushed = conn.filter(expire);

                if (crushed != null) {
                    logger.record(false, "diag", "["+System.currentTimeMillis()+"]"+Arrays.toString(crushed) + "\n");
                    if (crushed.length >= bare_minority){
                        stableConnCount = 0;
                        boolean oldState = forceFsync.getAndSet(true);
                        if (!oldState) {
                            int ballot = crtInstBallot.incrementAndGet();
                            metaFsync.set(true);
                            logger.record(false, "diag", "[" + System.currentTimeMillis() + "]" + "[FSYNC=true][new ballot=" + ballot + "]\n");
                            net.getPeerMessageSender().broadcastPeerMessage(new AdaPaxosMessage(true, maxReceivedInstance.get()));
                            fileSynchronize();
                        }
                    }
                    else {
                        ++stableConnCount;
                        if (stableConnCount >= stability){
                            boolean oldState = forceFsync.getAndSet(false);
                            if (oldState) {
                                int ballot = crtInstBallot.incrementAndGet();
                                metaFsync.set(true);
                                logger.record(false, "diag", "[" + System.currentTimeMillis() + "]" + "[FSYNC=false][new ballot=" + ballot + "]\n");
                                net.getPeerMessageSender().broadcastPeerMessage(new AdaPaxosMessage(false, maxReceivedInstance.get()));
                            }
                        }
                    }
                }
                else { // no crash
                    ++stableConnCount;
                    if (stableConnCount >= stability){
                        boolean oldState = forceFsync.getAndSet(false);
                        if (oldState) {
                            int ballot = crtInstBallot.incrementAndGet();
                            metaFsync.set(true);
                            logger.record(false, "diag", "[" + System.currentTimeMillis() + "]" + "[FSYNC=false][new ballot=" + ballot + "]\n");
                            net.getPeerMessageSender().broadcastPeerMessage(new AdaPaxosMessage(false, maxReceivedInstance.get()));
                        }
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

                    maxReceivedInstance.updateAndGet(i -> Integer.max(i, msg.inst_no));

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
                        learner.handleAckAccept(cast, i->updateConsecutiveCommit());
                    } else if (msg instanceof GenericPaxosMessage.Commit) {
                        GenericPaxosMessage.Commit cast = (GenericPaxosMessage.Commit) msg;
                        learner.handleCommit(cast, i->updateConsecutiveCommit());
                    } else if (msg instanceof GenericPaxosMessage.Sync) {
                        GenericPaxosMessage.Sync cast = (GenericPaxosMessage.Sync) msg;
                        memorySynchronize_immediate(cast.inst_no, (AdaPaxosInstance) cast.load, i->updateConsecutiveCommit());
                    }

                    if (forceFsync.get())
                        fileSynchronize(msg.inst_no);
                }

                if (asLeader.get()) {
                    DiskPaxosMessage dmsg = dMessages.poll();
                    if (dmsg != null) {
                        logger.log(true, "receive " + dmsg.toString() + "\n");
                        boolean update = false;

                        maxReceivedInstance.updateAndGet(i -> i = Integer.max(i, dmsg.inst_no));

                        if (proposer.isValidMessage(dmsg.inst_no, dmsg.dialog_no)) {
                            if (dmsg instanceof DiskPaxosMessage.ackWrite)
                                update = proposer.respond_ackWrite((DiskPaxosMessage.ackWrite) dmsg);
                            else if (dmsg instanceof DiskPaxosMessage.ackRead)
                                update = proposer.respond_ackRead((DiskPaxosMessage.ackRead) dmsg);
                        } else if (learner.isValidMessage(dmsg.inst_no, dmsg.dialog_no)) {
                            if (dmsg instanceof DiskPaxosMessage.ackWrite)
                                update = learner.respond_ackWrite((DiskPaxosMessage.ackWrite) dmsg, i->updateConsecutiveCommit());
                            else if (dmsg instanceof DiskPaxosMessage.ackRead)
                                update = learner.respond_ackRead((DiskPaxosMessage.ackRead) dmsg, i->updateConsecutiveCommit());
                        }
                        if (forceFsync.get() && update)
                            fileSynchronize(dmsg.inst_no);
                    }
                }
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    protected void routine_backup(final int backupItv){
        int persist = 0;

        while (routineOnRunning.get()) {
            try {
                Integer backup = fsyncQueue.poll(backupItv, TimeUnit.MILLISECONDS);
                if (backup == null) {
                    if (!forceFsync.get()) {
                        persist = Integer.max(fileSynchronize_immediate(), persist);
                        boolean oldState = metaFsync.getAndSet(false);
                        if (oldState)
                            localStore.meta("fast mode, persist=" + persist);
                    }
                } else {
                    int inst_no = backup;
                    AdaPaxosInstance instance = instanceSpace.get(inst_no);
                    if (instance != null) {
                        localStore.store(instance.crtLeaderId, inst_no, instance);
                        logger.logFormatted(false, "fsync", "confirm", "specific=" + inst_no, instance.toString());
                        if (instance.status == InstanceStatus.COMMITTED)
                            fsyncSignature[inst_no] = true;

                        persist = Integer.max(inst_no, persist);

                        if (forceFsync.get()) {
                            boolean oldState = metaFsync.getAndSet(false);
                            if (oldState)
                                localStore.meta("slow mode");
                        }
                    }
                }
            } catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    protected void routine_leadership(final int leadershipItv){
        int crtLeaderId;
        while (routineOnRunning.get()){

        }
    }

    /* protected-access file2mem-mem2file func */

    protected void fileSynchronize(){
        for (int inst_no = fsyncInitInstance.get(); inst_no < maxReceivedInstance.get(); inst_no++) {
            fileSynchronize(inst_no);
        }
    }

    protected int fileSynchronize_immediate(){
        int inst_no = fsyncInitInstance.get();
        int fsyncInit_sta = inst_no;
        for (; inst_no < consecutiveCommit.get(); inst_no++) {
            if (!fsyncSignature[inst_no]){
                AdaPaxosInstance instance = instanceSpace.get(inst_no);
                if (instance != null) {
                    localStore.store(instance.crtLeaderId, inst_no, instance);
                    fsyncSignature[inst_no] = true;
                }
                else
                    break;
            }
        }
        fsyncInitInstance.set(inst_no);
        int fsyncInit_end = inst_no;
        for (; inst_no < maxReceivedInstance.get(); inst_no++) {
            if (!fsyncSignature[inst_no]){
                AdaPaxosInstance instance = instanceSpace.get(inst_no);
                if (instance != null) {
                    localStore.store(instance.crtLeaderId, inst_no, instance);
                    if (instance.status == InstanceStatus.COMMITTED)
                        fsyncSignature[inst_no] = true;
                }
            }
        }
        logger.logFormatted(false, "fsync", "backup", "fsyncInit="+fsyncInit_sta+"->"+fsyncInit_end, "upto="+inst_no);
        return inst_no;
    }

    protected void memorySynchronize(final int upto){
        int upper = maxReceivedInstance.updateAndGet(i-> Integer.max(i, upto));
        for (int inst_no = consecutiveCommit.get(); inst_no < upper; inst_no++) {
            try {
                AdaPaxosInstance instance = instanceSpace.get(inst_no);
                if (instance == null || instance.status != InstanceStatus.COMMITTED) {
                    instance = null;
                    for (int server = 0; server < peerSize; server++) {
                        if (localStore.isExist(server, inst_no)) {
                            AdaPaxosInstance compare = (AdaPaxosInstance) localStore.fetch(server, inst_no);
                            if (instance == null
                                    || instance.crtInstBallot < compare.crtInstBallot
                                    || (instance.crtInstBallot == compare.crtInstBallot
                                    && !InstanceStatus.earlierThan(instance.status, compare.status))) {
                                instance = compare;
                                logger.logFormatted(false, "msync", "competitor", "inst_no=" + inst_no, "source=" + server, "inst=" + compare);
                            }
                        }
                    }
                    logger.logFormatted(false, "msync", "candidate", "inst_no=" + inst_no, "inst=" + (instance == null ? "null" : instance.toString()));
                }
                pMessages.put(new GenericPaxosMessage.Sync(inst_no, instance));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    protected void memorySynchronize_immediate(final int inst_no, AdaPaxosInstance candidate, CommitUpdater updater){
        AtomicBoolean commitUpdate = new AtomicBoolean(false);
        if (candidate != null){
            instanceSpace.updateAndGet(inst_no, instance -> {
                if (instance == null
                        || instance.crtInstBallot < candidate.crtInstBallot
                        || (instance.crtInstBallot == candidate.crtInstBallot
                            && !InstanceStatus.earlierThan(instance.status, candidate.status))) {

                    if ((instance == null || instance.status != InstanceStatus.COMMITTED)
                            && candidate.status == InstanceStatus.COMMITTED) {
                        commitUpdate.set(true);
                    }

                    return candidate;
                }
                else
                    return instance;
            });

            if (commitUpdate.get()){
                logger.logCommit(inst_no, new GenericPaxosMessage.Commit(inst_no, candidate.crtLeaderId, candidate.crtInstBallot, candidate.requests), "settled");
                updater.update(inst_no);
            }
        }
    }

    protected void fileSynchronize(final int specific){
        if (!fsyncSignature[specific]){
            try {
                fsyncQueue.put(specific);
                logger.logFormatted(false, "fsync", "submit", "specific=" + specific);
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
        while (iter < maxReceivedInstance.get()){
            AdaPaxosInstance inst = instanceSpace.get(iter);
            if (inst == null || inst.status != InstanceStatus.COMMITTED)
                break;
            ++iter;
        }
        consecutiveCommit.set(iter);
        logger.logFormatted(false, "consecutive-commit", "upto="+iter);
    }
}
