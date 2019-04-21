package rsm;

import agent.acceptor.Acceptor;
import agent.acceptor.AdaAcceptor;
import agent.learner.AdaLearner;
import agent.proposer.AdaProposer;
import agent.recovery.AdaRecovery;
import agent.recovery.LeaderElectionPerformer;
import client.ClientRequest;
import instance.AdaPaxosInstance;
import instance.InstanceStatus;
import instance.maintenance.AdaRecoveryMaintenance;
import instance.store.PseudoRemoteInstanceStore;
import instance.store.RemoteInstanceStore;
import javafx.util.Pair;
import logger.NaiveLogger;
import logger.PaxosLogger;
import network.message.protocols.*;
import network.service.GenericNetService;
import network.service.module.connection.ConnectionModule;
import network.service.module.controller.BipolarStateDecider;
import network.service.module.controller.BipolarStateReminder;
import utils.AdaAgents;
import utils.AdaPaxosParameters;
import utils.NetworkConfiguration;

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
public class AdaPaxosRSM implements Serializable {
    private static final long serialVersionUID = -1904538218951667113L;

    /* unique identity */
    protected int serverId;
    protected NetworkConfiguration nConfig;
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
    transient protected BlockingQueue<LeaderElectionMessage> lMessages;
    transient protected List<Pair<Distinguishable, BlockingQueue>> customizedChannels;


    /* instance and storage */
    transient protected AtomicReferenceArray<AdaPaxosInstance> instanceSpace;
    protected AtomicInteger crtInstBallot;
    protected AtomicInteger maxReceivedInstance;
    //protected AtomicInteger maxSendInstance;

    protected AtomicInteger consecutiveCommit;
    protected AtomicInteger fsyncInitInstance;

    transient protected RemoteInstanceStore remoteStore;
    transient protected int diskSize;
    protected AtomicBoolean forceFsync;
    protected AtomicBoolean metaFsync;
    transient protected boolean[] fsyncSignature;
    transient protected BlockingQueue<Integer> fsyncQueue;
    transient protected AtomicReferenceArray<AdaRecoveryMaintenance> recoveryList;

    /* agents */
    transient protected AdaProposer proposer;
    transient protected AdaLearner learner;
    transient protected Acceptor acceptor;
    transient protected AdaRecovery recovery;

    /* misc */
    transient protected PaxosLogger logger;
    transient protected AtomicBoolean routineOnRunning;
    transient protected Runnable[] supplementRoutines;


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
        maxReceivedInstance = new AtomicInteger(initFsyncInstance);
        //maxSendInstance = new AtomicInteger(0);

        consecutiveCommit = new AtomicInteger(0);
        fsyncInitInstance = new AtomicInteger(initFsyncInstance);

        return this;
    }

    protected AdaPaxosRSM instanceStorageBuild(RemoteInstanceStore remoteStore,
                                        boolean initFsync, int waitingQueueLength){

        this.remoteStore = remoteStore;
        this.diskSize = remoteStore.getDiskSize();
        ((PseudoRemoteInstanceStore)remoteStore).setLogger(logger);
        forceFsync = new AtomicBoolean(initFsync);
        metaFsync = new AtomicBoolean(true);
        fsyncSignature = new boolean[instanceSpace.length()];
        Arrays.fill(fsyncSignature, false);
        fsyncQueue = new ArrayBlockingQueue<>(waitingQueueLength);
        recoveryList = new AtomicReferenceArray<>(instanceSpace.length());

        return this;
    }

    @SuppressWarnings({"unchecked", "varargs"})
    protected AdaPaxosRSM messageChanBuild(final int sizeCMessageChan,
                                 final int sizePMessage,
                                 final int sizeDMessage,
                                 final int sizeAMessage,
                                 final int sizeLMessage,
                                 Pair<Distinguishable, BlockingQueue>... supplement){

        cMessages = new ArrayBlockingQueue<>(sizeCMessageChan);
        pMessages = new ArrayBlockingQueue<>(sizePMessage);
        dMessages = new ArrayBlockingQueue<>(sizeDMessage);
        aMessages = new ArrayBlockingQueue<>(sizeAMessage);
        lMessages = new ArrayBlockingQueue<>(sizeLMessage);

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
    public static AdaPaxosRSM makeInstance(final int id, final int epoch, final int peerSize, RemoteInstanceStore remoteStore, GenericNetService net){
        AdaPaxosRSM rsm = new AdaPaxosRSM(id, false, new NaiveLogger(id));
        rsm.netConnectionBuild(net, net.getConnectionModule(), peerSize)
                .instanceSpaceBuild(AdaPaxosParameters.RSM.DEFAULT_INSTANCE_SIZE, epoch << 16, 0)
                .instanceStorageBuild(remoteStore, true, AdaPaxosParameters.RSM.DEFAULT_INSTANCE_SIZE)
                .messageChanBuild(AdaPaxosParameters.RSM.DEFAULT_MESSAGE_SIZE, AdaPaxosParameters.RSM.DEFAULT_MESSAGE_SIZE, AdaPaxosParameters.RSM.DEFAULT_MESSAGE_SIZE, AdaPaxosParameters.RSM.DEFAULT_INSTANCE_SIZE, AdaPaxosParameters.RSM.DEFAULT_INSTANCE_SIZE);
        return rsm;
    }

    /* public-access deployment func, including:
    * - link: connection establishment of both net and remote-disk */

    public void link(NetworkConfiguration netConfig, final int clientPort) throws InterruptedException {
        link(netConfig, clientPort, AdaPaxosParameters.RSM.DEFAULT_LINK_STABLE_WAITING);
    }

    public void link(NetworkConfiguration netConfig, final int clientPort, int stableWaits) throws InterruptedException {
        String[] peerAddr = netConfig.peerAddr;
        int[] peerPort = netConfig.peerPort;
        this.nConfig = netConfig;
        asLeader.set(netConfig.initLeaderId == serverId);

        assert peerAddr.length == peerPort.length && peerAddr.length == peerSize;

        net.setClientChan(cMessages);
        net.setPaxosChan(pMessages);
        net.registerChannel(o->o instanceof DiskPaxosMessage, dMessages);
        net.registerChannel(o->o instanceof AdaPaxosMessage, aMessages);
        net.registerChannel(o->o instanceof LeaderElectionMessage, lMessages);
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
        recovery = new AdaRecovery(serverId, peerSize, nConfig.initLeaderId, net.getPeerMessageSender(), net.getConnectionModule(), maxReceivedInstance, remoteStore, instanceSpace, recoveryList, logger);
    }

    public void routine(Runnable... supplement){
        routineOnRunning = new AtomicBoolean(true);
        ExecutorService routines = Executors.newCachedThreadPool();
        routines.execute(()-> routine_batch(1000, GenericPaxosSMR.DEFAULT_REQUEST_COMPACTING_SIZE));
        routines.execute(() -> routine_monitor(20, 40, 3, 10, 5000));
        routines.execute(this::routine_response);
        routines.execute(() -> routine_backup(5000));
        routines.execute(() -> routine_leadership(5000));

        if (supplement != null && supplement.length != 0) {
            supplementRoutines = supplement;
            for (Runnable r : supplement)
                routines.execute(r);
        }
        routines.shutdown();
    }

    public void routine(BipolarStateReminder reminder, BipolarStateDecider decider, Runnable... supplement){
        routine(supplement);
        Thread t = new Thread(() -> thread_bipolar(reminder, decider));
        //t.setPriority(Thread.MAX_PRIORITY);
        t.start();
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

    protected void routine_monitor(final int monitorItv, final int expire, final int stability, final int decisionDelay, final int LEDeadline){
        final int bare_majority = (peerSize+1)/2;
        final int bare_minority = peerSize - bare_majority;

        int stableConnCount = 0;

        while (routineOnRunning.get()){
            if (!asLeader.get()) {
                try {
                    AdaPaxosMessage message = aMessages.poll(monitorItv, TimeUnit.MILLISECONDS);
                    if (message != null) {
                        if (message.fsync) {
                            forceFsync.set(true);
                            metaFsync.set(true);
                            logger.record(false, "diag", "[" + System.currentTimeMillis() + "]" + "[FSYNC=true][received]\n");
                            fileSynchronize();
                        } else {
                            forceFsync.set(false);
                            metaFsync.set(true);
                            logger.record(false, "diag", "[" + System.currentTimeMillis() + "]" + "[FSYNC=false][received]\n");
                            memorySynchronize(AdaAgents.newToken());
                        }
                    }
                    else if (!asLeader.get() && !recovery.onLeaderElection()){      // carry out leader detection
                        if (recovery.isLeaderSurvive(expire)){

                            /* at the first sight out timeout, follower should flush all on-memory instances to disk */
                            logger.record(false, "diag", "[" + System.currentTimeMillis() + "][leader failure][test=1]\n");
                            //fileSynchronize();

                            Thread.sleep(decisionDelay);
                            if (!routineOnRunning.get())
                                break;

                            if (recovery.isLeaderSurvive(expire)){  // leader crash confirmed, running into FAST_MODE
                                if (!forceFsync.getAndSet(false)){   // FAST_MODE before leader crashed
                                    metaFsync.set(true);
                                    long leToken = AdaAgents.newToken();
                                    recovery.stateSet(LeaderElectionPerformer.LeaderElectionState.RECOVERED);   // join LeaderElection
                                    logger.record(true, "diag", "[" + System.currentTimeMillis() + "][leader failure][test=2][confirmed][RECOVERED, token="+leToken+"]\n");
                                    LeaderElectionMessage.LeStart startSignal = new LeaderElectionMessage.LeStart(serverId, leToken, maxReceivedInstance.get());
                                    logger.logFormatted(false, "start leader election", startSignal.toString());
                                    lMessages.put(startSignal); // init LeaderElection
                                }
                                else {  // SLOW_MODE before leader crashed
                                    long leToken = AdaAgents.newToken();
                                    recovery.stateSet(LeaderElectionPerformer.LeaderElectionState.RECOVERING);  // wait for update
                                    logger.record(true, "diag", "[" + System.currentTimeMillis() + "][leader failure][test=2][confirmed][RECOVERING, token="+leToken+"]\n");
                                    memorySynchronize(leToken); // fetch up-to-date information from disk
                                }
                            }
                        }
                    }
                } catch (Exception e){
                    e.printStackTrace();
                }
            }
            else {  // if you are a leader, MSync is not necessary.
                int[] crushed = conn.filter(expire);

                if (crushed != null) {
                    logger.record(false, "diag", "["+System.currentTimeMillis()+"][crashed="+Arrays.toString(crushed) + "]\n");
                    if (crushed.length >= bare_minority){
                        try {
                            Thread.sleep(decisionDelay);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        if (!routineOnRunning.get())
                            break;

                        crushed = conn.filter(expire);
                        if (crushed != null && crushed.length >= bare_minority) {
                            boolean oldState = forceFsync.getAndSet(true);
                            if (!oldState) {
                                stableConnCount = 0;
                                int ballot = crtInstBallot.incrementAndGet();
                                metaFsync.set(true);
                                logger.record(false, "diag", "[" + System.currentTimeMillis() + "]" + "[FSYNC=true][new ballot=" + ballot + "]\n");
                                net.getPeerMessageSender().broadcastPeerMessage(new AdaPaxosMessage(true, maxReceivedInstance.get()));
                                fileSynchronize();
                            }
                        }
                        else {
                            logger.record(false, "diag", "[" + System.currentTimeMillis() + "][cancel]\n");
                            forceFsync.set(false);
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
                    logger.logFormatted(true, "receive", msg.toString());

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
                    } else if (msg instanceof GenericPaxosMessage.Sync){
                        GenericPaxosMessage.Sync cast = (GenericPaxosMessage.Sync) msg;
                        recovery.handleSync(cast);
                    } else if (msg instanceof GenericPaxosMessage.ackSync){
                        GenericPaxosMessage.ackSync cast = (GenericPaxosMessage.ackSync) msg;
                        recovery.handleAckSync(cast, i->updateConsecutiveCommit(), this::finishDisk2Mem);
                    }

                    if (forceFsync.get())
                        fileSynchronize(msg.inst_no);
                }

                DiskPaxosMessage dmsg = dMessages.poll();
                if (dmsg != null) {
                    logger.logFormatted(true, "receive", dmsg.toString());
                    boolean update = false;

                    maxReceivedInstance.updateAndGet(i -> Integer.max(i, dmsg.inst_no));

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
                    } else if (recovery.isValidMessage(dmsg.inst_no, dmsg.dialog_no)){
                        if (dmsg instanceof DiskPaxosMessage.ackRead)
                            update = recovery.respond_ackRead((DiskPaxosMessage.ackRead) dmsg, i->updateConsecutiveCommit(), this::finishDisk2Mem);
                    }
                    if (forceFsync.get() && update)
                        fileSynchronize(dmsg.inst_no);
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
                        if (oldState) {
                            // TODO: 2019/4/2 meta-data
                            //localStore.meta("fast mode, persist=" + persist);
                        }
                    }
                } else {
                    int inst_no = backup;
                    AdaPaxosInstance instance = instanceSpace.get(inst_no);
                    if (instance != null) {
                        remoteStore.launchRemoteStore(AdaAgents.newToken(), serverId, instance.crtLeaderId, inst_no, instance);
                        //localStore.store(instance.crtLeaderId, inst_no, instance);
                        logger.logFormatted(false, "fsync", "confirm", "specific=" + inst_no, instance.toString());
                        if (instance.status == InstanceStatus.COMMITTED)
                            fsyncSignature[inst_no] = true;

                        persist = Integer.max(inst_no, persist);

                        if (forceFsync.get()) {
                            boolean oldState = metaFsync.getAndSet(false);
                            if (oldState) {
                                // TODO: 2019/4/2 meta-data
                                //localStore.meta("slow mode");
                            }
                        }
                    }
                }
            } catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    protected void routine_leadership(final int leadershipItv){
        while (routineOnRunning.get()){
            try {
                LeaderElectionMessage msg = lMessages.poll(leadershipItv, TimeUnit.MILLISECONDS);
                if (msg != null){
                    if (msg instanceof LeaderElectionMessage.LeStart){
                        LeaderElectionMessage.LeStart cast = (LeaderElectionMessage.LeStart) msg;
                        recovery.handleLEStart(cast);
                    }
                    if (msg instanceof LeaderElectionMessage.Propaganda){
                        LeaderElectionMessage.Propaganda cast = (LeaderElectionMessage.Propaganda) msg;
                        recovery.handleLEPropaganda(cast);
                    }
                    else if (msg instanceof LeaderElectionMessage.Vote){
                        LeaderElectionMessage.Vote cast = (LeaderElectionMessage.Vote) msg;
                        recovery.handleLEVote(cast, (tk, id) -> asLeader.set(id == serverId));
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /* protected-access sync & recovery func */

    protected void fileSynchronize(){
        for (int inst_no = fsyncInitInstance.get(); inst_no < maxReceivedInstance.get(); inst_no++) {
            fileSynchronize(inst_no);
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

    protected int fileSynchronize_immediate(){
        int inst_no = fsyncInitInstance.get();
        int fsyncInit_sta = inst_no;
        for (; inst_no < consecutiveCommit.get(); inst_no++) {
            if (!fsyncSignature[inst_no]){
                AdaPaxosInstance instance = instanceSpace.get(inst_no);
                if (instance != null) {
                    remoteStore.launchRemoteStore(AdaAgents.newToken(), serverId, instance.crtLeaderId, inst_no, instance);
                    //localStore.store(instance.crtLeaderId, inst_no, instance);
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
                    remoteStore.launchRemoteStore(AdaAgents.newToken(), serverId, instance.crtLeaderId, inst_no, instance);
                    //localStore.store(instance.crtLeaderId, inst_no, instance);
                    if (instance.status == InstanceStatus.COMMITTED)
                        fsyncSignature[inst_no] = true;
                }
            }
        }
        logger.logFormatted(false, "fsync", "backup", "fsyncInit="+fsyncInit_sta+"->"+fsyncInit_end, "upto="+inst_no);
        return inst_no;
    }

    protected void memorySynchronize(final long token){
        int inst_no = consecutiveCommit.get();
        AdaPaxosInstance instance = instanceSpace.get(inst_no);

        while (instance != null && instance.status == InstanceStatus.COMMITTED){
            ++inst_no;
            instance = instanceSpace.get(inst_no);
        }

        recoveryList.set(inst_no, new AdaRecoveryMaintenance(token, diskSize));
        for (int disk_no = 0; disk_no < diskSize; disk_no++) {
            for (int leaderId = 0; leaderId < peerSize; leaderId++)
                remoteStore.launchRemoteFetch(token, disk_no, leaderId, inst_no);
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

    protected void finishDisk2Mem(long dialog_no, int vacant_no){
        if (recovery.stateCompareAndSet(LeaderElectionPerformer.LeaderElectionState.RECOVERING,
                LeaderElectionPerformer.LeaderElectionState.RECOVERED)){    // join LeaderElection
            LeaderElectionMessage.LeStart startSignal = new LeaderElectionMessage.LeStart(serverId, dialog_no, vacant_no-1);
            try {
                lMessages.put(startSignal);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    protected void thread_bipolar(final BipolarStateReminder reminder, final BipolarStateDecider decider){
        int lastState = decider.decide();
        while (reminder.remind() >= 0){
            int crtState = decider.decide();
            if (lastState != crtState){
                lastState = crtState;
                if (crtState == 0){
                    logger.record(false, "diag", "[" + System.currentTimeMillis() + "][state change][1->0]\n");
                    routineOnRunning.set(false);
                    asLeader.set(false);
                }
                else {
                    logger.record(false, "diag", "[" + System.currentTimeMillis() + "]" + "[state change][0->1][tkt="+fsyncInitInstance.get()+",init_lid="+nConfig.initLeaderId+"]\n");
                    instanceSpaceBuild(instanceSpace.length(), crtInstBallot.get(), fsyncInitInstance.get());
                    agent();
                    routine(supplementRoutines);
                }
            }
        }
    }
}
