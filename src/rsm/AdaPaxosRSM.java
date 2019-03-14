package rsm;

import client.ClientRequest;
import instance.InstanceStatus;
import instance.PaxosInstance;
import instance.store.InstanceStore;
import instance.store.OffsetIndexStore;
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
public class AdaPaxosRSM {
    /* unique identity */
    protected int serverId;
    protected boolean asLeader;


    /* net and connect */
    protected int peerSize;
    protected GenericNetService net;
    protected ConnectionModule conn;

    /* channels */
    protected BlockingQueue<ClientRequest[]> batchedRequestChan;
    protected Queue<ClientRequest> restoredQueue;

    protected BlockingQueue<ClientRequest> cMessages;
    protected BlockingQueue<GenericPaxosMessage> pMessages;
    protected BlockingQueue<DiskPaxosMessage> dMessages;
    protected BlockingQueue<AdaPaxosMessage> aMessages;
    protected List<Pair<Distinguishable, BlockingQueue>> customizedChannels;


    /* instance and storage */
    protected AtomicReferenceArray<PaxosInstance> instanceSpace;
    protected AtomicInteger crtInstBallot;
    protected AtomicInteger maxReceivedInstance;
    protected AtomicInteger maxSendInstance;

    protected AtomicInteger consecutiveCommit;
    protected AtomicInteger fsyncInitInstance;

    protected InstanceStore localStore;
    protected RemoteInstanceStore remoteStore;
    protected AtomicBoolean forceFsync;
    protected int fsyncItv;
    protected AtomicIntegerArray fsyncSignature;


    /* misc */
    protected PaxosLogger logger;
    protected AtomicBoolean routineOnRunning;


    protected AdaPaxosRSM(final int serverId,
                       final boolean initAsLeader,
                       PaxosLogger logger) {

        this.serverId = serverId;
        this.asLeader = initAsLeader;
        this.logger = logger;
    }

    /* protected-access build func */

    protected AdaPaxosRSM netConnectionBuild(GenericNetService net, ConnectionModule conn, int peerSize){
        this.peerSize = peerSize;
        this.net = net;
        this.conn = conn;

        net.setLogger(logger);

        return this;
    }

    protected AdaPaxosRSM batchBuild(final int sizeBatchChan){
        batchedRequestChan = new ArrayBlockingQueue<>(sizeBatchChan);
        restoredQueue = new ConcurrentLinkedQueue<>();

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
        fsyncInitInstance = new AtomicInteger(initFsyncInstance);

        return this;
    }

    protected AdaPaxosRSM instanceStorageBuild(InstanceStore localStore,
                                        RemoteInstanceStore remoteStore,
                                        boolean initFsync,
                                        final int fsyncItv){

        this.localStore = localStore;
        this.remoteStore = remoteStore;
        forceFsync = new AtomicBoolean(initFsync);
        this.fsyncItv = fsyncItv;
        fsyncSignature = new AtomicIntegerArray(instanceSpace.length());

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

        if (supplement != null && supplement.length != 0) {
            customizedChannels = new ArrayList<>();
            customizedChannels.addAll(Arrays.asList(supplement));
        }

        return this;
    }

    @SuppressWarnings("unchecked")
    public static AdaPaxosRSM makeInstance(final int id, final int epoch, final int peerSize, GenericNetService net, boolean initAsLeader){
        AdaPaxosRSM rsm = new AdaPaxosRSM(id, initAsLeader, new NaiveLogger(id));
        rsm.netConnectionBuild(net, net.getConnectionModule(), peerSize)
           .batchBuild(1)
           .instanceSpaceBuild(GenericPaxosSMR.DEFAULT_INSTANCE_SIZE, epoch << 16 + id, 0)
           .instanceStorageBuild(new OffsetIndexStore(DiskPaxosRSM.LOCAL_STORAGE_PREFIX+id), null, false, PseudoAdaPaxosRSM.DEFAULT_FSYNC_WIN)
           .messageChanBuild(GenericPaxosSMR.DEFAULT_MESSAGE_SIZE, GenericPaxosSMR.DEFAULT_MESSAGE_SIZE, GenericPaxosSMR.DEFAULT_MESSAGE_SIZE, GenericPaxosSMR.DEFAULT_INSTANCE_SIZE);
        return rsm;
    }

    /* public-access deployment func, including:
    * - link: connection establishment of both net and remote-disk */

    public void link(String[] peerAddr, int[] peerPort) throws InterruptedException {
        assert peerAddr.length == peerPort.length && peerAddr.length == peerSize;

        net.setClientChan(cMessages);
        net.setPaxosChan(pMessages);
        net.registerChannel(o->o instanceof DiskPaxosMessage, dMessages);
        net.registerChannel(o->o instanceof AdaPaxosMessage, aMessages);
        for (Pair<Distinguishable, BlockingQueue> chan : customizedChannels) {
            net.registerChannel(chan.getKey(), chan.getValue());
        }

        net.connect(peerAddr, peerPort);
        remoteStore.connect();
    }

    public void agent(){

    }

    public void routine(Runnable... supplement){
        ExecutorService routines = Executors.newCachedThreadPool();
        routines.execute(()-> routine_batch(GenericPaxosSMR.DEFAULT_COMPACT_INTERVAL, GenericPaxosSMR.DEFAULT_REQUEST_COMPACTING_SIZE));
        routines.execute(()-> routine_propose(GenericPaxosSMR.DEFAULT_COMPACT_INTERVAL));
        routines.execute(this::routine_response);
        if (supplement != null && supplement.length != 0)
            for (Runnable r : supplement) {
                routines.execute(r);
            }
        routines.shutdown();
    }

    /* protected-access routine func, including:
    * - batch: batch up requests from both clients or restoredQueue
    * - propose: initiate proposal on batched requests
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
                recv = restoredQueue.poll();
                ++count;
            }

            try {
                batchedRequestChan.put(requestList.toArray(new ClientRequest[0]));
            } catch (InterruptedException e) {
                e.printStackTrace();
                return;
            }

            try {
                Thread.sleep(batchItv); // drop the refreshing frequency of 'batch'
            } catch (InterruptedException e) {
                e.printStackTrace();
                return;
            }
        }
    }

    protected void routine_propose(final int proposeItv){
        while (routineOnRunning.get()){
            ClientRequest[] cmd = batchedRequestChan.poll();
            if (cmd != null){
                // TODO: aProposer
            }

            try {
                Thread.sleep(proposeItv); // drop the refreshing frequency of 'propose'
            } catch (InterruptedException e) {
                e.printStackTrace();
                return;
            }
        }
    }

    protected void routine_monitor(final int monitorItv, final int expire, final int stability){
        final int bare_majority = (peerSize+1)/2 + 1;
        int stableConnCount = 0;

        while (routineOnRunning.get()){
            AdaPaxosMessage message = aMessages.poll();
            if (message != null){
                if (message.fsync){
                    forceFsync.set(true);
                    fileSynchronize();
                }
                else {
                    forceFsync.set(false);
                    memorySynchronize();
                }
            }
            else if (asLeader){
                int[] crushed = conn.filter(expire);
                if (crushed.length <= bare_majority){
                    stableConnCount = 0;
                    forceFsync.set(true);
                    fileSynchronize();
                }
                else {
                    ++stableConnCount;
                    if (stableConnCount >= stability){
                        forceFsync.set(false);
                        memorySynchronize();
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

    protected void routine_response(){
        while (routineOnRunning.get()){
            GenericPaxosMessage msg = pMessages.poll();

            if (msg != null) {
                Pair<Integer, Object> retention = null;
                maxReceivedInstance.updateAndGet(i->i=Integer.max(i, 0));   // TODO: inst_no ?

                if (msg instanceof GenericPaxosMessage.Prepare) {
                    GenericPaxosMessage.Prepare cast = (GenericPaxosMessage.Prepare) msg;
                    //acceptor.handlePrepare(cast);
                } else if (msg instanceof GenericPaxosMessage.ackPrepare) {
                    GenericPaxosMessage.ackPrepare cast = (GenericPaxosMessage.ackPrepare) msg;
                    //proposer.handleAckPrepare(cast);
                } else if (msg instanceof GenericPaxosMessage.Accept) {
                    GenericPaxosMessage.Accept cast = (GenericPaxosMessage.Accept) msg;
                    //acceptor.handleAccept(cast);
                } else if (msg instanceof GenericPaxosMessage.ackAccept) {
                    GenericPaxosMessage.ackAccept cast = (GenericPaxosMessage.ackAccept) msg;
                    //learner.handleAckAccept(cast);
                } else if (msg instanceof GenericPaxosMessage.Commit) {
                    GenericPaxosMessage.Commit cast = (GenericPaxosMessage.Commit) msg;
                    //learner.handleCommit(cast);
                    updateConsecutiveCommit();
                } else if (msg instanceof GenericPaxosMessage.Restore) {
                    GenericPaxosMessage.Restore cast = (GenericPaxosMessage.Restore) msg;
                    //handleRestore(cast);
                }

                if (forceFsync.get())
                    fileSynchronize(0); // TODO: inst_no ?
                else if (retention != null){    // in case of retention
                    if (retention.getKey() == null) {
                        net.getPeerMessageSender().broadcastPeerMessage(retention.getValue());
                    } else {
                        net.getPeerMessageSender().sendPeerMessage(retention.getKey(), retention.getValue());
                    }
                }
            }
        }
    }

    protected void routine_backup(final int backupItv){
        while(routineOnRunning.get()){
            if (!forceFsync.get())
                fileSynchronize();

            try {
                Thread.sleep(backupItv); // drop the refreshing frequency of 'backup'
            } catch (InterruptedException e) {
                e.printStackTrace();
                return;
            }
        }
    }

    protected void routine_leadership(final int leadershipItv){

    }

    /* protected-access file2mem-mem2file func */

    synchronized protected void fileSynchronize(){

    }

    synchronized protected void memorySynchronize(){

    }

    synchronized protected void fileSynchronize(final int specific){

    }

    /* protected-access misc func */

    /*
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

    */

    protected void updateConsecutiveCommit(){
        int iter = consecutiveCommit.get();
        while (iter < Integer.max(maxReceivedInstance.get(), maxSendInstance.get())){
            PaxosInstance inst = instanceSpace.get(iter);
            if (inst != null && inst.status == InstanceStatus.COMMITTED)
                ++iter;
        }
        consecutiveCommit.set(iter);
    }
}
