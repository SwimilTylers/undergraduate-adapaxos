package rsm;

import agent.acceptor.IntegratedDiskAcceptor;
import agent.learner.DiskLearner;
import agent.proposer.DiskProposer;
import client.ClientRequest;
import instance.store.InstanceStore;
import instance.store.OffsetIndexStore;
import javafx.util.Pair;
import logger.PaxosLogger;
import network.message.protocols.DiskPaxosMessage;
import network.message.protocols.GenericPaxosMessage;
import network.service.SimulatedNetService;
import network.service.module.simulator.CrushedSimulator;
import network.service.module.simulator.DelayedSimulator;

import java.util.Arrays;
import java.util.concurrent.*;

/**
 * @author : Swimiltylers
 * @version : 2019/2/18 15:09
 */
public class DiskPaxosRSM extends GenericPaxosSMR{
    public static final String LOCAL_STORAGE_PREFIX = "disk-";
    private DiskProposer dProposer;
    private IntegratedDiskAcceptor dAcceptor;
    private DiskLearner dLearner;

    private BlockingQueue<DiskPaxosMessage> dMessage;
    private InstanceStore store;

    public DiskPaxosRSM(int id, String[] addr, int[] port) {
        super(id, addr, port);

        //if (id == 2)
        //    net = new SimulatedNetService(net, new CrushedSimulator());
        //net = new SimulatedNetService(net, id != 2 ? new DelayedSimulator(20, 0) : new CrushedSimulator());

        dMessage = new ArrayBlockingQueue<>(DEFAULT_MESSAGE_SIZE);
        store = new OffsetIndexStore(LOCAL_STORAGE_PREFIX+id);

        customizedChannels.add(new Pair<>(o -> o instanceof DiskPaxosMessage, dMessage));
    }

    public DiskPaxosRSM(int id, String[] addr, int[] port, InstanceStore store, PaxosLogger logger) {
        super(id, addr, port, logger);

        net = new SimulatedNetService(net, id != 2 ? new DelayedSimulator(100, 10) : new CrushedSimulator());

        dMessage = new ArrayBlockingQueue<>(DEFAULT_MESSAGE_SIZE);
        this.store = store;

        customizedChannels.add(new Pair<>(o -> o instanceof DiskPaxosMessage, dMessage));
    }

    public DiskProposer getProposer() {
        return dProposer;
    }

    public IntegratedDiskAcceptor getAcceptor() {
        return dAcceptor;
    }

    public DiskLearner getLearner() {
        return dLearner;
    }

    @Override
    protected void agentDeployment() {
        dProposer = new DiskProposer(serverId, peerSize, instanceSpace, net.getPeerMessageSender(), restoredRequestList);
        dAcceptor = IntegratedDiskAcceptor.makeInstance(net.getPeerMessageSender(), serverId, store, logger);
        dLearner = new DiskLearner(serverId, peerSize, instanceSpace, net.getPeerMessageSender(), restoredRequestList, logger);
    }

    private void genericPaxosMessageHandler(){
        GenericPaxosMessage msg = pMessage.poll();

        if (msg != null) {
            if (msg instanceof GenericPaxosMessage.Commit) {
                GenericPaxosMessage.Commit cast = (GenericPaxosMessage.Commit) msg;
                logger.logCommit(cast.inst_no, cast, "handle");
                dLearner.handleCommit(cast);
                updateConsecutiveCommit();
                logger.logCommit(cast.inst_no, cast, "exit handle");
            } else if (msg instanceof GenericPaxosMessage.Restore) {
                GenericPaxosMessage.Restore cast = (GenericPaxosMessage.Restore) msg;
                logger.logRestore(cast.inst_no, cast, "handle");
                handleRestore(cast);
                logger.logRestore(cast.inst_no, cast, "exit handle");
            }
        }
    }

    void diskPaxosMessageHandler() {
        DiskPaxosMessage msg = dMessage.poll();

        if (msg != null) {
            if (msg instanceof DiskPaxosMessage.PackedMessage) {
                DiskPaxosMessage.PackedMessage cast = (DiskPaxosMessage.PackedMessage) msg;
                // logger.logPrepare(cast.inst_no, cast, "handle");
                if (cast.desc.equals(DiskPaxosMessage.IRW_HEADER) || cast.desc.equals(DiskPaxosMessage.IR_HEADER)) {
                    logger.logFormatted(true, "DISK-PAXOS", cast.desc, "acceptor-"+serverId, "handle");
                    dAcceptor.handlePacked(cast);
                    logger.logFormatted(true, "DISK-PAXOS", cast.desc, "acceptor-"+serverId, "exit handle");
                }
                else if (cast.desc.equals(DiskPaxosMessage.IRW_ACK_HEADER) || cast.desc.equals(DiskPaxosMessage.IR_ACK_HEADER)){
                    /* we cannot distinguish which step the message involves in,
                     * so we give both of them an opportunity.
                     *
                     * The order of the following trials should not change:
                     *   - PREPARING ---> PREPARED: risk of duplicated-trial
                     *   - COMMITTED -x-> PREPARING: NO such risk */

                    logger.logFormatted(true, "DISK-PAXOS", cast.desc, "learner-"+serverId, "trial", "handle");
                    boolean isProcessed = dLearner.handlePacked(cast);
                    logger.logFormatted(true, "DISK-PAXOS", cast.desc, "learner-"+serverId, "processed="+isProcessed, "exit handle");

                    if (!isProcessed) {
                        logger.logFormatted(true, "DISK-PAXOS", cast.desc, "proposer-"+serverId, "trial", "handle");
                        isProcessed =dProposer.handlePacked(cast);
                        logger.logFormatted(true, "DISK-PAXOS", cast.desc, "proposer-"+serverId, "processed="+isProcessed, "exit handle");
                    }
                }
            }
        }
    }

    @Override
    protected void peerConversation() {
        diskPaxosMessageHandler();
        genericPaxosMessageHandler();
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
            logger.log(true, "xixi "+ Arrays.toString(compact)+"\n");
            dProposer.handleRequests(maxInstance.getAndIncrement(), crtBallot.getAndIncrement(), compact);
        }
    }
}
