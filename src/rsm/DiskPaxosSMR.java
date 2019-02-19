package rsm;

import agent.acceptor.IntegratedDiskAcceptor;
import agent.learner.DiskLearner;
import agent.proposer.DiskProposer;
import client.ClientRequest;
import instance.store.InstanceStore;
import instance.store.OffsetIndexStore;
import network.message.protocols.DiskPaxosMessage;
import network.message.protocols.GenericPaxosMessage;

import java.util.concurrent.*;

/**
 * @author : Swimiltylers
 * @version : 2019/2/18 15:09
 */
public class DiskPaxosSMR extends GenericPaxosSMR{
    private DiskProposer dProposer;
    private IntegratedDiskAcceptor dAcceptor;
    private DiskLearner dLearner;

    private BlockingQueue<DiskPaxosMessage> dMessage;
    private InstanceStore store;

    public DiskPaxosSMR(int id, String[] addr, int[] port) {
        super(id, addr, port);

        dMessage = new ArrayBlockingQueue<>(DEFAULT_MESSAGE_SIZE);
        store = new OffsetIndexStore("disk-"+id);
    }

    @Override
    public void run() {
        try {
            net.connect(peerAddr, peerPort);
            net.registerChannel(o -> o instanceof DiskPaxosMessage, dMessage);
        } catch (InterruptedException e) {
            System.out.println("Net Connection is interrupted: "+e.getMessage());
            return;
        }

        dProposer = new DiskProposer(serverId, peerSize, instanceSpace, net, restoredRequestList);
        dAcceptor = IntegratedDiskAcceptor.makeInstance(net, serverId, store, logger);
        dLearner = new DiskLearner(serverId, peerSize, instanceSpace, net, restoredRequestList, logger);

        ExecutorService service = Executors.newCachedThreadPool();

        if (serverId == 0) {
            System.out.println("Server-"+serverId+" is watching");
            service.execute(() -> net.watch());
        }

        service.execute(this::compact);
        service.execute(this::paxosRoutine);

        service.shutdown();
    }

    @Override
    protected void paxosRoutine() {
        while (true) {
            try {
                diskConversation();
                peerConversation();
            } catch (Exception ignored){}

            ClientRequest[] compact = null;
            try {
                compact = compactChan.poll(clientComWaiting, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (compact != null)
                dProposer.handleRequests(compact);
        }
    }

    @Override
    protected void peerConversation() {
        GenericPaxosMessage msg;
        msg = pMessage.poll();

        if (msg != null) {
            if (msg instanceof GenericPaxosMessage.Commit) {
                GenericPaxosMessage.Commit cast = (GenericPaxosMessage.Commit) msg;
                logger.logCommit(cast.inst_no, cast, "handle");
                dLearner.handleCommit(cast);
                logger.logCommit(cast.inst_no, cast, "exit handle");
            }
        }
    }

    private void diskConversation() {
        DiskPaxosMessage msg;
        try {
            msg = dMessage.poll(peerComWaiting, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            System.out.println("Unsuccessfully message taking");
            return;
        }

        if (msg != null) {
            if (msg instanceof DiskPaxosMessage.PackedMessage) {
                DiskPaxosMessage.PackedMessage cast = (DiskPaxosMessage.PackedMessage) msg;
                // logger.logPrepare(cast.inst_no, cast, "handle");
                if (cast.desc.equals(DiskPaxosMessage.IRW_HEADER)) {
                    logger.logFormatted(true, "hehe");
                    dAcceptor.handleIntegrated(cast);
                }
                else if (cast.desc.equals(DiskPaxosMessage.IRW_ACK_HEADER)){
                    DiskPaxosMessage.ackWrite ackWrite = (DiskPaxosMessage.ackWrite) cast.packages[0];
                    DiskPaxosMessage.ackRead[] ackReads = (DiskPaxosMessage.ackRead[]) ((DiskPaxosMessage.PackedMessage) cast.packages[1]).packages;

                    /* we cannot distinguish which step the message involves in,
                    * so we give both of them an opportunity.
                    *
                    * The order of the following trials should not change:
                    *   - PREPARING ---> PREPARED: risk of duplicated-trial
                    *   - COMMITTED -x-> PREPARING: NO such risk */

                    dLearner.handle(ackWrite, ackReads);
                    dProposer.handle(ackWrite, ackReads);
                }
            }
        }
    }
}
