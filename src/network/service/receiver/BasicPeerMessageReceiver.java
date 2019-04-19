package network.service.receiver;

import com.sun.istack.internal.NotNull;
import javafx.util.Pair;
import logger.PaxosLogger;
import network.message.protocols.Distinguishable;
import network.message.protocols.GenericConnectionMessage;
import network.message.protocols.GenericPaxosMessage;
import network.message.protocols.TaggedMessage;
import network.service.module.ConnectionModule;
import network.service.sender.PeerMessageSender;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author : Swimiltylers
 * @version : 2019/2/26 20:13
 */
public class BasicPeerMessageReceiver implements PeerMessageReceiver, PeerMessageProcessor {
    private int netServiceId;

    private PeerMessageSender sender;

    private ConnectionModule cModule;

    private BlockingQueue<GenericPaxosMessage> paxosChan;
    private List<Pair<Distinguishable, BlockingQueue>> channels;

    private PaxosLogger logger;

    private ExecutorService msgProcessor;

    public BasicPeerMessageReceiver(int netServiceId,
                                    @NotNull PeerMessageSender sender,
                                    @NotNull ConnectionModule cModule,
                                    @NotNull BlockingQueue<GenericPaxosMessage> paxosChan,
                                    @NotNull List<Pair<Distinguishable, BlockingQueue>> channels, PaxosLogger logger) {

        this.netServiceId = netServiceId;
        this.sender = sender;
        this.cModule = cModule;
        this.paxosChan = paxosChan;
        this.channels = channels;
        this.logger = logger;

        msgProcessor = Executors.newCachedThreadPool();
    }

    @Override
    protected void finalize() throws Throwable {
        if (msgProcessor != null)
            msgProcessor.shutdown();
        super.finalize();
    }

    @Override
    public void listenToPeers(@NotNull final Socket chan, final int id){
        while (true){
            Object msg;
            try {
                msg = (new ObjectInputStream(chan.getInputStream())).readObject();
            } catch (IOException |ClassNotFoundException e) {
                System.out.println("ERROR [server "+netServiceId+"]: " + e.getMessage());
                continue;
            }
            try {
                messageProcess(msg, id);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void messageProcess(final Object wrapped, int fromId) throws InterruptedException {
        long tag = ((TaggedMessage) wrapped).tag;
        final Object msg = ((TaggedMessage) wrapped).load;

        if (msg instanceof GenericConnectionMessage.Beacon){
            long ts = System.currentTimeMillis();
            GenericConnectionMessage.Beacon cast = (GenericConnectionMessage.Beacon) msg;
            GenericConnectionMessage.ackBeacon ack = cModule.makeAck(ts, cast);
            if (ack != null) sender.sendPeerMessage(cast.fromId, ack);
            cModule.update(fromId, tag);
        }
        else if (msg instanceof GenericConnectionMessage.ackBeacon){
            long ts = System.currentTimeMillis();
            GenericConnectionMessage.ackBeacon cast = (GenericConnectionMessage.ackBeacon) msg;
            cModule.updateRound(ts, cast);
        }
        else if (msg instanceof GenericPaxosMessage){
            cModule.update(fromId, tag);
            msgProcessor.execute(()->{
                GenericPaxosMessage cast = (GenericPaxosMessage) msg;
                logger.logPeerNet(fromId, netServiceId, cast.toString());
                try {
                    paxosChan.put(cast);
                    logger.logPeerNet(fromId, netServiceId, "pSize="+paxosChan.size());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
        else{
            cModule.update(fromId, tag);
            msgProcessor.execute(()->{
                for (Pair<Distinguishable, BlockingQueue> t:channels) {
                    if (t.getKey().meet(msg)){
                        try {
                            logger.logPeerNet(fromId, netServiceId, msg.toString());
                            t.getValue().put(msg);
                            break;
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
    }
}
