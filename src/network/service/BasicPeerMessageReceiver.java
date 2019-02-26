package network.service;

import com.sun.istack.internal.NotNull;
import javafx.util.Pair;
import network.message.protocols.Distinguishable;
import network.message.protocols.GenericConnectionMessage;
import network.message.protocols.GenericPaxosMessage;
import network.service.module.ConnectionModule;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * @author : Swimiltylers
 * @version : 2019/2/26 20:13
 */
public class BasicPeerMessageReceiver implements PeerMessageReceiver{
    private int netServiceId;

    private PeerMessageSender sender;

    private ConnectionModule cModule;

    private BlockingQueue<GenericPaxosMessage> paxosChan;
    private List<Pair<Distinguishable, BlockingQueue>> channels;

    public BasicPeerMessageReceiver(int netServiceId,
                                    @NotNull PeerMessageSender sender,
                                    @NotNull ConnectionModule cModule,
                                    @NotNull BlockingQueue<GenericPaxosMessage> paxosChan,
                                    @NotNull List<Pair<Distinguishable, BlockingQueue>> channels) {

        this.netServiceId = netServiceId;
        this.sender = sender;
        this.cModule = cModule;
        this.paxosChan = paxosChan;
        this.channels = channels;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void listenToPeers(@NotNull Socket chan){
        while (true){
            Object msg;
            try {
                msg = (new ObjectInputStream(chan.getInputStream())).readObject();
            } catch (IOException |ClassNotFoundException e) {
                System.out.println("ERROR [server "+netServiceId+"]: " + e.getMessage());
                continue;
            }

            if (msg instanceof GenericConnectionMessage.Beacon){
                long ts = System.currentTimeMillis();
                GenericConnectionMessage.Beacon cast = (GenericConnectionMessage.Beacon) msg;
                GenericConnectionMessage.ackBeacon ack = cModule.ack(ts, cast);
                if (ack != null) sender.sendPeerMessage(cast.fromId, ack);
                cModule.updateByBeacon(ts, cast);
            }
            else if (msg instanceof GenericConnectionMessage.ackBeacon){
                long ts = System.currentTimeMillis();
                GenericConnectionMessage.ackBeacon cast = (GenericConnectionMessage.ackBeacon) msg;
                cModule.updateByAckBeacon(ts, cast);
            }
            else if (msg instanceof GenericPaxosMessage){
                GenericPaxosMessage cast = (GenericPaxosMessage) msg;
                try {
                    paxosChan.put(cast);
                } catch (InterruptedException e) {
                    System.out.println("Generic Paxos Message Interrupted");
                    break;
                }
            }
            else{
                for (Pair<Distinguishable, BlockingQueue> t:channels) {
                    if (t.getKey().meet(msg)){
                        try {
                            t.getValue().put(msg);
                            break;
                        } catch (InterruptedException e) {
                            System.out.println("Costumed Message Interrupted");
                            break;
                        }
                    }
                }
            }
        }
    }
}
