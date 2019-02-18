package agent.acceptor;

import instance.store.InstanceStore;
import javafx.util.Pair;
import logger.PaxosLogger;
import network.message.protocols.DiskPaxosMessage;
import network.service.PeerMessageSender;

import java.util.*;

/**
 * @author : Swimiltylers
 * @version : 2019/2/18 13:19
 */
public class IntegratedDiskAcceptor extends DiskAcceptor{
    static class IntegratedDiskAcceptorNetService implements PeerMessageSender{
        private int netServiceId;
        private PeerMessageSender net;
        private PaxosLogger logger;

        private Map<Integer, Pair<DiskPaxosMessage.ackWrite, List<DiskPaxosMessage.ackRead>>> sendOutBuffer;

        IntegratedDiskAcceptorNetService(int netServiceId, PeerMessageSender net, PaxosLogger logger) {
            this.netServiceId = netServiceId;
            this.net = net;
            this.logger = logger;
            this.sendOutBuffer = new HashMap<>();
        }

        private boolean filter(DiskPaxosMessage.ackRead msg){
            return msg.status == DiskPaxosMessage.DiskStatus.READ_NO_SUCH_FILE;
        }

        @Override
        synchronized public void sendPeerMessage(int toId, Object msg) {
            if (msg instanceof DiskPaxosMessage.ackWrite){
                sendOutBuffer.remove(toId);
                sendOutBuffer.put(toId, new Pair<>(((DiskPaxosMessage.ackWrite) msg), new ArrayList<>()));
            }
            else if (msg instanceof DiskPaxosMessage.ackRead){
                if (sendOutBuffer.containsKey(toId)) {
                    DiskPaxosMessage.ackRead cast = (DiskPaxosMessage.ackRead) msg;
                    if (!filter(cast))
                        sendOutBuffer.get(toId).getValue().add(cast);
                }
            }
            else
                net.sendPeerMessage(toId, msg);
        }

        @Override
        synchronized public void broadcastPeerMessage(Object msg) {
            net.broadcastPeerMessage(msg);
        }

        synchronized void packAndSend(){
            //System.out.println("@@"+netServiceId+": "+sendOutBuffer.size());

            for (Map.Entry<Integer, Pair<DiskPaxosMessage.ackWrite, List<DiskPaxosMessage.ackRead>>> entry :
                    sendOutBuffer.entrySet()) {
                int toId = entry.getKey();
                Pair<DiskPaxosMessage.ackWrite, List<DiskPaxosMessage.ackRead>> info = entry.getValue();

                if (info.getValue().isEmpty()) {
                    DiskPaxosMessage.PackedMessage sendOut = DiskPaxosMessage.integratedWriteAndRead_ack(
                            info.getKey().DIALOGUE_NO,
                            info.getKey(),
                            null
                    );
                    //System.out.println("$$" + sendOut);
                    logger.logPeerNet(netServiceId, toId, "PACKING: " + sendOut.toString());
                    net.sendPeerMessage(toId, sendOut);
                } else {
                    DiskPaxosMessage.PackedMessage sendOut = DiskPaxosMessage.integratedWriteAndRead_ack(
                            info.getKey().DIALOGUE_NO,
                            info.getKey(),
                            info.getValue().toArray(new DiskPaxosMessage.ackRead[0])
                    );
                    //System.out.println("%%" + sendOut);
                    logger.logPeerNet(netServiceId, toId, "PACKING: " + sendOut.toString());
                    net.sendPeerMessage(toId, sendOut);
                }
            }

            //System.out.println("->@@"+netServiceId+": "+sendOutBuffer.size());
            sendOutBuffer.clear();
        }
    }

    private IntegratedDiskAcceptorNetService wrappedNet;

    private IntegratedDiskAcceptor(IntegratedDiskAcceptorNetService net, int serverId, InstanceStore store) {
        super(net, serverId, store);
        wrappedNet = net;
    }

    public static IntegratedDiskAcceptor makeInstance(PeerMessageSender net, int serverId, InstanceStore store, PaxosLogger logger){
        IntegratedDiskAcceptorNetService idNet = new IntegratedDiskAcceptorNetService(serverId, net, logger);
        return new IntegratedDiskAcceptor(idNet, serverId, store);
    }

    public void handleIntegrated(DiskPaxosMessage.PackedMessage packedMessage){
        if (packedMessage.desc.equals(DiskPaxosMessage.IRW_HEADER)){
            super.handle((DiskPaxosMessage.Write) packedMessage.packages[0]);

            for (DiskPaxosMessage m : ((DiskPaxosMessage.PackedMessage)packedMessage.packages[1]).packages) {
                if (m != null)  // in normal case, packages[leaderId] == null
                    super.handle((DiskPaxosMessage.Read) m);
            }
            wrappedNet.packAndSend();
        }
    }
}
