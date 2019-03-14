package network.service.sender;

import javafx.util.Pair;
import logger.PaxosLogger;
import network.message.protocols.DiskPaxosMessage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author : Swimiltylers
 * @version : 2019/2/26 20:59
 */
public class BufferedDiskAcceptorSender implements PeerMessageSender{
    private int netServiceId;
    private PeerMessageSender net;
    private PaxosLogger logger;

    private Map<Integer, Pair<DiskPaxosMessage.ackWrite, List<DiskPaxosMessage.ackRead>>> sendOutBuffer;

    public BufferedDiskAcceptorSender(int netServiceId, PeerMessageSender net, PaxosLogger logger) {
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

    synchronized public void packAndSend(String header, int inst_no, int leaderId, int inst_ballot, long dialogue_no){
        //System.out.println("@@"+netServiceId+": "+sendOutBuffer.size());

        for (Map.Entry<Integer, Pair<DiskPaxosMessage.ackWrite, List<DiskPaxosMessage.ackRead>>> entry :
                sendOutBuffer.entrySet()) {
            int toId = entry.getKey();
            Pair<DiskPaxosMessage.ackWrite, List<DiskPaxosMessage.ackRead>> info = entry.getValue();

            if (info.getValue().isEmpty()) {
                DiskPaxosMessage.PackedMessage sendOut = null;
                if (header.equals(DiskPaxosMessage.IRW_ACK_HEADER)) {
                    sendOut = DiskPaxosMessage.IRW_ACK(
                            inst_no,
                            leaderId,
                            inst_ballot,
                            dialogue_no,
                            info.getKey(),
                            null
                    );
                    logger.logPeerNet(netServiceId, toId, "PACKED: " + sendOut.toString());
                    net.sendPeerMessage(toId, sendOut);
                }
                else if (header.equals(DiskPaxosMessage.IR_ACK_HEADER)){
                    sendOut = DiskPaxosMessage.IR_ACK(inst_no, leaderId, inst_ballot, dialogue_no, null);
                    logger.logPeerNet(netServiceId, toId, "PACKED: " + sendOut.toString());
                    net.sendPeerMessage(toId, sendOut);
                }

                //System.out.println("$$" + sendOut);

            } else {
                DiskPaxosMessage.PackedMessage sendOut;
                if (header.equals(DiskPaxosMessage.IRW_ACK_HEADER)) {
                    sendOut= DiskPaxosMessage.IRW_ACK(
                            inst_no,
                            leaderId,
                            inst_ballot,
                            dialogue_no,
                            info.getKey(),
                            info.getValue().toArray(new DiskPaxosMessage.ackRead[0])
                    );
                    //System.out.println("%%" + sendOut);
                    logger.logPeerNet(netServiceId, toId, "PACKED: " + sendOut.toString());
                    net.sendPeerMessage(toId, sendOut);
                }
                else if (header.equals(DiskPaxosMessage.IR_ACK_HEADER)){
                    sendOut = DiskPaxosMessage.IR_ACK(inst_no, leaderId, inst_ballot, dialogue_no, info.getValue().toArray(new DiskPaxosMessage.ackRead[0]));
                    logger.logPeerNet(netServiceId, toId, "PACKED: " + sendOut.toString());
                    net.sendPeerMessage(toId, sendOut);
                }
            }
        }

        //System.out.println("->@@"+netServiceId+": "+sendOutBuffer.size());
        sendOutBuffer.clear();
    }
}
