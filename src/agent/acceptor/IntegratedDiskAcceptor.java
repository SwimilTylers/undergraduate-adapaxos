package agent.acceptor;

import instance.store.InstanceStore;
import logger.PaxosLogger;
import network.message.protocols.DiskPaxosMessage;
import network.service.peer.BufferedDiskAcceptorSender;
import network.service.peer.PeerMessageSender;

/**
 * @author : Swimiltylers
 * @version : 2019/2/18 13:19
 */
public class IntegratedDiskAcceptor extends DiskAcceptor {

    private BufferedDiskAcceptorSender wrappedNet;

    private IntegratedDiskAcceptor(BufferedDiskAcceptorSender net, int serverId, InstanceStore store) {
        super(net, serverId, store);
        wrappedNet = net;
    }

    public static IntegratedDiskAcceptor makeInstance(PeerMessageSender net, int serverId, InstanceStore store, PaxosLogger logger){
        BufferedDiskAcceptorSender idNet = new BufferedDiskAcceptorSender(serverId, net, logger);
        return new IntegratedDiskAcceptor(idNet, serverId, store);
    }

    public void handlePacked(DiskPaxosMessage.PackedMessage packedMessage){
        if (packedMessage.desc.equals(DiskPaxosMessage.IRW_HEADER)){
            super.handle((DiskPaxosMessage.Write) packedMessage.packages[0]);

            for (DiskPaxosMessage m : ((DiskPaxosMessage.PackedMessage)packedMessage.packages[1]).packages) {
                if (m != null)  // in normal case, packages[leaderId] == null
                    super.handle((DiskPaxosMessage.Read) m);
            }
            wrappedNet.packAndSend(
                    DiskPaxosMessage.IRW_ACK_HEADER,
                    packedMessage.inst_no,
                    packedMessage.leaderId,
                    packedMessage.inst_ballot,
                    packedMessage.dialog_no
            );
        }
        else if (packedMessage.desc.equals(DiskPaxosMessage.IR_HEADER)){
            for (DiskPaxosMessage m:packedMessage.packages) {
                if (m != null)
                    super.handle((DiskPaxosMessage.Read) m);
            }
            wrappedNet.packAndSend(
                    DiskPaxosMessage.IR_ACK_HEADER,
                    packedMessage.inst_no,
                    packedMessage.leaderId,
                    packedMessage.inst_ballot,
                    packedMessage.dialog_no
            );
        }
    }
}
