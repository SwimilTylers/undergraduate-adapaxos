package agent.acceptor;

import instance.PaxosInstance;
import instance.store.InstanceStore;
import network.message.protocols.DiskPaxosMessage;
import network.service.PeerMessageSender;

/**
 * @author : Swimiltylers
 * @version : 2019/2/18 12:38
 */
public class DiskAcceptor {
    protected PeerMessageSender net;
    protected int serverId;

    private InstanceStore store;

    public DiskAcceptor(PeerMessageSender net, int serverId, InstanceStore store) {
        this.net = net;
        this.serverId = serverId;
        this.store = store;
    }

    public void handle(DiskPaxosMessage.Write write){
        DiskPaxosMessage.ackWrite ackWrite = null;

        boolean check = store.store(write.leaderId, write.inst_no, write.load);

        if (check)
            ackWrite = new DiskPaxosMessage.ackWrite(
                    write.DIALOGUE_NO,
                    write.inst_no,
                    write.leaderId,
                    serverId,
                    DiskPaxosMessage.DiskStatus.WRITE_SUCCESS
            );

        net.sendPeerMessage(write.leaderId, ackWrite);
    }

    public void handle(DiskPaxosMessage.Read read){
        DiskPaxosMessage.ackRead ackRead = null;
        if (store.isExist(read.accessId, read.inst_no)){
            ackRead = new DiskPaxosMessage.ackRead(
                    read.DIALOGUE_NO,
                    read.inst_no,
                    read.leaderId,
                    serverId,
                    read.accessId,
                    DiskPaxosMessage.DiskStatus.READ_SUCCESS,
                    store.fetch(read.accessId, read.inst_no)
            );
        }
        else{
            ackRead = new DiskPaxosMessage.ackRead(
                    read.DIALOGUE_NO,
                    read.inst_no,
                    read.leaderId,
                    serverId,
                    read.accessId,
                    DiskPaxosMessage.DiskStatus.READ_NO_SUCH_FILE,
                    null
            );
        }

        net.sendPeerMessage(read.leaderId, ackRead);
    }
}
