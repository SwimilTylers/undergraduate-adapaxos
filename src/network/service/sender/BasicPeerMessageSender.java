package network.service.sender;

import com.sun.istack.internal.NotNull;
import logger.PaxosLogger;
import network.message.protocols.GenericConnectionMessage;
import network.message.protocols.TaggedMessage;
import network.service.module.connection.ConnectionModule;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * @author : Swimiltylers
 * @version : 2019/2/26 19:48
 */
public class BasicPeerMessageSender implements PeerMessageSender {
    protected int netServiceId;
    protected int peerSize;
    private Socket[] peers;

    private ConnectionModule cModule;

    private PaxosLogger logger;

    public BasicPeerMessageSender(int netServiceId, int peerSize, Socket[] peers, ConnectionModule cModule, PaxosLogger logger) {
        this.netServiceId = netServiceId;
        this.peerSize = peerSize;
        this.peers = peers;
        this.cModule = cModule;
        this.logger = logger;
    }

    @Override
    synchronized public void sendPeerMessage(int toId, @NotNull Object msg){
        if (toId < peerSize){
            try {
                if (! (msg instanceof GenericConnectionMessage))
                    logger.logPeerNet(netServiceId, toId, msg.toString());
                OutputStream socketStream = peers[toId].getOutputStream();
                ObjectOutputStream ostream = new ObjectOutputStream(socketStream);
                ostream.writeObject(new TaggedMessage(System.currentTimeMillis(), msg));
                ostream.flush();
                socketStream.flush();
            } catch (IOException e) {
                System.out.println("Paxos Message send faliure: "+e.getMessage());
            }
        }
    }

    @Override
    synchronized public void broadcastPeerMessage(@NotNull Object msg){
        for (int i = 0; i < peerSize; i++) {
            if (i != netServiceId){
                sendPeerMessage(i, msg);
            }
        }
    }
}
