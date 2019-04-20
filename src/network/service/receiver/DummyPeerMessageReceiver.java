package network.service.receiver;

import java.net.Socket;

/**
 * @author : Swimiltylers
 * @version : 2019/4/19 11:22
 */
public class DummyPeerMessageReceiver implements PeerMessageReceiver, PeerMessageProcessor{
    @Override
    public void listenToPeers(Socket chan, int id) {

    }

    @Override
    public void messageProcess(Object msg, int fromId) {

    }
}
