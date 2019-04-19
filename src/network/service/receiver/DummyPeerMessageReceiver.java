package network.service.receiver;

import java.net.Socket;

/**
 * @author : Swimiltylers
 * @version : 2019/4/19 11:22
 */
public class DummyPeerMessageReceiver implements PeerMessageReceiver{
    @Override
    public void listenToPeers(Socket chan, int id) {

    }
}
