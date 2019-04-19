package network.service.sender;

/**
 * @author : Swimiltylers
 * @version : 2019/4/19 11:18
 */
public class DummyPeerMessageSender implements PeerMessageSender{
    @Override
    public void sendPeerMessage(int toId, Object msg) {

    }

    @Override
    public void broadcastPeerMessage(Object msg) {

    }
}
