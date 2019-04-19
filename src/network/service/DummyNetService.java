package network.service;

import network.service.receiver.DummyPeerMessageReceiver;
import network.service.sender.DummyPeerMessageSender;

/**
 * @author : Swimiltylers
 * @version : 2019/4/19 11:33
 */
public class DummyNetService extends GenericNetService{
    public DummyNetService(int thisId) {
        super(thisId);
    }

    @Override
    protected void setPeerMessageProcessor() {
        sender = new DummyPeerMessageSender();
        receiver = new DummyPeerMessageReceiver();
    }
}
