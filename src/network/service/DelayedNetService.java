package network.service;

import network.service.receiver.DelayedPeerMessageReceiver;
import network.service.receiver.PeerMessageProcessor;

/**
 * @author : Swimiltylers
 * @version : 2019/4/19 15:37
 */
public class DelayedNetService extends GenericNetService{
    private int[] delayed;

    public DelayedNetService(int thisId, int[] delayed) {
        super(thisId);
        this.delayed = delayed;
    }

    @Override
    protected void setPeerMessageProcessor() {
        super.setPeerMessageProcessor();
        if (receiver instanceof PeerMessageProcessor)
            receiver = new DelayedPeerMessageReceiver((PeerMessageProcessor) receiver, delayed);
    }
}
