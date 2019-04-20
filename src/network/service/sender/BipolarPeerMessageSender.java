package network.service.sender;

import network.service.module.controller.BipolarStateDecider;

/**
 * @author : Swimiltylers
 * @version : 2019/4/19 17:34
 */
public class BipolarPeerMessageSender implements PeerMessageSender{
    private final PeerMessageSender[] bipolar;
    private final BipolarStateDecider decider;

    public BipolarPeerMessageSender(PeerMessageSender[] bipolar, BipolarStateDecider decider) {
        this.bipolar = bipolar;
        this.decider = decider;
    }

    @Override
    public void sendPeerMessage(int toId, Object msg) {
        PeerMessageSender sender = bipolar[decider.decide()];
        sender.sendPeerMessage(toId, msg);
    }

    @Override
    public void broadcastPeerMessage(Object msg) {
        PeerMessageSender sender = bipolar[decider.decide()];
        sender.broadcastPeerMessage(msg);
    }
}
