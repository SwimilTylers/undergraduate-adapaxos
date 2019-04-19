package network.service.sender;

/**
 * @author : Swimiltylers
 * @version : 2019/4/19 17:34
 */
public class BipolarPeerMessageSender implements PeerMessageSender{
    private final PeerMessageSender[] bipolar;
    private final BipolarSenderDecider decider;

    public BipolarPeerMessageSender(PeerMessageSender[] bipolar, BipolarSenderDecider decider) {
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
