package network;

import network.service.GenericNetService;
import network.service.module.controller.GlobalBipolarController;
import network.service.sender.BipolarPeerMessageSender;
import network.service.sender.BipolarSenderDecider;
import network.service.sender.DummyPeerMessageSender;
import network.service.sender.PeerMessageSender;

/**
 * @author : Swimiltylers
 * @version : 2019/4/19 23:10
 */
public class BipolarNetService extends GenericNetService {
    private GlobalBipolarController controller;

    public BipolarNetService(int thisId, GlobalBipolarController controller) {
        super(thisId);
        this.controller = controller;
    }

    @Override
    protected void setPeerMessageProcessor() {
        super.setPeerMessageProcessor();
        sender = new BipolarPeerMessageSender(new PeerMessageSender[]{new DummyPeerMessageSender(), sender}, controller.getDecider(netServiceId));
    }
}
