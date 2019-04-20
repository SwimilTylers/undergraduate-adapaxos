package network.service.receiver;

import network.service.module.controller.BipolarStateDecider;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;

/**
 * @author : Swimiltylers
 * @version : 2019/4/20 11:48
 */
public class BipolarPeerMessageReceiver implements PeerMessageReceiver{
    private PeerMessageProcessor[] bipolar;
    private BipolarStateDecider decider;

    public BipolarPeerMessageReceiver(PeerMessageProcessor[] bipolar, BipolarStateDecider decider) {
        this.bipolar = bipolar;
        this.decider = decider;
    }

    @Override
    public void listenToPeers(Socket chan, int id) {
        while (true){
            try {
                Object msg = (new ObjectInputStream(chan.getInputStream())).readObject();
                bipolar[decider.decide()].messageProcess(msg, id);
            } catch (IOException |ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
}
