package network.service;

import network.message.protocols.GenericConnectionMessage;
import network.service.receiver.DelayedPeerMessageReceiver;
import network.service.receiver.PeerMessageProcessor;

import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author : Swimiltylers
 * @version : 2019/4/19 15:37
 */
@Deprecated
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

    @Override
    protected void openPeerListener() {
        for (int i = 0; i < peerSize; i++) {
            if (i != netServiceId){
                int id = i;
                Socket socket = peers[i];
                Thread rThread = new Thread(() -> receiver.listenToPeers(socket, id));
                rThread.setPriority(Thread.MAX_PRIORITY);
                rThread.start();
            }
        }
    }

}
