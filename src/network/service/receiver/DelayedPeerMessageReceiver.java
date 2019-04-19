package network.service.receiver;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;

/**
 * @author : Swimiltylers
 * @version : 2019/4/19 15:55
 */
public class DelayedPeerMessageReceiver implements PeerMessageReceiver {
    private PeerMessageProcessor processor;
    private int[] delayed;

    public DelayedPeerMessageReceiver(PeerMessageProcessor processor, int[] delayed) {
        this.processor = processor;
        this.delayed = delayed;
    }

    @Override
    public void listenToPeers(Socket chan, int id) {
        if (id < delayed.length) {
            while (true) {
                try {
                    Object msg = (new ObjectInputStream(chan.getInputStream())).readObject();
                    Thread.sleep(delayed[id]);
                    processor.messageProcess(msg, id);
                } catch (IOException | ClassNotFoundException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        else {
            while (true) {
                try {
                    Object msg = (new ObjectInputStream(chan.getInputStream())).readObject();
                    processor.messageProcess(msg, id);
                } catch (IOException | ClassNotFoundException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
