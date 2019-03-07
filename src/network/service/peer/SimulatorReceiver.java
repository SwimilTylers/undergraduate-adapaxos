package network.service.peer;

import com.sun.istack.internal.NotNull;
import javafx.util.Pair;
import network.message.protocols.Distinguishable;
import network.message.protocols.GenericPaxosMessage;
import network.service.module.ConnectionModule;
import network.service.module.simulator.SimulatorModule;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * @author : Swimiltylers
 * @version : 2019/2/26 21:15
 */
public class SimulatorReceiver implements PeerMessageReceiver{
    private int netServiceId;

    private PeerMessageReceiver receiver;
    private SimulatorModule simulator;

    public SimulatorReceiver(int netServiceId, PeerMessageReceiver receiver,
                             @NotNull SimulatorModule simulator) {
        this.netServiceId = netServiceId;
        this.receiver = receiver;
        this.simulator = simulator;
    }

    @Override
    public void listenToPeers(Socket chan) {
        while (true){
            Object msg;
            try {
                msg = (new ObjectInputStream(chan.getInputStream())).readObject();
            } catch (IOException |ClassNotFoundException e) {
                System.out.println("ERROR [server "+netServiceId+"]: " + e.getMessage());
                continue;
            }
            try {
                putInChannel(msg);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    @Override
    public void putInChannel(Object msg) throws InterruptedException {
        if (!simulator.crush())
            receiver.putInChannel(msg);
    }
}
