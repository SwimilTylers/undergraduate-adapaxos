package network.service.receiver;

import com.sun.istack.internal.NotNull;
import network.service.module.simulator.SimulatorModule;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;

/**
 * @author : Swimiltylers
 * @version : 2019/2/26 21:15
 */
public class SimulatorReceiver implements PeerMessageReceiver{
    private int netServiceId;

    private PeerMessageProcessor processor;
    private SimulatorModule simulator;

    public SimulatorReceiver(int netServiceId, PeerMessageProcessor processor,
                             @NotNull SimulatorModule simulator) {
        this.netServiceId = netServiceId;
        this.processor = processor;
        this.simulator = simulator;
    }

    @Override
    public void listenToPeers(final Socket chan, final int id) {
        while (true){
            Object msg;
            try {
                msg = (new ObjectInputStream(chan.getInputStream())).readObject();
            } catch (IOException |ClassNotFoundException e) {
                System.out.println("ERROR [server "+netServiceId+"]: " + e.getMessage());
                continue;
            }
            try {
                if (!simulator.crush())
                    processor.messageProcess(msg, id);
            } catch (InterruptedException e) {
                break;
            }
        }
    }
}
