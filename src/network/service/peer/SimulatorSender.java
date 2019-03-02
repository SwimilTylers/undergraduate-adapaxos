package network.service.peer;

import com.sun.istack.internal.NotNull;
import logger.PaxosLogger;
import network.service.module.ConnectionModule;
import network.service.module.simulator.SimulatorModule;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author : Swimiltylers
 * @version : 2019/2/26 20:57
 */
public class SimulatorSender implements PeerMessageSender{
    private int netServiceId;
    private int peerSize;

    private PeerMessageSender sender;
    private SimulatorModule simulator;

    public SimulatorSender(int netServiceId, int peerSize, @NotNull PeerMessageSender sender, @NotNull SimulatorModule simulator) {
        this.netServiceId = netServiceId;
        this.peerSize = peerSize;

        this.sender = sender;
        this.simulator = simulator;
    }

    @Override
    synchronized public void sendPeerMessage(int toId, @NotNull Object msg){
        if (!simulator.crush()) {
            if (!simulator.lost(toId)) {
                simulator.delay(toId);
                msg = simulator.byzantine(toId, msg);

                if (msg != null) {
                    sender.sendPeerMessage(toId, msg);
                }
            }
        }
    }


    @Override
    synchronized public void broadcastPeerMessage(@NotNull Object msg){
        if (!simulator.crush()) {
            ExecutorService service = Executors.newCachedThreadPool();

            for (int i = 0; i < peerSize; i++) {
                if (i != netServiceId) {
                    int id = i;
                    service.execute(() -> sendPeerMessage(id, msg));
                }
            }
        }
    }
}
