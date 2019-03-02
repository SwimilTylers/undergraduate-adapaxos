package network.service.peer;

import com.sun.istack.internal.NotNull;
import javafx.util.Pair;
import network.message.protocols.Distinguishable;
import network.message.protocols.GenericPaxosMessage;
import network.service.module.ConnectionModule;
import network.service.module.simulator.SimulatorModule;

import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * @author : Swimiltylers
 * @version : 2019/2/26 21:15
 */
public class SimulatorReceiver extends BasicPeerMessageReceiver{
    private SimulatorModule simulator;

    public SimulatorReceiver(int netServiceId,
                             @NotNull PeerMessageSender sender,
                             @NotNull ConnectionModule cModule,
                             @NotNull BlockingQueue<GenericPaxosMessage> paxosChan,
                             @NotNull List<Pair<Distinguishable, BlockingQueue>> channels,
                             @NotNull SimulatorModule simulator) {
        super(netServiceId, sender, cModule, paxosChan, channels);
        this.simulator = simulator;
    }

    @Override
    public void putInChannel(Object msg) throws InterruptedException {
        if (!simulator.crush())
            super.putInChannel(msg);
    }
}
