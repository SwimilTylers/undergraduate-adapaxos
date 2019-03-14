package network.service;

import client.ClientRequest;
import logger.PaxosLogger;
import network.message.protocols.GenericPaxosMessage;
import network.service.module.simulator.SimulatorModule;
import network.service.receiver.BasicPeerMessageReceiver;
import network.service.sender.BasicPeerMessageSender;
import network.service.receiver.SimulatorReceiver;
import network.service.sender.SimulatorSender;

import java.util.concurrent.BlockingQueue;

/**
 * @author : Swimiltylers
 * @version : 2019/2/26 20:49
 */
public class SimulatedNetService extends GenericNetService{
    private SimulatorModule simulator;

    public SimulatedNetService(int thisId,
                               int toClientPort,
                               BlockingQueue<ClientRequest> clientChan,
                               BlockingQueue<GenericPaxosMessage> paxosChan,
                               PaxosLogger logger, SimulatorModule simulator) {
        super(thisId, toClientPort, clientChan, paxosChan, logger);
        this.simulator = simulator;
    }

    public SimulatedNetService(GenericNetService net, SimulatorModule simulator){
        super(net.netServiceId, net.toClientPort, net.clientChan, net.paxosChan, net.logger);
        this.simulator = simulator;
    }

    @Override
    protected void peerTransDeployment() {
        sender = new SimulatorSender(netServiceId, peerSize, new BasicPeerMessageSender(netServiceId, peerSize, peers, cModule, logger), simulator);
        receiver = new SimulatorReceiver(netServiceId, new BasicPeerMessageReceiver(netServiceId, sender, cModule, paxosChan, channels), simulator);
    }
}
