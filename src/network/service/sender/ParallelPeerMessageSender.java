package network.service.sender;

import logger.PaxosLogger;
import network.service.module.ConnectionModule;

import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author : Swimiltylers
 * @version : 2019/4/13 15:19
 */
public class ParallelPeerMessageSender extends BasicPeerMessageSender{
    private ExecutorService sendService;

    public ParallelPeerMessageSender(int netServiceId, int peerSize, Socket[] peers, ConnectionModule cModule, PaxosLogger logger) {
        super(netServiceId, peerSize, peers, cModule, logger);
        sendService = Executors.newFixedThreadPool(peerSize);
    }

    @Override
    protected void finalize() throws Throwable {
        sendService.shutdown();
        super.finalize();
    }

    @Override
    public synchronized void broadcastPeerMessage(Object msg) {
        for (int i = 0; i < peerSize; i++) {
            if (i != netServiceId){
                int id = i;
                sendService.execute(() -> sendPeerMessage(id, msg));
            }
        }
    }
}
