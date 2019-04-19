package network.service.receiver;

/**
 * @author : Swimiltylers
 * @version : 2019/4/19 11:24
 */

@FunctionalInterface
public interface PeerMessageProcessor {
    void messageProcess(Object msg, int fromId) throws InterruptedException;
}
