package network.service.receiver;

import com.sun.istack.internal.NotNull;

import java.net.Socket;

/**
 * @author : Swimiltylers
 * @version : 2019/2/26 19:50
 */
public interface PeerMessageReceiver {
    void listenToPeers(@NotNull Socket chan);
    void messageProcess(Object msg) throws InterruptedException;
}