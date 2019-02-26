package network.service.peer;

import com.sun.istack.internal.NotNull;

/**
 * @author : Swimiltylers
 * @version : 2019/2/18 13:53
 */
public interface PeerMessageSender {
    public void sendPeerMessage(int toId, @NotNull Object msg);
    public void broadcastPeerMessage(@NotNull Object msg);
}
