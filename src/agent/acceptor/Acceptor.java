package agent.acceptor;

import network.message.protocols.GenericPaxosMessage;

/**
 * @author : Swimiltylers
 * @version : 2019/2/14 17:34
 */
public interface Acceptor {
    void handlePrepare(GenericPaxosMessage.Prepare prepare);
    void handleAccept(GenericPaxosMessage.Accept accept);
}
