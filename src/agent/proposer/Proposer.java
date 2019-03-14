package agent.proposer;

import client.ClientRequest;
import com.sun.istack.internal.NotNull;
import network.message.protocols.GenericPaxosMessage;

/**
 * @author : Swimiltylers
 * @version : 2019/2/14 17:32
 */
public interface Proposer {
    void handleRequests(int inst_no, int ballot, @NotNull ClientRequest[] requests);
    void handleAckPrepare(GenericPaxosMessage.ackPrepare ackPrepare);
}
