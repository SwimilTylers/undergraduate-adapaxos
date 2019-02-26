package network.service.module;

import network.message.protocols.GenericConnectionMessage;

/**
 * @author : Swimiltylers
 * @version : 2019/2/26 17:03
 */
public interface ConnectionModule {
    GenericConnectionMessage.Beacon makeBeacon(long ts);
    GenericConnectionMessage.ackBeacon ack(long recvTs, GenericConnectionMessage.Beacon beacon);

    void updateByBeacon(long recvTs, GenericConnectionMessage.Beacon beacon);
    void updateByAckBeacon(long recvTs, GenericConnectionMessage.ackBeacon ackBeacon);

    boolean connected(int toId);
    void init(int toId);
}
