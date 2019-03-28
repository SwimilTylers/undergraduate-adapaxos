package network.service.module;

import network.message.protocols.GenericConnectionMessage;

/**
 * @author : Swimiltylers
 * @version : 2019/2/26 17:03
 */
public interface ConnectionModule {
    GenericConnectionMessage.Beacon makeBeacon(long ts);
    GenericConnectionMessage.ackBeacon makeAck(long recvTs, GenericConnectionMessage.Beacon beacon);

    void update(int fromId, long timestamp);
    void updateRound(long recvTs, GenericConnectionMessage.ackBeacon ackBeacon);

    void init(int toId);

    int[] filter(long threshold);
    int filterCount(long threshold);
}
