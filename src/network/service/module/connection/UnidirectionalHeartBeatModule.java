package network.service.module.connection;

import network.message.protocols.GenericConnectionMessage;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * @author : Swimiltylers
 * @version : 2019/4/13 15:43
 */
public class UnidirectionalHeartBeatModule implements ConnectionModule{
    private int moduleId;
    private int totalSize;
    private AtomicLongArray nodes;

    public UnidirectionalHeartBeatModule(int moduleId, int totalSize) {
        this.moduleId = moduleId;
        this.totalSize = totalSize;
        nodes = new AtomicLongArray(totalSize);
        nodes.set(moduleId, 0);
    }

    @Override
    public GenericConnectionMessage.Beacon makeBeacon(long ts) {
        return new GenericConnectionMessage.Beacon(moduleId, ts);
    }

    @Override
    public GenericConnectionMessage.ackBeacon makeAck(long recvTs, GenericConnectionMessage.Beacon beacon) {
        return null;
    }

    @Override
    public void update(int fromId, long timestamp) {
        nodes.updateAndGet(fromId, l -> Long.max(l, timestamp));
    }

    @Override
    public void updateRound(long recvTs, GenericConnectionMessage.ackBeacon ackBeacon) {
        nodes.updateAndGet(ackBeacon.fromId, l -> Long.max(l, ackBeacon.timestamp));
    }

    @Override
    public void init(int toId) {
        nodes.set(toId, 0);
    }

    @Override
    public int[] filter(long threshold) {
        int[] ret = new int[totalSize];
        int ptr = 0;

        long crt = System.currentTimeMillis();

        for (int i = 0; i < totalSize; i++) {
            if (i != moduleId){
                long ts = nodes.get(i);
                if (crt - ts > threshold)
                    ret[ptr++] = i;
            }
        }

        if (ptr == 0)
            return null;
        else
            return Arrays.copyOfRange(ret, 0, ptr);
    }

    @Override
    public int filterCount(long threshold) {
        return 0;
    }

    @Override
    public boolean survive(int specific, long threshold) {
        if (specific == moduleId)
            return true;
        else {
            long crt = System.currentTimeMillis();

            return crt - nodes.get(specific) > threshold;
        }
    }

    @Override
    public String toString() {
        long crt = System.currentTimeMillis();
        StringBuilder builder = new StringBuilder();
        builder.append("nodes: at ").append(crt).append("\n");
        for (int i = 0; i < totalSize; i++) {
            if (i != moduleId) {
                long lst = nodes.get(i);
                builder.append("[node ").append(i).append("][lst=").append(lst).append("][diff=").append(crt - lst).append("]\n");
            }
        }
        builder.append("\n");
        return builder.toString();
    }
}
