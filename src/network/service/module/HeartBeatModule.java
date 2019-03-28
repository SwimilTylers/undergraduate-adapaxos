package network.service.module;

import com.sun.istack.internal.NotNull;
import network.message.protocols.GenericConnectionMessage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * @author : Swimiltylers
 * @version : 2019/2/26 17:19
 */
public class HeartBeatModule implements ConnectionModule{
    private static class NodeMonitor{
        private boolean isConn = true;
        private long lst_srv = 0;

        private long lst_rndSrv = 0;

        private long lst_rndInit = 0;
        private long lst_rndRecv = 0;
        private long rnd = Long.MAX_VALUE;

        @Override
        public String toString() {
            return isConn + " | " + lst_srv + " | " + lst_rndSrv + " | " + "<" + lst_rndInit + ", " + lst_rndRecv + ">" + " | " + (rnd == Long.MAX_VALUE ? "INF" : rnd) + "\n";
        }
    }

    private int moduleId;
    private AtomicReferenceArray<NodeMonitor> nodes;

    public HeartBeatModule(int moduleId, int totalSize) {
        this.moduleId = moduleId;
        nodes = new AtomicReferenceArray<>(totalSize);
        nodes.set(moduleId, new NodeMonitor());
    }

    @Override
    public GenericConnectionMessage.Beacon makeBeacon(long ts) {
        return new GenericConnectionMessage.Beacon(moduleId, ts);
    }

    @Override
    public GenericConnectionMessage.ackBeacon makeAck(long recvTs, @NotNull GenericConnectionMessage.Beacon beacon) {
        return new GenericConnectionMessage.ackBeacon(moduleId, recvTs, beacon);
    }

    @Override
    public void update(int fromId, long timestamp) {
        nodes.updateAndGet(fromId, nodeMonitor -> {
            if (nodeMonitor != null)
                nodeMonitor.lst_srv = Long.max(nodeMonitor.lst_srv, timestamp);

            return nodeMonitor;
        });

    }

    @Override
    public void updateRound(long recvTs, GenericConnectionMessage.ackBeacon ackBeacon) {
        nodes.updateAndGet(ackBeacon.fromId, nodeMonitor -> {
            if (nodeMonitor != null) {
                nodeMonitor.lst_srv = Long.max(nodeMonitor.lst_srv, ackBeacon.timestamp);

                if (ackBeacon.timestamp > nodeMonitor.lst_rndSrv) {
                    nodeMonitor.lst_rndSrv = ackBeacon.timestamp;

                    nodeMonitor.lst_rndInit = ackBeacon.beacon.timestamp;
                    nodeMonitor.lst_rndRecv = recvTs;
                    nodeMonitor.rnd = nodeMonitor.lst_rndRecv - nodeMonitor.lst_rndInit;
                }
            }

            return nodeMonitor;
        });
    }

    @Override
    public void init(int toId) {
        nodes.set(toId, new NodeMonitor());
    }

    @Override
    public int[] filter(long threshold) {
        List<Integer> lost = new ArrayList<>();
        long crtTime = System.currentTimeMillis();

        for (int i = 0; i < nodes.length(); i++) {
            if (i != moduleId) {
                int id = i;
                nodes.updateAndGet(i, nodeMonitor -> {
                    nodeMonitor.isConn = crtTime - nodeMonitor.lst_srv <= threshold;
                    if (!nodeMonitor.isConn)
                        lost.add(id);
                    return nodeMonitor;
                });
            }
        }

        if (lost.isEmpty())
            return null;
        else{
            int[] ret = new int[lost.size()];
            for (int i = 0; i < lost.size(); i++) {
                ret[i] = lost.get(i);
            }
            return ret;
        }
    }

    @Override
    public int filterCount(long threshold) {
        AtomicInteger count = new AtomicInteger(0);
        long crtTime = System.currentTimeMillis();

        for (int i = 0; i < nodes.length(); i++) {
            if (i != moduleId) {
                int id = i;
                nodes.updateAndGet(id, nodeMonitor -> {
                    if (nodeMonitor != null){
                        nodeMonitor.isConn = crtTime - nodeMonitor.lst_srv <= threshold;
                        if (!nodeMonitor.isConn)
                            count.incrementAndGet();
                    }
                    return nodeMonitor;
                });
            }
        }

        return count.get();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("[").append(System.currentTimeMillis()).append("][").append("[module:").append(moduleId).append("]\n").append("\t id | isConn | lst_srv | lst_rndSrv | lst_rnd <INIT, ARR> | rnd \n");

        for (int i = 0; i < nodes.length(); i++) {
            int id = i;
            nodes.updateAndGet(id, nodeMonitor -> {
                if (nodeMonitor != null)
                    builder.append("\t ").append(id).append(" | ").append(nodeMonitor.toString());
                return nodeMonitor;
            });
        }

        return builder.toString();
    }
}
