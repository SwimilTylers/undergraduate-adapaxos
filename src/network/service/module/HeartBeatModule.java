package network.service.module;

import com.sun.istack.internal.NotNull;
import network.message.protocols.GenericConnectionMessage;

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

        private boolean isConnected(){
            return isConn;
        }

        private void updateBeacon(long sndTs){
            synchronized (this) {lst_srv = Long.max(lst_srv, sndTs);}
        }

        private void updateRound(long initTs, long sndTs, long arrTs){
            synchronized (this) {
                lst_srv = Long.max(lst_srv, sndTs);

                if (sndTs > lst_rndSrv) {
                    lst_rndSrv = sndTs;

                    lst_rndInit = initTs;
                    lst_rndRecv = arrTs;
                    rnd = lst_rndRecv - lst_rndInit;
                }
            }
        }

        @Override
        public String toString() {
            synchronized (this) {
                return isConn + " | " +
                        lst_srv + " | " +
                        lst_rndSrv + " | " +
                        "<" + lst_rndInit + ", " + lst_rndRecv + ">" + " | " +
                        (rnd == Long.MAX_VALUE ? "INF" : rnd) + "\n";
            }
        }
    }

    private int moduleId;
    private NodeMonitor[] nodes;

    public HeartBeatModule(int moduleId, int totalSize) {
        this.moduleId = moduleId;
        nodes = new NodeMonitor[totalSize];
        nodes[moduleId] = new NodeMonitor();
    }

    @Override
    public GenericConnectionMessage.Beacon makeBeacon(long ts) {
        return new GenericConnectionMessage.Beacon(moduleId, ts);
    }

    @Override
    public GenericConnectionMessage.ackBeacon ack(long recvTs, @NotNull GenericConnectionMessage.Beacon beacon) {
        return new GenericConnectionMessage.ackBeacon(moduleId, recvTs, beacon);
    }

    @Override
    public void updateByBeacon(long recvTs, GenericConnectionMessage.Beacon beacon) {
        if (nodes[beacon.fromId] != null){
            nodes[beacon.fromId].updateBeacon(beacon.timestamp);
        }
    }

    @Override
    public void updateByAckBeacon(long recvTs, GenericConnectionMessage.ackBeacon ackBeacon) {
        if (nodes[ackBeacon.fromId] != null){
            nodes[ackBeacon.fromId].updateRound(ackBeacon.beacon.timestamp, ackBeacon.timestamp, recvTs);
        }
    }

    @Override
    public boolean connected(int toId) {
        if (nodes[toId] != null)
            return nodes[toId].isConnected();
        else
            return false;
    }

    @Override
    public void init(int toId) {
        nodes[toId] = new NodeMonitor();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("[module:").append(moduleId).append("]\n").append("\t id | isConn | lst_srv | lst_rndSrv | lst_rnd <INIT, ARR> | rnd \n");
        for (NodeMonitor node :
                nodes) {
            if (node != null) {
                builder.append("\t ").append(moduleId).append(" | ").append(node);
            }
        }
        return builder.toString();
    }
}
