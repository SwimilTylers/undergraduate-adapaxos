package network.message.protocols;

import java.io.Serializable;

/**
 * @author : Swimiltylers
 * @version : 2019/1/27 14:39
 */
public class GenericConnectionMessage implements Serializable {
    private static final long serialVersionUID = 7628147712644967198L;

    public static class Beacon extends GenericConnectionMessage{
        private static final long serialVersionUID = -5082149272025151213L;
        public final int fromId;
        public final long timestamp;

        public Beacon(int fromId, long timestamp) {
            this.fromId = fromId;
            this.timestamp = timestamp;
        }
    }

    public static class ackBeacon extends GenericConnectionMessage{
        private static final long serialVersionUID = 2173408396068955774L;
        public final int fromId;
        public final long timestamp;
        public final Beacon beacon;

        public ackBeacon(int fromId, long timestamp, Beacon beacon) {
            this.fromId = fromId;
            this.timestamp = timestamp;
            this.beacon = beacon;
        }
    }
}
