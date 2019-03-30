package network.message.protocols;

import java.io.Serializable;

/**
 * @author : Swimiltylers
 * @version : 2019/3/8 13:46
 */
public class AdaPaxosMessage implements Serializable {
    private static final long serialVersionUID = 948019606028744210L;
    public final boolean fsync;

    public AdaPaxosMessage(boolean fsync) {
        this.fsync = fsync;
    }

    @Override
    public String toString() {
        return "[ADA_PAXOS][fsync="+fsync+"]";
    }
}
