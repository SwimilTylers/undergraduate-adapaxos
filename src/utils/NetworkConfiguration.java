package utils;

import java.io.Serializable;

/**
 * @author : Swimiltylers
 * @version : 2019/4/16 22:08
 */
public class NetworkConfiguration implements Serializable {
    private static final long serialVersionUID = 2285355545467176038L;
    public final String[] peerAddr;
    public final int[] peerPort;

    public final int initLeaderId;

    public NetworkConfiguration(String[] peerAddr, int[] peerPort, int initLeaderId) {
        this.peerAddr = peerAddr;
        this.peerPort = peerPort;
        this.initLeaderId = initLeaderId;
    }
}
