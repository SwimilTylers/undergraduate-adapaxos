package utils;

import java.io.Serializable;

/**
 * @author : Swimiltylers
 * @version : 2019/4/16 22:08
 */
public class NetworkConfiguration implements Serializable {
    private static final long serialVersionUID = 2285355545467176038L;
    public final int peerSize;

    public final String[] peerAddr;
    public final int[] peerPort;
    public final int[] externalPort;

    public final int initLeaderId;

    public NetworkConfiguration(int peerSize, String[] peerAddr, int[] peerPort, int[] externalPort, int initLeaderId) {
        this.peerSize = peerSize;
        this.peerAddr = peerAddr;
        this.peerPort = peerPort;
        this.externalPort = externalPort;
        this.initLeaderId = initLeaderId;
    }
}
