package instance;

import client.ClientRequest;

import java.io.Serializable;

/**
 * @author : Swimiltylers
 * @version : 2019/3/15 13:51
 */
abstract public class PaxosInstance implements Serializable {
    private static final long serialVersionUID = 5675852448786423374L;

    public int crtLeaderId;
    public int crtInstBallot;
    public InstanceStatus status;

    public ClientRequest[] requests;
}
