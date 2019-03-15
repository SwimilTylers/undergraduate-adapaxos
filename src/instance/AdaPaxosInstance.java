package instance;

import client.ClientRequest;
import instance.maintenance.AdaLeaderMaintenance;
import instance.maintenance.HistoryMaintenance;
import instance.maintenance.LeaderMaintenance;

import java.io.Serializable;

/**
 * @author : Swimiltylers
 * @version : 2019/3/15 12:37
 */
public class AdaPaxosInstance implements Serializable {
    private static final long serialVersionUID = 3496369406585981372L;

    public int crtLeaderId;
    public int crtInstBallot;
    public InstanceStatus status;

    public ClientRequest[] requests;

    transient public AdaLeaderMaintenance lmu;
    transient public HistoryMaintenance hmu;

    private AdaPaxosInstance(int leaderId, int ballot, InstanceStatus status, ClientRequest[] requests){
        crtLeaderId = leaderId;
        crtInstBallot = ballot;
        this.status = status;

        this.requests = requests;
    }

    private AdaPaxosInstance(AdaPaxosInstance old){
        crtLeaderId = old.crtLeaderId;
        crtInstBallot = old.crtInstBallot;
        this.status = old.status;

        this.requests = old.requests;

        lmu = old.lmu;
        hmu = old.hmu;
    }

    public static AdaPaxosInstance leaderInst(long token, int serverId, int peerSize, int ballot, InstanceStatus status, ClientRequest[] requests){
        AdaPaxosInstance instance = new AdaPaxosInstance(serverId, ballot, status, requests);
        instance.lmu = new AdaLeaderMaintenance(token, serverId, peerSize);
        instance.hmu = null;
        return instance;
    }

    public static AdaPaxosInstance subInst(int leaderId, int ballot, InstanceStatus status, ClientRequest[] requests){
        AdaPaxosInstance instance = new AdaPaxosInstance(leaderId, ballot, status, requests);
        instance.lmu = null;
        instance.hmu = null;
        return instance;
    }

    public static AdaPaxosInstance copy(AdaPaxosInstance old){
        return new AdaPaxosInstance(old);
    }
}
