package instance;

import client.ClientRequest;
import instance.maintenance.LeaderMaintenance;

import java.io.Serializable;

/**
 * @author : Swimiltylers
 * @version : 2019/2/14 18:37
 */
public class PaxosInstance implements Serializable {
    private static final long serialVersionUID = -966463150573981018L;
    public int crtLeaderId;
    public InstanceStatus status;
    public int crtInstBallot;
    public ClientRequest[] cmds;
    public LeaderMaintenance leaderMaintenanceUnit;

    public PaxosInstance copyOf() {
        PaxosInstance ret = new PaxosInstance();

        ret.crtLeaderId = crtLeaderId;
        ret.status = status;
        ret.crtInstBallot = crtInstBallot;
        ret.cmds = cmds;
        ret.leaderMaintenanceUnit = leaderMaintenanceUnit;

        return ret;
    }
}
