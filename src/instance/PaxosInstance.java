package instance;

import client.ClientRequest;
import instance.maintenance.LeaderMaintenance;

/**
 * @author : Swimiltylers
 * @version : 2019/2/14 18:37
 */
public class PaxosInstance {
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
