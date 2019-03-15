package instance;

import client.ClientRequest;
import instance.maintenance.LeaderMaintenance;

/**
 * @author : Swimiltylers
 * @version : 2019/2/14 18:37
 */
public class StaticPaxosInstance extends PaxosInstance {
    private static final long serialVersionUID = -966463150573981018L;
    public LeaderMaintenance leaderMaintenanceUnit;

    public StaticPaxosInstance copyOf() {
        StaticPaxosInstance ret = new StaticPaxosInstance();

        ret.crtLeaderId = crtLeaderId;
        ret.status = status;
        ret.crtInstBallot = crtInstBallot;
        ret.requests = requests;
        ret.leaderMaintenanceUnit = leaderMaintenanceUnit;

        return ret;
    }
}
