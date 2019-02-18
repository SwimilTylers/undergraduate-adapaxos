package instance.maintenance;

import java.io.Serializable;

/**
 * @author : Swimiltylers
 * @version : 2019/2/14 18:41
 */
public class LeaderMaintenance implements Serializable {
    private static final long serialVersionUID = -6883780105004316420L;
    public HistoryMaintenance historyMaintenanceUnit = null;
    public int prepareResponse = 0;
    public int acceptResponse = 0;
}
