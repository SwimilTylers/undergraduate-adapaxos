package instance.maintenance;

import instance.AdaPaxosInstance;

/**
 * @author : Swimiltylers
 * @version : 2019/4/2 15:39
 */
public class AdaRecoveryMaintenance {
    public final long token;
    public AdaPaxosInstance potential = null;
    public int readCount = 1;

    public AdaRecoveryMaintenance(long token) {
        this.token = token;
    }
}
