package instance.maintenance;

import instance.AdaPaxosInstance;

import java.util.Arrays;

/**
 * @author : Swimiltylers
 * @version : 2019/4/2 15:39
 */
public class AdaRecoveryMaintenance {
    public final long token;
    public boolean recovered;
    public AdaPaxosInstance potential = null;

    public int peerCount = 0;
    public int diskCount = 0;
    public int[] readCount;

    public AdaRecoveryMaintenance(long token, int disk_size) {
        this.token = token;
        readCount = new int[disk_size];
        Arrays.fill(readCount, 0);
        recovered = false;
    }

    public AdaRecoveryMaintenance (long token) {
        this.token = token;
        readCount = null;
        recovered = false;
    }

    public static AdaRecoveryMaintenance copyOf(AdaRecoveryMaintenance old){
        AdaRecoveryMaintenance ret = new AdaRecoveryMaintenance(old.token);
        ret.potential = old.potential;

        ret.diskCount = old.diskCount;
        ret.recovered = old.recovered;
        if (old.readCount != null)
            ret.readCount = Arrays.copyOfRange(old.readCount, 0, old.readCount.length);

        return ret;
    }
}
