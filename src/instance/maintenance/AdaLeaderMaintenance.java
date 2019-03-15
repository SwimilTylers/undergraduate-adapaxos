package instance.maintenance;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @author : Swimiltylers
 * @version : 2019/3/15 12:39
 */
public class AdaLeaderMaintenance {
    public int response = 1;
    public long token;

    public final boolean[] ignore;
    public final boolean[] writeSign;
    public final int[] readCount;

    public AdaLeaderMaintenance(long token, int id, int peerSize) {
        this.token = token;

        ignore = new boolean[peerSize];
        writeSign = new boolean[peerSize];
        readCount = new int[peerSize];

        Arrays.fill(ignore, false);
        ignore[id] = true;

        Arrays.fill(writeSign, false);
        Arrays.fill(readCount, 0);
    }

    public void refresh(long token, int id){
        this.token = token;
        this.response = 1;

        Arrays.fill(ignore, false);
        ignore[id] = true;

        Arrays.fill(writeSign, false);
        Arrays.fill(readCount, 0);
    }
}
