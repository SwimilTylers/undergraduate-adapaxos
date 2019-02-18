package instance.maintenance;

import client.ClientRequest;
import instance.PaxosInstance;
import javafx.util.Pair;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author : Swimiltylers
 * @version : 2019/2/15 14:24
 */
public class DiskLeaderMaintenance extends LeaderMaintenance{
    private static final long serialVersionUID = 7505325295603950176L;
    public int totalDisk;
    public Map<Integer, Integer> dialogueRecord = new HashMap<>();      // <disk_no, read_count>
    public long crtDialogue;

    public DiskLeaderMaintenance(int totalDisk, long initDialogue) {
        this.totalDisk = totalDisk;
        this.crtDialogue = initDialogue;
    }
}