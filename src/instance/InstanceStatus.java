package instance;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author : Swimiltylers
 * @version : 2019/2/14 18:35
 */
public enum InstanceStatus implements Serializable {
    PREPARING, PREPARED, ACCEPTED, COMMITTED;

    private static Map<InstanceStatus, Set<InstanceStatus>> earlier;

    static {
        earlier = new HashMap<>();

        Set<InstanceStatus> set = new HashSet<>();
        earlier.put(PREPARING, set);

        set = new HashSet<>();
        set.add(PREPARING);
        earlier.put(PREPARED, set);

        set = new HashSet<>();
        set.add(PREPARING);
        earlier.put(ACCEPTED, set);

        set = new HashSet<>();
        set.add(PREPARING);
        set.add(PREPARED);
        set.add(ACCEPTED);
        earlier.put(COMMITTED, set);
    }

    public static boolean earlierThan (InstanceStatus a, InstanceStatus b){
        return earlier.get(a).contains(b);
    }
}
