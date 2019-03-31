package instance.store;

import instance.InstanceStatus;
import instance.PaxosInstance;
import javafx.util.Pair;

import java.util.HashMap;
import java.util.Map;

/**
 * @author : Swimiltylers
 * @version : 2019/3/24 20:05
 */
public class TaggedOffsetIndexStore extends OffsetIndexStore{
    private Map<Pair<Integer, Integer>, Pair<Integer, InstanceStatus>> tags;

    public TaggedOffsetIndexStore(String store_name) {
        super(store_name);
        tags = new HashMap<>();
    }

    @Override
    public synchronized boolean store(int access_id, int inst_id, PaxosInstance instance) {
        Pair<Integer, Integer> key = new Pair<>(access_id, inst_id);
        int inst_ballot = instance.crtInstBallot;
        if (!tags.containsKey(key)) {
            tags.put(key, new Pair<>(inst_ballot, instance.status));
            return super.store(access_id, inst_id, instance);
        }
        else if (tags.get(key).getKey() <= inst_ballot && !InstanceStatus.earlierThan(tags.get(key).getValue(), instance.status)){
            tags.replace(key, new Pair<>(inst_ballot, instance.status));
            return super.store(access_id, inst_id, instance);
        }
        else
            return false;
    }
}
