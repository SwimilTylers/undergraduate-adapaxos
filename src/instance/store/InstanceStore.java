package instance.store;

import instance.AdaPaxosInstance;
import instance.PaxosInstance;

/**
 * @author : Swimiltylers
 * @version : 2019/2/18 12:58
 */
public interface InstanceStore {
    boolean isExist(int access_id, int inst_id);
    boolean store(int access_id, int inst_id, PaxosInstance instance);
    PaxosInstance fetch(int access_id, int inst_id);
}
