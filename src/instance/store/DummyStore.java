package instance.store;

import instance.PaxosInstance;
import instance.StaticPaxosInstance;

/**
 * @author : Swimiltylers
 * @version : 2019/2/18 15:05
 */
public class DummyStore implements InstanceStore{
    @Override
    public boolean isExist(int access_id, int inst_id) {
        return false;
    }

    @Override
    public boolean store(int access_id, int inst_id, PaxosInstance instance) {
        return true;
    }

    @Override
    public PaxosInstance fetch(int access_id, int inst_id) {
        return null;
    }
}
