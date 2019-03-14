package instance.store;

import instance.PaxosInstance;

/**
 * @author : Swimiltylers
 * @version : 2019/3/14 18:19
 */
public interface RemoteInstanceStore {
    void connect();
    void launchRemoteStore(int disk_no, int access_id, int inst_id, PaxosInstance instance);
    void launchRemoteFetch(int disk_no, int access_id, int inst_id);
}
