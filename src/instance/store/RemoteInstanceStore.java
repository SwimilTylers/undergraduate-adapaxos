package instance.store;

import instance.PaxosInstance;

/**
 * @author : Swimiltylers
 * @version : 2019/3/14 18:19
 */
public interface RemoteInstanceStore {
    void connect();
    void launchRemoteStore(long token, int disk_no, int access_id, int inst_id, PaxosInstance instance);
    void launchRemoteFetch(long token, int disk_no, int access_id, int inst_id);
}
