package instance.store;

import instance.PaxosInstance;
import instance.StaticPaxosInstance;
import logger.PaxosLogger;
import network.message.protocols.DiskPaxosMessage;

import java.util.concurrent.BlockingQueue;

/**
 * @author : Swimiltylers
 * @version : 2019/3/14 18:19
 */
public interface RemoteInstanceStore {
    void connect(BlockingQueue<DiskPaxosMessage> dMessages);
    void launchRemoteStore(long token, int disk_no, int access_id, int inst_id, PaxosInstance instance);
    void launchRemoteFetch(long token, int disk_no, int access_id, int inst_id);
    void setLogger(PaxosLogger logger);
}
