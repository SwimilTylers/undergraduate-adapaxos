package instance.store;

import instance.PaxosInstance;
import network.message.protocols.DiskPaxosMessage;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author : Swimiltylers
 * @version : 2019/3/24 20:36
 */
public class PseudoRemoteInstanceStore implements RemoteInstanceStore{
    private BlockingQueue<DiskPaxosMessage> dMessages;
    private InstanceStore[] stores;
    private final int thisId;
    private ExecutorService launchService;

    public PseudoRemoteInstanceStore(int thisId, InstanceStore[] stores) {
        this.stores = stores;
        this.thisId = thisId;
        this.launchService = Executors.newCachedThreadPool();
    }

    @Override
    protected void finalize() throws Throwable {
        if (launchService != null)
            launchService.shutdown();
    }

    @Override
    public void connect(BlockingQueue<DiskPaxosMessage> dMessages) {
        this.dMessages = dMessages;
    }

    @Override
    public void launchRemoteStore(long token, int disk_no, int access_id, int inst_id, PaxosInstance instance) {
        launchService.execute(() -> {
            try {
                if (stores[disk_no].store(access_id, inst_id, instance)) {
                    dMessages.put(new DiskPaxosMessage.ackWrite(
                            inst_id, thisId,
                            instance.crtInstBallot,
                            token, disk_no,
                            DiskPaxosMessage.DiskStatus.WRITE_SUCCESS
                    ));
                }
                else {
                    dMessages.put(new DiskPaxosMessage.ackWrite(
                            inst_id, thisId,
                            instance.crtInstBallot,
                            token, disk_no,
                            DiskPaxosMessage.DiskStatus.WRITE_FAILED
                    ));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void launchRemoteFetch(long token, int disk_no, int access_id, int inst_id) {
        launchService.execute(() -> {
            try {
                if (stores[disk_no].isExist(access_id, inst_id)) {
                    PaxosInstance instance = stores[disk_no].fetch(access_id, inst_id);
                    dMessages.put(new DiskPaxosMessage.ackRead(
                            inst_id, thisId,
                            instance.crtInstBallot,
                            token, disk_no, access_id,
                            DiskPaxosMessage.DiskStatus.READ_SUCCESS,
                            instance
                    ));
                }
                else {
                    dMessages.put(new DiskPaxosMessage.ackRead(
                            inst_id, thisId,
                            0,
                            token, disk_no, access_id,
                            DiskPaxosMessage.DiskStatus.READ_NO_SUCH_FILE,
                            null
                    ));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
}
