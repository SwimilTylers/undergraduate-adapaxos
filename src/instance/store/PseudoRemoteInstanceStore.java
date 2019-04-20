package instance.store;

import instance.PaxosInstance;
import logger.PaxosLogger;
import network.message.protocols.DiskPaxosMessage;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author : Swimiltylers
 * @version : 2019/3/24 20:36
 */
public class PseudoRemoteInstanceStore implements RemoteInstanceStore{
    private BlockingQueue<DiskPaxosMessage> dMessages;
    private InstanceStore[] stores;
    private final int thisId;
    private BlockingQueue<InstanceStoreRequest> launchService;
    private AtomicBoolean onRunning;

    private PaxosLogger logger;

    private static class InstanceStoreRequest {
        private final boolean isWrite;
        private final long token;
        private final int disk_no;
        private final int access_id;
        private final int inst_id;
        private final PaxosInstance instance;

        private InstanceStoreRequest(boolean isWrite, long token, int disk_no, int access_id, int inst_id, PaxosInstance instance) {
            this.isWrite = isWrite;
            this.token = token;
            this.disk_no = disk_no;
            this.access_id = access_id;
            this.inst_id = inst_id;
            this.instance = instance;
        }

        private static InstanceStoreRequest write(long token, int disk_no, int access_id, int inst_id, PaxosInstance instance) {
            return new InstanceStoreRequest(true, token, disk_no, access_id, inst_id, instance);
        }

        private static InstanceStoreRequest read(long token, int disk_no, int access_id, int inst_id){
            return new InstanceStoreRequest(false, token, disk_no, access_id, inst_id, null);
        }

        @Override
        public String toString() {
            return "[" + (isWrite ? "STORE" : "FETCH") + "][dno="+disk_no+",aid="+access_id+",ino="+inst_id+"][tkn="+token+"][inst="+(isWrite ? instance.toString() : "")+"]";
        }
    }

    public PseudoRemoteInstanceStore(int thisId, InstanceStore[] stores, int maxProcessingSize) {
        this.stores = stores;
        this.thisId = thisId;
        this.launchService = new ArrayBlockingQueue<>(maxProcessingSize);

        onRunning = new AtomicBoolean(true);
    }

    @Override
    protected void finalize() throws Throwable {
        onRunning.set(false);
        super.finalize();
    }

    @Override
    public void connect(BlockingQueue<DiskPaxosMessage> dMessages) {
        this.dMessages = dMessages;
        new Thread(() -> {
            while (onRunning.get()){
                try {
                    InstanceStoreRequest request = launchService.take();
                    processing(request);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    private void processing(InstanceStoreRequest request){
        if (request.isWrite){
            try {
                if (stores[request.disk_no].store(request.access_id, request.inst_id, request.instance)) {
                    dMessages.put(new DiskPaxosMessage.ackWrite(
                            request.inst_id, thisId,
                            request.instance.crtInstBallot,
                            request.token, request.disk_no,
                            DiskPaxosMessage.DiskStatus.WRITE_SUCCESS
                    ));
                }
                else {
                    dMessages.put(new DiskPaxosMessage.ackWrite(
                            request.inst_id, thisId,
                            request.instance.crtInstBallot,
                            request.token, request.disk_no,
                            DiskPaxosMessage.DiskStatus.WRITE_FAILED
                    ));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        else {
            try {
                if (stores[request.disk_no].isExist(request.access_id, request.inst_id)) {
                    PaxosInstance instance = stores[request.disk_no].fetch(request.access_id, request.inst_id);
                    dMessages.put(new DiskPaxosMessage.ackRead(
                            request.inst_id, thisId,
                            instance.crtInstBallot,
                            request.token, request.disk_no, request.access_id,
                            DiskPaxosMessage.DiskStatus.READ_SUCCESS,
                            instance
                    ));
                }
                else {
                    dMessages.put(new DiskPaxosMessage.ackRead(
                            request.inst_id, thisId,
                            0,
                            request.token, request.disk_no, request.access_id,
                            DiskPaxosMessage.DiskStatus.READ_NO_SUCH_FILE,
                            null
                    ));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public int getDiskSize() {
        return stores.length;
    }

    @Override
    public void launchRemoteStore(long token, int disk_no, int access_id, int inst_id, PaxosInstance instance) {
        logger.logFormatted(false, "remote store", "ino="+inst_id+",tkn="+token+",aid="+access_id, "inst="+(instance==null?"null":instance.toString()));
        try {
            launchService.put(InstanceStoreRequest.write(token, disk_no, access_id, inst_id, instance));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void launchRemoteFetch(long token, int disk_no, int access_id, int inst_id) {
        logger.logFormatted(false, String.valueOf(System.currentTimeMillis()), "remote fetch", "ino="+inst_id+",tkn="+token+",aid="+access_id);
        try {
            launchService.put(InstanceStoreRequest.read(token, disk_no, access_id, inst_id));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void setLogger(PaxosLogger logger) {
        this.logger = logger;
    }
}
