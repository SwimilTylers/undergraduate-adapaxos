package utils;

import instance.AdaPaxosInstance;
import instance.store.RemoteInstanceStore;

/**
 * @author : Swimiltylers
 * @version : 2019/3/15 18:07
 */
public class AdaAgents {
    public static long newToken(){
        return System.currentTimeMillis();
    }

    public static void broadcastOnDisks(long token, int inst_no, AdaPaxosInstance inst, int diskSize, RemoteInstanceStore remoteStore){
        for (int disk_no = 0; disk_no < diskSize; disk_no++) {
            remoteStore.launchRemoteStore(token, disk_no, inst.crtLeaderId, inst_no, inst);
        }
    }
}
