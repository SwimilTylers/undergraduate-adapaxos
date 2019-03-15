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

    public static void broadcastOnDisks(long token, int inst_no, AdaPaxosInstance inst, int serverId, int peerSize, RemoteInstanceStore remoteStore){
        for (int disk_no = 0; disk_no < peerSize; disk_no++) {
            if (disk_no != serverId){
                remoteStore.launchRemoteStore(token, disk_no, serverId, inst_no, inst);
                for (int other_writer = 0; other_writer < peerSize; other_writer++) {
                    if (other_writer != serverId)
                        remoteStore.launchRemoteFetch(token, disk_no, other_writer, inst_no);
                }
            }
        }
    }
}
