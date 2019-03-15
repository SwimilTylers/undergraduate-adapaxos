package agent.proposer;

import client.ClientRequest;
import instance.AdaPaxosInstance;
import instance.InstanceStatus;
import instance.PaxosInstance;
import instance.maintenance.DiskLeaderMaintenance;
import instance.maintenance.LeaderMaintenance;
import instance.store.InstanceStore;
import instance.store.RemoteInstanceStore;
import network.message.protocols.GenericPaxosMessage;
import network.service.sender.PeerMessageSender;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * @author : Swimiltylers
 * @version : 2019/3/15 12:17
 */
public class AdaProposer implements Proposer{
    private final int serverId;
    private final int peerSize;

    private final PeerMessageSender sender;
    private final InstanceStore localStore;
    private final RemoteInstanceStore remoteStore;

    private final AtomicReferenceArray<AdaPaxosInstance> instanceSpace;
    private final Queue<ClientRequest> restoreRequests;

    private final AtomicBoolean forceFsync;

    public AdaProposer(int serverId, int peerSize,
                       PeerMessageSender sender,
                       InstanceStore localStore, RemoteInstanceStore remoteStore,
                       AtomicReferenceArray<AdaPaxosInstance> instanceSpace,
                       Queue<ClientRequest> restoreRequests,
                       AtomicBoolean forceFsync) {
        this.serverId = serverId;
        this.peerSize = peerSize;
        this.sender = sender;
        this.localStore = localStore;
        this.remoteStore = remoteStore;
        this.instanceSpace = instanceSpace;
        this.restoreRequests = restoreRequests;
        this.forceFsync = forceFsync;
    }

    public static long newToken(){
        return System.currentTimeMillis();
    }

    @Override
    public void handleRequests(int inst_no, int ballot, ClientRequest[] requests) {
        long token = newToken();
        AdaPaxosInstance inst = AdaPaxosInstance.leaderInst(token, serverId, peerSize, ballot, InstanceStatus.PREPARING, requests);
        instanceSpace.set(inst_no, inst);

        if (!forceFsync.get()) {
            sender.broadcastPeerMessage(new GenericPaxosMessage.Prepare(inst_no, inst.crtLeaderId, inst.crtInstBallot));
        }
        else {
            for (int disk_no = 0; disk_no < peerSize; disk_no++) {
                if (disk_no == serverId){
                    localStore.store(serverId, inst_no, inst);
                }
                else{
                    remoteStore.launchRemoteStore(token, disk_no, serverId, inst_no, inst);
                    for (int other_writer = 0; other_writer < peerSize; other_writer++) {
                        if (other_writer != serverId)
                            remoteStore.launchRemoteFetch(token, disk_no, other_writer, inst_no);
                    }
                }
            }
        }
    }

    @Override
    public void handleAckPrepare(GenericPaxosMessage.ackPrepare ackPrepare) {

    }
}
