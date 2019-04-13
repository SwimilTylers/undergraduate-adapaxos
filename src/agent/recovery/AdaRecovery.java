package agent.recovery;

import agent.learner.CommitUpdater;
import agent.learner.DiskCommitResponder;
import instance.AdaPaxosInstance;
import instance.maintenance.AdaRecoveryMaintenance;
import instance.store.RemoteInstanceStore;
import logger.PaxosLogger;
import network.message.protocols.DiskPaxosMessage;
import network.message.protocols.GenericPaxosMessage;
import network.service.sender.PeerMessageSender;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * @author : Swimiltylers
 * @version : 2019/4/13 20:49
 */
public class AdaRecovery implements DiskCommitResponder {
    private final int serverId;
    private final int peerSize;

    private final PeerMessageSender sender;
    private final RemoteInstanceStore remoteStore;

    private final AtomicReferenceArray<AdaPaxosInstance> instanceSpace;
    private final AtomicReferenceArray<AdaRecoveryMaintenance> recoveryList;;

    private final AtomicBoolean forceFsync;

    private final PaxosLogger logger;

    public AdaRecovery(int serverId, int peerSize,
                       PeerMessageSender sender,
                       RemoteInstanceStore remoteStore,
                       AtomicReferenceArray<AdaPaxosInstance> instanceSpace,
                       AtomicReferenceArray<AdaRecoveryMaintenance> recoveryList,
                       AtomicBoolean forceFsync, PaxosLogger logger) {
        this.serverId = serverId;
        this.peerSize = peerSize;
        this.sender = sender;
        this.remoteStore = remoteStore;
        this.instanceSpace = instanceSpace;
        this.recoveryList = recoveryList;
        this.forceFsync = forceFsync;
        this.logger = logger;
    }

    public void handleSync(GenericPaxosMessage.Sync sync, CommitUpdater updater){

    }

    @Override
    public boolean isValidMessage(int inst_no, long token) {
        return false;
    }

    @Override
    public boolean respond_ackWrite(DiskPaxosMessage.ackWrite ackWrite, CommitUpdater updater) {
        return false;
    }

    @Override
    public boolean respond_ackRead(DiskPaxosMessage.ackRead ackRead, CommitUpdater updater) {
        return false;
    }
}
