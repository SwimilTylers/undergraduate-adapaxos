package client.grs;

import client.ClientRequest;
import instance.InstanceStatus;
import network.message.protocols.GenericClientMessage;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author : Swimiltylers
 * @version : 2019/4/28 19:53
 */
public class GlobalRequestStatistics {
    static class GRSEntry{
        InstanceStatus status;
        int requestLeader;
        int firstCommitLeader;

        int verified;

        GRSEntry(int serverId){
            status = InstanceStatus.PREPARING;
            requestLeader = serverId;
        }
    }


    private ConcurrentHashMap<String, GRSEntry> statistics;
    private Random rnd;

    public GlobalRequestStatistics() {
        this.statistics = new ConcurrentHashMap<>();
        this.rnd = new Random();
    }

    public GRSMessageGetter getMessageGetter(int serverId){
        return () -> {
            String request = String.format("%x", rnd.nextLong());
            statistics.put(request, new GRSEntry(serverId));
            return new ClientRequest(new GenericClientMessage.Propose(request), "global request statistics");
        };
    }

    public GRSMessageReporter getMessageReporter(int serverId){
        return (request, status) -> {
            GRSEntry entry = statistics.get(request);
            entry.status = status;
            if (status == InstanceStatus.COMMITTED)
                entry.firstCommitLeader = serverId;
        };
    }
}
