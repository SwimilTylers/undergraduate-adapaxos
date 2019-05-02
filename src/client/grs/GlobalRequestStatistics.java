package client.grs;

import client.ClientRequest;
import instance.InstanceStatus;
import javafx.util.Pair;
import network.message.protocols.GenericClientMessage;

import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author : Swimiltylers
 * @version : 2019/4/28 19:53
 */
public class GlobalRequestStatistics {
    static class GRSEntry{
        InstanceStatus status;
        int requestLeader;
        int firstCommitLeader = -1;

        int verified = -99;

        long ts_start;
        long ts_commit;

        GRSEntry(int serverId){
            status = InstanceStatus.PREPARING;
            requestLeader = serverId;
            ts_start = System.currentTimeMillis();
        }

        @Override
        public String toString() {
            return status.toString()+"\t"+requestLeader+"\t"+firstCommitLeader+"\t"+verified+"\t"+ts_start+"\t"+ts_commit+"\t"+(ts_commit == 0 ? "nil" : (ts_commit-ts_start));
        }
    }


    private ConcurrentHashMap<String, GRSEntry> statistics;
    private AtomicBoolean conclude;
    private Random rnd;
    private GRSInstanceAnalytical[] analytics;
    private AtomicInteger lastMGetter;
    private BlockingQueue<String> requests;

    public GlobalRequestStatistics(int peerSize) {
        this.statistics = new ConcurrentHashMap<>();
        this.rnd = new Random();
        this.conclude = new AtomicBoolean(false);
        analytics = new GRSInstanceAnalytical[peerSize];
        lastMGetter = new AtomicInteger();

        requests = new ArrayBlockingQueue<>(32);
    }

    public GRSMessageGetter getMessageGetter(int serverId){
        return () -> {
            if (!conclude.get()) {
                String request = String.format("%x", rnd.nextLong());
                lastMGetter.set(serverId);

                statistics.put(request, new GRSEntry(serverId));
                return new ClientRequest(new GenericClientMessage.Propose(request), "global request statistics");
            }
            else
                return null;
        };
    }

    public GRSMessageReporter getMessageReporter(int serverId){
        return (request, status) -> {
            GRSEntry entry = statistics.get(request);
            entry.status = status;
            if (status == InstanceStatus.COMMITTED) {
                entry.firstCommitLeader = serverId;
                entry.ts_commit = System.currentTimeMillis();
            }
        };
    }

    public String makeConclusion(int timeout) throws InterruptedException {
        conclude.set(true);
        Thread.sleep(timeout);

        List<Pair<String, InstanceStatus>> analyze = analytics[lastMGetter.get()].analyze();
        for (int i = 0; i < analyze.size(); i++) {
            Pair<String, InstanceStatus> pair = analyze.get(i);
            if (pair != null)
                statistics.get(pair.getKey()).verified = (pair.getValue() == InstanceStatus.COMMITTED) ? i : -i;
        }

        StringBuilder builder = new StringBuilder();

        builder.append(String.format("%-16s\t%-9s", "REQUEST", "STATUS")).append("\tRL\tCL\tVD\t").append(String.format("%-13s\t%-13s\t%s", "START_TS", "COMMIT_TS", "ITV")).append("\n\n");
        statistics.forEach((request, grsEntry) -> builder.append(String.format("%-16s\t", request)).append(grsEntry).append("\n"));
        return builder.toString();
    }

    public void releaseRequests(){
        String request = String.format("%x", rnd.nextLong());
    }

    public void setAnalytic(int serverId, GRSInstanceAnalytical analytic){
        analytics[serverId] = analytic;
    }
}
