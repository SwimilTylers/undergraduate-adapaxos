package network.service.module.controller;

import javafx.util.Pair;
import network.message.protocols.LeaderElectionMessage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author : Swimiltylers
 * @version : 2019/4/27 12:03
 */
public class GlobalLeaderElectionController {
    protected int peerSize;
    private static final int LEADER_UNDECIDED = -1;

    private BlockingQueue<LeaderElectionMessage.LEOffer> offers;
    private List<BlockingQueue<LeaderElectionMessage>> lMessages;
    private AtomicInteger leader;

    private AtomicInteger countdown;

    public GlobalLeaderElectionController(int peerSize, int offerSize, int initLeader) {
        this.peerSize = peerSize;
        this.offers = new ArrayBlockingQueue<>(offerSize);
        this.leader = new AtomicInteger(initLeader);

        lMessages = new ArrayList<>();
        for (int i = 0; i < peerSize; i++) {
            lMessages.add(null);
        }

        countdown = new AtomicInteger(0);
    }

    public void stateUpdate(int[] bipolar){
        leader.updateAndGet(former -> {
            int count = 0;
            for (int value : bipolar) {
                if (value == 1)
                    ++count;
            }
            countdown.set(count);
            if (former != LEADER_UNDECIDED && bipolar[former] == 0)
                return LEADER_UNDECIDED;
            else
                return former;
        });
    }

    public void LEDecision(){
        List<Pair<LeaderElectionMessage.LEOffer, BlockingQueue<LeaderElectionMessage>>> ackList = new ArrayList<>();
        int[] tickets = new int[peerSize];
        Arrays.fill(tickets, 0);

        while (true){
            try {
                LeaderElectionMessage.LEOffer msg = offers.take();
                int leaderFormal = leader.get();
                if (leader.get() != LEADER_UNDECIDED)
                    lMessages.get(msg.fromId).put(new LeaderElectionMessage.LEForce(leaderFormal, msg.token, leaderFormal));
                else {
                    tickets[msg.fromId] = Integer.max(tickets[msg.fromId], msg.ticket);
                    ackList.add(new Pair<>(msg, lMessages.get(msg.fromId)));
                    if (countdown.decrementAndGet() == 0) {
                        final int leaderDecided = leader.updateAndGet( old -> {
                            int potentialLeader = 0;
                            for (int i = 0; i < tickets.length; i++) {
                                if (tickets[i] > tickets[potentialLeader])
                                    potentialLeader = i;
                            }
                            return potentialLeader;
                        });

                        System.out.println("decide "+ leaderDecided);

                        ackList.forEach(leOfferBlockingQueuePair -> {
                            try {
                                long ackToken = leOfferBlockingQueuePair.getKey().token;
                                leOfferBlockingQueuePair.getValue().put(new LeaderElectionMessage.LEForce(leaderDecided, ackToken, leaderDecided));
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        });

                        ackList.clear();
                        Arrays.fill(tickets, 0);
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public LeaderElectionProvider getLEProvider(int serverId, BlockingQueue<LeaderElectionMessage> lChan){
        lMessages.set(serverId, lChan);
        return offer -> {
            try {
                offers.put(offer);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        };
    }
}
