package instance.maintenance;

import javafx.util.Pair;

import java.util.Arrays;

/**
 * @author : Swimiltylers
 * @version : 2019/4/16 11:02
 */
public class LeaderElectionMaintenance {
    public Pair<Long, Integer> leaderChosen;
    public int[] peerTickets;

    public int localLeTicket;
    public long leToken;
    public int leCount;

    public static LeaderElectionMaintenance init(int serverId, long leToken, int[] initTickets, boolean isLeader){
        LeaderElectionMaintenance ret = new LeaderElectionMaintenance();
        ret.peerTickets = initTickets;
        ret.localLeTicket = initTickets[serverId];
        ret.leToken = leToken;
        ret.leCount = 0;

        if (isLeader) {
            ret.leaderChosen = new Pair<>(leToken, serverId);
        }
        else
            ret.leaderChosen = null;

        return ret;
    }

    public static LeaderElectionMaintenance copyOf(LeaderElectionMaintenance old){
        LeaderElectionMaintenance ret = new LeaderElectionMaintenance();
        ret.leaderChosen = old.leaderChosen;
        ret.peerTickets = Arrays.copyOfRange(old.peerTickets, 0, old.peerTickets.length);
        ret.localLeTicket = old.localLeTicket;
        ret.leToken = old.leToken;
        ret.leCount = old.leCount;

        return ret;
    }
}
