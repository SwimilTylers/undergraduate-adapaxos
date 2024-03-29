package network.message.protocols;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @author : Swimiltylers
 * @version : 2019/4/14 22:31
 */
public class LeaderElectionMessage implements Serializable {
    private static final long serialVersionUID = -1114752667053952156L;
    public final int fromId;
    public final long token;


    public LeaderElectionMessage(int fromId, long token) {
        this.fromId = fromId;
        this.token = token;
    }

    public static class LeStart extends LeaderElectionMessage{
        public final long LeDialog_no;
        public final int LeTicket_local;

        public LeStart(int fromId, long leDialog_no, int leTicket_local) {
            super(fromId, leDialog_no);
            LeDialog_no = leDialog_no;
            LeTicket_local = leTicket_local;
        }

        @Override
        public String toString() {
            return "[LE][Start][tkn="+LeDialog_no+"][tkt="+LeTicket_local+"]";
        }
    }

    public static class Propaganda extends LeaderElectionMessage{
        private static final long serialVersionUID = 8087204963401567707L;
        public final int[] tickets;

        public Propaganda(int fromId, long token, int[] tickets) {
            super(fromId, token);
            this.tickets = tickets;
        }

        @Override
        public String toString() {
            return "[LE][Propaganda][tkn="+token+",fid="+fromId+"][tks="+ (tickets == null ? "null" : Arrays.toString(tickets)) +"]";
        }
    }

    public static class Vote extends LeaderElectionMessage {
        private static final long serialVersionUID = 3554427274394541688L;
        public final int[] tickets;
        public final int[] votes;

        public Vote(int fromId, long token, int[] tickets, int[] votes) {
            super(fromId, token);
            this.tickets = tickets;
            this.votes = votes;
        }

        @Override
        public String toString() {
            return "[LE][Vote][tkn="+token+",fid="+fromId+"][tks="+ (tickets == null ? "null" : Arrays.toString(tickets)) +"][votes="+ (votes == null ? "null" : Arrays.toString(votes)) + "]";
        }
    }

    public static class LEOffer extends LeaderElectionMessage{
        private static final long serialVersionUID = 7905747041438510087L;
        public final int ticket;

        public LEOffer(int fromId, long token, int ticket) {
            super(fromId, token);
            this.ticket = ticket;
        }
    }

    public static class LEForce extends LeaderElectionMessage {
        private static final long serialVersionUID = 3554427274394541688L;
        public final int leaderId;

        public LEForce(int fromId, long token, int leaderId) {
            super(fromId, token);
            this.leaderId = leaderId;
        }
    }
}
