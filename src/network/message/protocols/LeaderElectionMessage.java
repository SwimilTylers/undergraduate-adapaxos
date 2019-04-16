package network.message.protocols;

import java.io.Serializable;

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

    public static class Propaganda extends LeaderElectionMessage{
        private static final long serialVersionUID = 8087204963401567707L;
        public final int ticket;

        public Propaganda(int fromId, long token, int ticket) {
            super(fromId, token);
            this.ticket = ticket;
        }
    }

    public static class Vote extends LeaderElectionMessage {
        private static final long serialVersionUID = 3554427274394541688L;
        public final int[] tickets;

        public Vote(int fromId, long token, int[] tickets) {
            super(fromId, token);
            this.tickets = tickets;
        }
    }

    public static class LESync extends LeaderElectionMessage {
        private static final long serialVersionUID = 3554427274394541688L;
        public final int[] tickets;
        public final boolean asLeader;

        public LESync(int fromId, long token, int[] tickets, boolean asLeader) {
            super(fromId, token);
            this.tickets = tickets;
            this.asLeader = asLeader;
        }
    }
}
