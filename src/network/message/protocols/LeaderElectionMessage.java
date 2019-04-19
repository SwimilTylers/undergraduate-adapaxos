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

        public Vote(int fromId, long token, int[] tickets) {
            super(fromId, token);
            this.tickets = tickets;
        }

        @Override
        public String toString() {
            return "[LE][Vote][tkn="+token+",fid="+fromId+"][tks="+ (tickets == null ? "null" : Arrays.toString(tickets)) +"]";
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
