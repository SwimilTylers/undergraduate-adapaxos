package network.message.protocols;

import client.ClientRequest;
import instance.PaxosInstance;

import java.io.Serializable;

/**
 * @author : Swimiltylers
 * @version : 2019/1/28 18:13
 */
public class GenericPaxosMessage implements Serializable {
    private static final long serialVersionUID = 4362925581572352758L;


    public enum ackMessageType {
        PROCEEDING, RESTORE, RECOVER, ABORT
    }

    public static class Prepare extends GenericPaxosMessage{
        private static final long serialVersionUID = -8756381233529135142L;
        public final int inst_no;
        public final int leaderId;
        public final int inst_ballot;

        public Prepare(int inst_no, int leaderId, int inst_ballot){
            this.inst_no = inst_no;
            this.leaderId = leaderId;
            this.inst_ballot = inst_ballot;
        }
    }

    public static class ackPrepare extends GenericPaxosMessage{
        private static final long serialVersionUID = 8624284808684642886L;
        public final int inst_no;
        public final ackMessageType type;
        public final int ack_leaderId;
        public final int inst_ballot;
        public final PaxosInstance load;

        public ackPrepare(int inst_no, ackMessageType type, int ack_leaderId, int inst_ballot, PaxosInstance load) {
            this.inst_no = inst_no;
            this.type = type;
            this.ack_leaderId = ack_leaderId;
            this.inst_ballot = inst_ballot;
            this.load = load;
        }
    }

    public static class Accept extends GenericPaxosMessage{
        private static final long serialVersionUID = -2766698737132928205L;
        public final int inst_no;
        public final int leaderId;
        public final int inst_ballot;
        public final ClientRequest[] cmds;

        public Accept(int inst_no, int leaderId, int inst_ballot, ClientRequest[] cmds) {
            this.inst_no = inst_no;
            this.leaderId = leaderId;
            this.inst_ballot = inst_ballot;
            this.cmds = cmds;
        }

        @Override
        public String toString() {
            return "["+inst_no+","+leaderId+","+inst_ballot+",cmd_length="+cmds.length+"["+cmds[0].exec+"...]]";
        }
    }

    public static class ackAccept extends GenericPaxosMessage{
        private static final long serialVersionUID = 679679722103052592L;
        public final int inst_no;
        public final ackMessageType type;
        public final int ack_leaderId;
        public final int inst_ballot;
        public final PaxosInstance load;
        public final ClientRequest[] cmds;

        public ackAccept(int inst_no, ackMessageType type, int ack_leaderId, int inst_ballot, PaxosInstance load, ClientRequest[] cmds) {
            this.inst_no = inst_no;
            this.type = type;
            this.ack_leaderId = ack_leaderId;
            this.inst_ballot = inst_ballot;
            this.load = load;
            this.cmds = cmds;
        }
    }

    public static class Commit extends GenericPaxosMessage{
        private static final long serialVersionUID = -6709805198564087486L;
        public final int inst_no;
        public final int leaderId;
        public final int inst_ballot;
        public final ClientRequest[] cmds;

        public Commit(int inst_no, int leaderId, int inst_ballot, ClientRequest[] cmds) {
            this.inst_no = inst_no;
            this.leaderId = leaderId;
            this.inst_ballot = inst_ballot;
            this.cmds = cmds;
        }
    }

    public static class Restore extends GenericPaxosMessage{
        private static final long serialVersionUID = -6283205199126333110L;
        public final int inst_no;
        public final PaxosInstance load;

        public Restore(int inst_no, PaxosInstance load) {
            this.inst_no = inst_no;
            this.load = load;
        }
    }
}
