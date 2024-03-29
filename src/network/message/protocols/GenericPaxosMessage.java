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
    public final int inst_no;

    public GenericPaxosMessage(int inst_no) {
        this.inst_no = inst_no;
    }

    public enum ackMessageType {
        PROCEEDING, RESTORE, RECOVER, ABORT
    }

    public static class Prepare extends GenericPaxosMessage{
        private static final long serialVersionUID = -8756381233529135142L;
        public final int leaderId;
        public final int inst_ballot;

        public Prepare(int inst_no, int leaderId, int inst_ballot){
            super(inst_no);
            this.leaderId = leaderId;
            this.inst_ballot = inst_ballot;
        }

        @Override
        public String toString() {
            return "[PREPARE][ino="+inst_no+",lid="+leaderId+",blt="+inst_ballot+"]";
        }
    }

    public static class ackPrepare extends GenericPaxosMessage{
        private static final long serialVersionUID = 8624284808684642886L;
        public final ackMessageType type;
        public final int ack_leaderId;
        public final int inst_ballot;
        public final PaxosInstance load;

        public ackPrepare(int inst_no, ackMessageType type, int ack_leaderId, int inst_ballot, PaxosInstance load) {
            super(inst_no);
            this.type = type;
            this.ack_leaderId = ack_leaderId;
            this.inst_ballot = inst_ballot;
            this.load = load;
        }

        @Override
        public String toString() {
            return "[ACK_PREPARE][ino="+inst_no+",lid="+ack_leaderId+",blt="+inst_ballot+"]";
        }
    }

    public static class Accept extends GenericPaxosMessage{
        private static final long serialVersionUID = -2766698737132928205L;
        public final int leaderId;
        public final int inst_ballot;
        public final ClientRequest[] cmds;

        public Accept(int inst_no, int leaderId, int inst_ballot, ClientRequest[] cmds) {
            super(inst_no);
            this.leaderId = leaderId;
            this.inst_ballot = inst_ballot;
            this.cmds = cmds;
        }

        @Override
        public String toString() {
            return "[ACCEPT][ino="+inst_no+",lid="+leaderId+",blt="+inst_ballot+",cmd_length="+cmds.length+"["+cmds[0].exec+"...]]";
        }
    }

    public static class ackAccept extends GenericPaxosMessage{
        private static final long serialVersionUID = 679679722103052592L;
        public final ackMessageType type;
        public final int ack_leaderId;
        public final int inst_ballot;
        public final PaxosInstance load;
        public final ClientRequest[] cmds;

        public ackAccept(int inst_no, ackMessageType type, int ack_leaderId, int inst_ballot, PaxosInstance load, ClientRequest[] cmds) {
            super(inst_no);
            this.type = type;
            this.ack_leaderId = ack_leaderId;
            this.inst_ballot = inst_ballot;
            this.load = load;
            this.cmds = cmds;
        }

        @Override
        public String toString() {
            return "[ACK_ACCEPT][ino="+inst_no+",lid="+ack_leaderId+",blt="+inst_ballot+"]"+",cmd_length="+cmds.length+"["+cmds[0].exec+"...]]";
        }
    }

    public static class Commit extends GenericPaxosMessage{
        private static final long serialVersionUID = -6709805198564087486L;
        public final int leaderId;
        public final int inst_ballot;
        public final ClientRequest[] cmds;

        public Commit(int inst_no, int leaderId, int inst_ballot, ClientRequest[] cmds) {
            super(inst_no);
            this.leaderId = leaderId;
            this.inst_ballot = inst_ballot;
            this.cmds = cmds;
        }

        @Override
        public String toString() {
            return "[COMMIT][ino="+inst_no+",lid="+leaderId+",blt="+inst_ballot+",cmd_length="+cmds.length+"]["+cmds[0].exec+"...]";
        }
    }

    public static class Restore extends GenericPaxosMessage{
        private static final long serialVersionUID = -6283205199126333110L;
        public final PaxosInstance load;

        public Restore(int inst_no, PaxosInstance load) {
            super(inst_no);
            this.load = load;
        }
    }

    public static class Sync extends GenericPaxosMessage{
        private static final long serialVersionUID = -8757260106040114565L;
        public final long dialog_no;
        public final int fromId;
        public final PaxosInstance load;

        public Sync(int inst_no, long dialog_no, int fromId, PaxosInstance load) {
            super(inst_no);
            this.dialog_no = dialog_no;
            this.fromId = fromId;
            this.load = load;
        }

        @Override
        public String toString() {
            return "[SYNC][inst_no="+inst_no+"]";
        }
    }

    public static class ackSync extends GenericPaxosMessage{
        private static final long serialVersionUID = -8757260106040114565L;
        public final long dialog_no;
        public final PaxosInstance load;

        public ackSync(int inst_no, long dialog_no, PaxosInstance load) {
            super(inst_no);
            this.dialog_no = dialog_no;
            this.load = load;
        }

        @Override
        public String toString() {
            return "[ACK_SYNC][inst_no="+inst_no+"]";
        }
    }
}
