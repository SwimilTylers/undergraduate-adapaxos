package network.message.protocols;

import instance.PaxosInstance;

import java.io.Serializable;

/**
 * @author : Swimiltylers
 * @version : 2019/2/15 14:03
 */
public class DiskPaxosMessage implements Serializable {
    private static final long serialVersionUID = 4839141113425063951L;

    public final long dialog_no;
    public final int inst_no;

    public DiskPaxosMessage(long dialog_no, int inst_no) {
        this.dialog_no = dialog_no;
        this.inst_no = inst_no;
    }

    public enum DiskStatus implements Serializable{
        WRITE_SUCCESS, WRITE_FAILED,
        READ_SUCCESS, READ_NO_SUCH_FILE
    }

    public static class Write extends DiskPaxosMessage{
        private static final long serialVersionUID = 3349670115707669068L;

        public final int leaderId;
        public final int inst_ballot;

        public final PaxosInstance load;

        public Write(int inst_no, int leaderId, int inst_ballot, long dialog_no, PaxosInstance load) {
            super(dialog_no, inst_no);
            this.leaderId = leaderId;
            this.inst_ballot = inst_ballot;
            this.load = load;
        }
    }

    public static class ackWrite extends DiskPaxosMessage{
        private static final long serialVersionUID = -7655282631076359738L;
        public final int leaderId;
        public final int inst_ballot;

        public final int disk_no;
        public final DiskStatus status;

        public ackWrite(int inst_no, int leaderId, int inst_ballot, long dialog_no, int disk_no, DiskStatus status) {
            super(dialog_no, inst_no);
            this.leaderId = leaderId;
            this.inst_ballot = inst_ballot;
            this.disk_no = disk_no;
            this.status = status;
        }
    }

    public static class Read extends DiskPaxosMessage{
        private static final long serialVersionUID = -496589918364578353L;
        public final int leaderId;
        public final int inst_ballot;

        public final int accessId;

        public Read(int inst_no, int leaderId, int inst_ballot, long dialog_no, int accessId) {
            super(dialog_no, inst_no);
            this.leaderId = leaderId;
            this.inst_ballot = inst_ballot;
            this.accessId = accessId;
        }
    }

    public static class ackRead extends DiskPaxosMessage{
        private static final long serialVersionUID = -8402451465172952340L;
        public final int leaderId;
        public final int inst_ballot;

        public final int disk_no;
        public final int accessId;
        public final DiskStatus status;
        public final PaxosInstance load;

        public ackRead(int inst_no, int leaderId, int inst_ballot, long dialog_no, int disk_no, int accessId, DiskStatus status, PaxosInstance load) {
            super(dialog_no, inst_no);
            this.leaderId = leaderId;
            this.inst_ballot = inst_ballot;
            this.disk_no = disk_no;
            this.accessId = accessId;
            this.status = status;
            this.load = load;
        }
    }

    public static class PackedMessage extends DiskPaxosMessage{
        private static final long serialVersionUID = 556089935144378109L;
        public final int leaderId;
        public final int inst_ballot;

        public final String desc;
        public final DiskPaxosMessage[] packages;

        public PackedMessage(int inst_no, int leaderId, int inst_ballot,
                             String desc, long dialog_no,
                             DiskPaxosMessage[] packages) {
            super(dialog_no, inst_no);
            this.leaderId = leaderId;
            this.inst_ballot = inst_ballot;
            this.desc = desc;
            this.packages = packages;
        }
    }

    public static final String IRW_HEADER = "integrated Write and Read";
    public static final String IR_HEADER = "integrated read";
    public static final String IRW_ACK_HEADER = "makeAck: "+IRW_HEADER;
    public static final String IR_ACK_HEADER = "makeAck: "+IR_HEADER;

    public static DiskPaxosMessage.PackedMessage IRW(int inst_no, int leaderId, int inst_ballot,
                                                     long dialog_no,
                                                     int totalSize, PaxosInstance load){
        DiskPaxosMessage.Write writeOp = new DiskPaxosMessage.Write(inst_no, leaderId, inst_ballot, dialog_no, load);

        return new DiskPaxosMessage.PackedMessage(
                inst_no, leaderId, inst_ballot, IRW_HEADER, dialog_no,
                new DiskPaxosMessage[]{
                        writeOp,
                        IR(inst_no, leaderId, inst_ballot, dialog_no, totalSize)
                }
        );
    }

    public static DiskPaxosMessage.PackedMessage IR(int inst_no, int leaderId, int inst_ballot,
                                                    long dialog_no,
                                                    int totalSize){
        DiskPaxosMessage.Read[] readOps = new DiskPaxosMessage.Read[totalSize];
        for (int i = 0; i < totalSize; i++)
            if (i != leaderId)
                readOps[i] = new DiskPaxosMessage.Read(inst_no, leaderId, inst_ballot, dialog_no, i);

        return new DiskPaxosMessage.PackedMessage(
                inst_no, leaderId, inst_ballot, IR_HEADER, dialog_no,
                readOps
        );
    }

    public static DiskPaxosMessage.PackedMessage IRW_ACK(int inst_no, int leaderId, int inst_ballot, long dialog_no, ackWrite ackWrite, ackRead[] ackReads){
        return new DiskPaxosMessage.PackedMessage(
                inst_no, leaderId, inst_ballot, IRW_ACK_HEADER, dialog_no,
                new DiskPaxosMessage[]{
                        ackWrite,
                        IR_ACK(inst_no, leaderId, inst_ballot, dialog_no, ackReads)
                }
        );
    }

    public static DiskPaxosMessage.PackedMessage IR_ACK(int inst_no, int leaderId, int inst_ballot, long dialog_no, ackRead[] ackReads){
        return new DiskPaxosMessage.PackedMessage(
                inst_no, leaderId, inst_ballot, IR_ACK_HEADER, dialog_no,
                ackReads
        );
    }
}
