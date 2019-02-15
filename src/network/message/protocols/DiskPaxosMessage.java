package network.message.protocols;

import instance.PaxosInstance;

import java.io.Serializable;

/**
 * @author : Swimiltylers
 * @version : 2019/2/15 14:03
 */
public class DiskPaxosMessage implements Serializable {
    private static final long serialVersionUID = 4839141113425063951L;
    public final long DIALOGUE_NO;

    public DiskPaxosMessage(long dialogue_no) {
        DIALOGUE_NO = dialogue_no;
    }

    public enum DiskStatus{
        WRITE_SUCCESS,
        READ_SUCCESS, READ_NO_SUCH_FILE
    }

    public static class Write extends DiskPaxosMessage{
        private static final long serialVersionUID = 3349670115707669068L;
        public final int inst_no;
        public final int leaderId;
        public final PaxosInstance load;

        public Write(long dialogue_no, int inst_no, int leaderId, PaxosInstance load) {
            super(dialogue_no);
            this.inst_no = inst_no;
            this.leaderId = leaderId;
            this.load = load;
        }
    }

    public static class ackWrite extends DiskPaxosMessage{
        private static final long serialVersionUID = -7655282631076359738L;
        public final int inst_no;
        public final int leaderId;

        public final int disk_no;
        public final DiskStatus status;

        public ackWrite(long dialogue_no, int inst_no, int leaderId, int disk_no, DiskStatus status) {
            super(dialogue_no);
            this.inst_no = inst_no;
            this.leaderId = leaderId;
            this.disk_no = disk_no;
            this.status = status;
        }
    }

    public static class Read extends DiskPaxosMessage{
        private static final long serialVersionUID = -496589918364578353L;
        public final int inst_no;
        public final int leaderId;

        public final int accessId;

        public Read(long dialogue_no, int inst_no, int leaderId, int accessId) {
            super(dialogue_no);
            this.inst_no = inst_no;
            this.leaderId = leaderId;
            this.accessId = accessId;
        }
    }

    public static class ackRead extends DiskPaxosMessage{
        private static final long serialVersionUID = -8402451465172952340L;
        public final int inst_no;
        public final int leaderId;

        public final int disk_no;
        public final int accessId;
        public final DiskStatus status;
        public final PaxosInstance load;

        public ackRead(long dialogue_no, int inst_no, int leaderId, int disk_no, int accessId, DiskStatus status, PaxosInstance load) {
            super(dialogue_no);
            this.inst_no = inst_no;
            this.leaderId = leaderId;
            this.disk_no = disk_no;
            this.accessId = accessId;
            this.status = status;
            this.load = load;
        }
    }

    public static class PackedMessage extends DiskPaxosMessage{
        private static final long serialVersionUID = 556089935144378109L;
        public final String desc;
        public final DiskPaxosMessage[] packages;

        public PackedMessage(long dialogue_no, String desc, DiskPaxosMessage[] packages) {
            super(dialogue_no);
            this.desc = desc;
            this.packages = packages;
        }
    }

    public static DiskPaxosMessage.PackedMessage integratedWriteAndRead(long dialogue_no, int inst_no, int leaderId, int totalSize, PaxosInstance load){
        DiskPaxosMessage.Write writeOp = new DiskPaxosMessage.Write(dialogue_no, inst_no, leaderId, load);
        DiskPaxosMessage.Read[] readOps = new DiskPaxosMessage.Read[totalSize];
        for (int i = 0; i < totalSize; i++) {
            if (i != leaderId){
                readOps[i] = new DiskPaxosMessage.Read(dialogue_no, inst_no, leaderId, i);
            }
        }

        return new DiskPaxosMessage.PackedMessage(dialogue_no, "integrated Write and Read",
                new DiskPaxosMessage[]{writeOp, new DiskPaxosMessage.PackedMessage(dialogue_no, "integrated read", readOps)});
    }
}
