package agent;

import network.message.protocols.DiskPaxosMessage;

/**
 * @author : Swimiltylers
 * @version : 2019/3/24 21:39
 */
public interface DiskResponder {
    boolean isValidMessage(int inst_no, long token);
    void respond_ackWrite(DiskPaxosMessage.ackWrite ackWrite);
    void respond_ackRead(DiskPaxosMessage.ackRead ackRead);
}
