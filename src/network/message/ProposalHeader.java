package network.message;

import java.io.Serializable;

/**
 * @author : Swimiltylers
 * @version : 2018/12/30 16:30
 */
public class ProposalHeader implements PaxosMessage {
    private long proposalNum;

    @Override
    public void fromBytes(byte[] msg, int offset, int length) {

    }

    @Override
    public byte[] toBytes() {
        return new byte[0];
    }

    @Override
    public int getLength() {
        return 0;
    }
}
