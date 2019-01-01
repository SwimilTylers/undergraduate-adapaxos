package network.message;

import com.sun.istack.internal.NotNull;

/**
 * @author : Swimiltylers
 * @version : 2018/12/30 19:45
 */
public interface PaxosMessage {

    public void fromBytes(@NotNull byte[] msg, int offset, int length);

    public byte[] toBytes();

    public int getLength();

}
