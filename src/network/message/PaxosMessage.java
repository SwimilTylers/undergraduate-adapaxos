package network.message;

import com.sun.istack.internal.NotNull;

/**
 * @author : Swimiltylers
 * @version : 2018/12/30 19:45
 */
public abstract class PaxosMessage<T> {
    protected String senderName;
    protected byte[] rawInfo;

    public void setSenderName(@NotNull String senderName) {
        this.senderName = senderName;
    }

    public void setRawInfo(@NotNull byte[] rawInfo) {
        this.rawInfo = rawInfo;
    }

    public int getMessageLength() {
        if (rawInfo != null)
            return rawInfo.length;
        else
            return 0;
    }

    public String getSenderName() {
        return senderName;
    }

    public byte[] getRawInfo() {
        return rawInfo;
    }

    abstract protected byte[] toRawInfo(T info);
    abstract protected T fromRawInfo(byte[] raw);

    abstract public T getInfo();
    abstract public void SetInfo(T msg);
}
