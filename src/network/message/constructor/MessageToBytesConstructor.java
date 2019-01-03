package network.message.constructor;

import com.sun.istack.internal.NotNull;

import java.nio.*;

/**
 * @author : Swimiltylers
 * @version : 2019/1/3 21:20
 */
public class MessageToBytesConstructor {
    private ByteBuffer byteBuffer;

    public static final int MAX_WRITE_BUFFER_LENGTH = 1024;
    public static final ByteOrder DEFAULT_WRITE_BYTEORDER = ByteOrder.LITTLE_ENDIAN;

    public MessageToBytesConstructor(){
        byteBuffer = ByteBuffer.allocate(MAX_WRITE_BUFFER_LENGTH);
        byteBuffer.order(DEFAULT_WRITE_BYTEORDER);
    }

    public void writeByte(byte b){
        byteBuffer.put(b);
    }

    public void writeBytes(@NotNull byte[] b){
        byteBuffer.put(b);
    }

    public void writeBytes(@NotNull byte[] b, int offset, int length){
        byteBuffer.put(b, offset, length);
    }

    public void writeInt(int a){
        byteBuffer.putInt(a);
    }

    public void writeLong(long l){
        byteBuffer.putLong(l);
    }

    public void writeDouble(double d){
        byteBuffer.putDouble(d);
    }

    public byte[] toBytes(){
        byteBuffer.flip();
        byte[] ret = new byte[byteBuffer.limit()];
        byteBuffer.get(ret).clear();
        return ret;
    }
}
