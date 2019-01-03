package network.message.constructor;

import com.sun.istack.internal.NotNull;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * @author : Swimiltylers
 * @version : 2019/1/3 22:14
 */
public class BytesToMessageConstructor {
    private ByteBuffer byteBuffer;

    public static final int MAX_READ_BUFFER_LENGTH = 1024;
    public static final ByteOrder DEFAULT_READ_BYTEORDER = MessageToBytesConstructor.DEFAULT_WRITE_BYTEORDER;

    public BytesToMessageConstructor(@NotNull byte[] b){
        byteBuffer = ByteBuffer.wrap(b);
        byteBuffer.order(DEFAULT_READ_BYTEORDER);
        byteBuffer.position(b.length);
        byteBuffer.flip();
    }

    public byte readByte(byte b){
        return byteBuffer.get();
    }

    public byte[] readBytes(int length){
        byte[] b = new byte[length];
        byteBuffer.get(b);
        return b;
    }

    public int readInt(){
        return byteBuffer.getInt();
    }

    public long readLong(){
        return byteBuffer.getLong();
    }

    public double readDouble(){
        return byteBuffer.getDouble();
    }

    public void reconstruct(byte[] b){
        byteBuffer.clear();
        byteBuffer.put(b);
        byteBuffer.flip();
    }
}
