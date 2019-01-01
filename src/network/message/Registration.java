package network.message;

import com.sun.istack.internal.NotNull;

/**
 * @author : Swimiltylers
 * @version : 2018/12/30 19:43
 */
public class Registration implements PaxosMessage{
    public final static int MAX_ID_LENGTH = 32;

    public String fromName;
    public String comAddr;
    public int comPort;

    Registration(){}


    public static Registration getInstance(byte[] info, int offset, int length){
        String[] values = new String(info, offset, length).split(" ");
        return getInstance(values[0], values[1], Integer.valueOf(values[2]));
    }

    public static Registration getInstance(String fromName, String comAddr, int comPort){
        Registration reg = new Registration();

        reg.fromName = fromName;
        reg.comAddr = comAddr;
        reg.comPort = comPort;

        return reg;
    }

    @Override
    public void fromBytes(byte[] msg, int offset, int length) {
        String[] values = new String(msg, offset, length).split(" ");

        fromName = values[0];
        comAddr = values[1];
        comPort = Integer.valueOf(values[2]);
    }

    @Override
    public byte[] toBytes() {
        return (fromName+" "+comAddr+" "+comPort).getBytes();
    }

    @Override
    public int getLength() {
        return fromName.length()+comAddr.length()+String.valueOf(comPort).length()+2;
    }
}
