package network.message;

import com.sun.istack.internal.NotNull;

import java.net.DatagramPacket;
import java.util.Arrays;

/**
 * @author : Swimiltylers
 * @version : 2019/1/3 10:40
 */
public class PaxosMessageFactory {
    static final int PM_FROMNAME_LENGTH = 8;
    static final int PM_TYPE_LENGTH = 8;
    static final int PM_INFOLENGTH_LENGTH = 4;

    static final int[] PM_SECTION_SUMLENGTH = {
            0,
            PM_FROMNAME_LENGTH,
            PM_TYPE_LENGTH + PM_FROMNAME_LENGTH,
            PM_TYPE_LENGTH + PM_FROMNAME_LENGTH + PM_INFOLENGTH_LENGTH
    };

    public static PaxosMessage readFromPacket(@NotNull DatagramPacket packet) {
        byte[] buffer = Arrays.copyOf(packet.getData(), packet.getLength());

        /*
        Paxos Message Header Protocol works like:

        | FromName | Type | InfoLength |     Info     |
        | 8 bytes  | 8 B  |    4 B     | infoLength B |
        0..........8.....16...........20...............

        */

        String fromName = new String(buffer, PM_SECTION_SUMLENGTH[0], PM_SECTION_SUMLENGTH[1]);
        String type = new String(buffer, PM_SECTION_SUMLENGTH[1], PM_SECTION_SUMLENGTH[2]);
        int infoLength = Integer.valueOf(new String(buffer, PM_SECTION_SUMLENGTH[2], PM_SECTION_SUMLENGTH[3]));
        byte[] info = Arrays.copyOfRange(buffer, PM_SECTION_SUMLENGTH[3], PM_SECTION_SUMLENGTH[3] + infoLength);

        PaxosMessage msg = null;

        if (type.equals("REGMSSGE")){
            msg = new RegPaxosMessage();
            msg.setSenderName(fromName);
            msg.setRawInfo(info);
        }

        return msg;
    }

    public static byte[] writeToBytes(String senderName, PaxosMessage message){
        return null;
    }
}
