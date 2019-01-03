package network.message;

import javafx.util.Pair;

import java.net.InetAddress;

/**
 * @author : Swimiltylers
 * @version : 2018/12/30 19:43
 */
public class RegPaxosMessage extends PaxosMessage<Pair<InetAddress, Integer>>{
    private Pair<InetAddress, Integer> info;


    @Override
    protected byte[] toRawInfo(Pair<InetAddress, Integer> info) {
        return null;
    }

    @Override
    protected Pair<InetAddress, Integer> fromRawInfo(byte[] raw) {
        return null;
    }

    @Override
    public Pair<InetAddress, Integer> getInfo() {
        return null;
    }

    @Override
    public void SetInfo(Pair<InetAddress, Integer> msg) {

    }
}
