package network.service;

import com.sun.istack.internal.NotNull;
import javafx.util.Pair;

import java.io.IOException;
import java.net.InetAddress;

/**
 * @author : Swimiltylers
 * @version : 2019/1/18 15:04
 */
@Deprecated
public interface NetService <Message_t> {
    void putDepartureObject (@NotNull Message_t message, @NotNull InetAddress addr, int port) throws IOException;
    void putBroadcastObject (@NotNull Message_t message) throws IOException;
    Pair<Message_t, Pair<InetAddress, Integer>> getArrivalObject () throws IOException, ClassNotFoundException;
}
