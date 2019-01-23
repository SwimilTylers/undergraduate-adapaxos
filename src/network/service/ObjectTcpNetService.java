package network.service;

import com.sun.istack.internal.NotNull;
import javafx.util.Pair;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;

/**
 * @author : Swimiltylers
 * @version : 2019/1/23 16:48
 */
public class ObjectTcpNetService<ComMessage_t> implements NetService<ComMessage_t>{

    protected String netServiceName;

    protected Set<Pair<InetAddress, Integer>> regNetPool;
    protected Socket regSocket;
    protected int regPort;

    protected ObjectInputStream regIn;
    protected ObjectOutputStream regOut;


    protected Set<Pair<InetAddress, Integer>> comNetPool;
    protected Socket comSocket;
    protected int comPort;


    protected ObjectInputStream comIn;
    protected ObjectOutputStream comOut;

    ObjectTcpNetService(@NotNull String id){
        netServiceName = id;
    }

    @Override
    public void putDepartureObject(ComMessage_t message, InetAddress addr, int port) throws IOException {

    }

    @Override
    public void putBroadcastObject(ComMessage_t message) throws IOException {

    }

    @Override
    public Pair<ComMessage_t, Pair<InetAddress, Integer>> getArrivalObject() throws IOException, ClassNotFoundException {
        return null;
    }

    public static class Server<Message> extends ObjectTcpNetService<Message> {

        private ServerSocket serverSocket;

        Server(String id) {
            super(id);
        }
    }
}
