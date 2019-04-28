package client;

import com.sun.istack.internal.NotNull;
import javafx.util.Pair;
import network.message.protocols.GenericClientMessage;
import utils.NetworkConfiguration;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author : Swimiltylers
 * @version : 2019/4/28 14:41
 */
public class TriggeredClient {
    private BlockingQueue<Pair<Integer, String>> info;
    private Socket[] conn;

    public TriggeredClient(int size) {
        this.info = new ArrayBlockingQueue<>(size);
    }

    public void online(NetworkConfiguration netConfig, final int itv){
        try {
            conn = new Socket[netConfig.peerSize];
            for (int i = 0; i < conn.length; i++) {
                conn[i] = new Socket(netConfig.peerAddr[i], netConfig.externalPort[i]);
            }

            while (true){
                Pair<Integer, String> msg = info.poll(itv, TimeUnit.MILLISECONDS);
                if (msg != null){
                    sendServerMessage(conn[msg.getKey()], new GenericClientMessage.Propose(msg.getValue()));
                }
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void trigger(int toId, String message){
        try {
            info.put(new Pair<>(toId, message));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private synchronized void sendServerMessage(@NotNull Socket server, @NotNull GenericClientMessage msg){
        try {
            OutputStream socketStream = server.getOutputStream();
            ObjectOutputStream outputStream = new ObjectOutputStream(socketStream);
            outputStream.writeObject(msg);

            outputStream.flush();
            socketStream.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
