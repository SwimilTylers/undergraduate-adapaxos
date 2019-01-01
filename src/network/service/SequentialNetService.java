package network.service;

import com.sun.istack.internal.NotNull;
import javafx.util.Pair;

import java.io.IOException;
import java.net.*;
import java.util.Arrays;

/**
 * @author : Swimiltylers
 * @version : 2018/12/30 14:47
 */
public class SequentialNetService {
    private String netServiceId;
    private int port;

    private DatagramSocket com;
    private final byte[] receiveBuffer;

    public static final int MAX_RECEIVE_TOTAL_SIZE = 1024;

    public SequentialNetService(int port, @NotNull String id) throws SocketException {
        this.netServiceId = id;
        this.port = port;

        com = new DatagramSocket(this.port);
        receiveBuffer = new byte[MAX_RECEIVE_TOTAL_SIZE];
    }

    public byte[] fetchMessage() throws IOException {
        synchronized (receiveBuffer){
            DatagramPacket recvPac = new DatagramPacket(receiveBuffer, receiveBuffer.length);
            com.receive(recvPac);
            return Arrays.copyOf(recvPac.getData(), recvPac.getLength());
        }
    }

    public void sendMessage(@NotNull byte[] msg, @NotNull Pair<InetAddress, Integer> dest) throws IOException {
        DatagramPacket sendPac = new DatagramPacket(msg, msg.length, dest.getKey(), dest.getValue());
        com.send(sendPac);
    }

    @Override
    protected void finalize() throws Throwable {
        com.close();
        super.finalize();
    }

    public String getNetServiceId() {
        return netServiceId;
    }
}
