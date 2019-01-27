package network.service;

import com.sun.istack.internal.NotNull;
import javafx.util.Pair;
import network.message.protocols.Beacon;
import network.message.protocols.Distinguishable;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author : Swimiltylers
 * @version : 2019/1/27 11:35
 */
public class GenericNetService {
    public static final int MAX_MESSAGE_TYPE = 16;

    private int netServiceId;
    private int peerSize;
    private String[] peerAddrList;
    private int[] peerPortList;

    private Socket[] peers;
    private BufferedInputStream[] peerReaderBuffer;
    private BufferedOutputStream[] peerWriteBuffer;
    private boolean[] Alive;

    private ExecutorService listenService;

    private List<Pair<Distinguishable, BlockingDeque>> channels;
    private boolean onRunning;

    public GenericNetService(int thisId){
        netServiceId = thisId;
        onRunning = false;
        channels = new ArrayList<>();
    }

    @Override
    protected void finalize() throws Throwable {
        if (listenService != null)
            listenService.shutdown();
        onRunning = false;
        super.finalize();
    }

    public void connect(@NotNull String[] addr, @NotNull int[] port) throws InterruptedException {
        assert addr.length == port.length;

        peerSize = addr.length;
        peerAddrList = addr;
        peerPortList = port;

        peers = new Socket[peerSize];
        peerReaderBuffer = new BufferedInputStream[peerSize];
        peerWriteBuffer = new BufferedOutputStream[peerSize];
        Alive = new boolean[peerSize];

        CountDownLatch latch = new CountDownLatch(2);

        ExecutorService service = Executors.newCachedThreadPool();
        service.execute(() -> connectToPeers(latch));
        service.execute(() -> waitingForPeers(latch));
        service.shutdown();

        latch.await();

        if (listenService == null)
            listenService = Executors.newCachedThreadPool();

        for (int i = 0; i < peerSize; i++) {
            if (i != netServiceId){
                BufferedInputStream istream = peerReaderBuffer[i];
                listenService.execute(() -> listen(istream));
            }
        }
    }

    private void connectToPeers(CountDownLatch latch){
        try {
            for (int i = 0; i < netServiceId; i++) {
                Socket socket = null;
                try {
                    socket = new Socket(peerAddrList[i], peerPortList[i]);
                } catch (IOException e) {
                    System.out.println("Cannot establish Socket to ["+peerAddrList[i]+":"+peerPortList[i]+"]: "+e.getMessage());
                    continue;
                }

                BufferedOutputStream buffer;
                ObjectOutputStream writer;

                try {
                    buffer = new BufferedOutputStream(socket.getOutputStream());
                    writer = new ObjectOutputStream(buffer);
                } catch (IOException e) {
                    System.out.println("Connection Failed (OUTPUT): "+e.getMessage());
                    continue;
                }

                try {
                    writer.writeInt(netServiceId);
                    writer.flush();
                    writer.reset();
                } catch (IOException e) {
                    System.out.println("Cannot dispatch this.ID: "+e.getMessage());
                    continue;
                }
                peers[i] = socket;
                peerWriteBuffer[i] = buffer;

                try {
                    peerReaderBuffer[i] = new BufferedInputStream(socket.getInputStream());
                } catch (IOException e) {
                    System.out.println("Connection Failed (INPUT): "+e.getMessage());
                    continue;
                }
                Alive[i] = true;
            }
        } finally {
            latch.countDown();
        }
    }

    private void waitingForPeers(CountDownLatch latch){
        try {
            ServerSocket listener = new ServerSocket(peerPortList[netServiceId]);
            for (int i = netServiceId + 1; i < peerSize; i++) {
                Socket conn = null;
                try {
                    conn = listener.accept();
                } catch (IOException e) {
                    System.out.println("Accept error: "+e.getMessage());
                    continue;
                }

                BufferedInputStream buffer;
                ObjectInputStream reader;
                try {
                    buffer = new BufferedInputStream(conn.getInputStream());
                    reader = new ObjectInputStream(buffer);
                } catch (IOException e) {
                    System.out.println("Connection Failed (INPUT): "+e.getMessage());
                    continue;
                }

                int remoteId = 0;
                try {
                    remoteId = reader.readInt();
                } catch (IOException e) {
                    System.out.println("Cannot fetch the ID: "+e.getMessage());
                    continue;
                }

                peers[remoteId] = conn;
                peerReaderBuffer[remoteId] = buffer;
                try {
                    peerWriteBuffer[remoteId] = new BufferedOutputStream(conn.getOutputStream());
                } catch (IOException e) {
                    System.out.println("Connection Failed (OUTPUT): "+e.getMessage());
                    continue;
                }
                Alive[remoteId] = true;
                System.out.println("Successfully Connected: from "+remoteId+" to "+netServiceId);
            }
        } catch (IOException e) {
            System.out.println("Cannot establish ServerSocket, abort: "+e.getMessage());
        } finally {
            latch.countDown();
        }
    }

    private void listen(BufferedInputStream chan){
        onRunning = true;

        while (onRunning){
            Object msg;
            try {
                msg = (new ObjectInputStream(chan)).readObject();
            } catch (IOException|ClassNotFoundException e) {
                continue;
            }

            if (msg instanceof Beacon){
                System.out.println("beacon");
            }
            else{
                for (Pair<Distinguishable, BlockingDeque> t:channels) {
                    if (t.getKey().meet(msg)){
                        try {
                            t.getValue().put(msg);
                            break;
                        } catch (InterruptedException ignored) {}
                    }
                }
            }
        }
    }
}
