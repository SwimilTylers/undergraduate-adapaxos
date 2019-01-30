package network.service;

import com.sun.istack.internal.NotNull;
import instance.ClientRequest;
import javafx.util.Pair;
import network.message.protocols.GenericBeacon;
import network.message.protocols.Distinguishable;
import network.message.protocols.GenericClientMessage;
import network.message.protocols.GenericPaxosMessage;
import network.service.handler.GenericBeaconHandler;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author : Swimiltylers
 * @version : 2019/1/27 11:35
 */
public class GenericNetService {
    private int netServiceId;
    private int peerSize;
    private String[] peerAddrList;
    private int[] peerPortList;

    private Socket[] peers;
    private BufferedInputStream[] peerReaderBuffer;
    private BufferedOutputStream[] peerWriteBuffer;
    private GenericBeaconHandler beaconHandler;

    private ExecutorService listenService;

    private BlockingQueue<ClientRequest> clientChan;
    private BlockingQueue<GenericPaxosMessage> paxosChan;
    private List<Pair<Distinguishable, BlockingQueue>> channels;
    private boolean onRunning;

    public GenericNetService(int thisId, @NotNull BlockingQueue<ClientRequest> clientChan, @NotNull BlockingQueue<GenericPaxosMessage> paxosChan){
        netServiceId = thisId;
        onRunning = false;
        channels = new ArrayList<>();
        this.clientChan = clientChan;
        this.paxosChan = paxosChan;
    }

    @Override
    protected void finalize() throws Throwable {
        if (listenService != null)
            listenService.shutdown();
        onRunning = false;
        super.finalize();
    }

    public void addNewChannel(@NotNull Distinguishable condition, @NotNull BlockingQueue channel){
        if (!onRunning)
            channels.add(new Pair<>(condition, channel));
    }

    public void connect(@NotNull String[] addr, @NotNull int[] port) throws InterruptedException {
        assert addr.length == port.length;

        peerSize = addr.length;
        peerAddrList = addr;
        peerPortList = port;

        peers = new Socket[peerSize];
        peerReaderBuffer = new BufferedInputStream[peerSize];
        peerWriteBuffer = new BufferedOutputStream[peerSize];
        beaconHandler = new GenericBeaconHandler(peerSize);

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
                listenService.execute(() -> listenTOPeers(istream));
            }
        }
    }

    private void connectToPeers(@NotNull CountDownLatch latch){
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
                beaconHandler.alive(i);
            }
        } finally {
            latch.countDown();
        }
    }

    private void waitingForPeers(@NotNull CountDownLatch latch){
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
                beaconHandler.alive(remoteId);
                System.out.println("Successfully Connected: from "+remoteId+" to "+netServiceId);
            }
        } catch (IOException e) {
            System.out.println("Cannot establish ServerSocket, abort: "+e.getMessage());
        } finally {
            latch.countDown();
        }
    }

    @SuppressWarnings("unchecked")
    private void listenTOPeers(@NotNull BufferedInputStream chan){
        onRunning = true;

        while (onRunning){
            Object msg;
            try {
                msg = (new ObjectInputStream(chan)).readObject();
            } catch (IOException|ClassNotFoundException e) {
                continue;
            }

            if (msg instanceof GenericBeacon){
                System.out.println("Receive beacon");
                GenericBeacon cast = (GenericBeacon) msg;
                sendPeerMessage(cast.fromId, beaconHandler.handle(cast));
            }
            else if (msg instanceof GenericPaxosMessage){
                GenericPaxosMessage cast = (GenericPaxosMessage) msg;
                try {
                    paxosChan.put(cast);
                } catch (InterruptedException e) {
                    System.out.println("Generic Paxos Message Interrupted");
                }
            }
            else{
                for (Pair<Distinguishable, BlockingQueue> t:channels) {
                    if (t.getKey().meet(msg)){
                        try {
                            t.getValue().put(msg);
                            break;
                        } catch (InterruptedException e) {
                            System.out.println("Costumed Message Interrupted");
                        }
                    }
                }
            }
        }
    }

    private void listenToClient(@NotNull BufferedInputStream istream, @NotNull BufferedOutputStream ostream){
        onRunning = true;

        while (onRunning){
            Object msg;
            try {
                msg = (new ObjectInputStream(istream)).readObject();
            } catch (IOException|ClassNotFoundException e) {
                continue;
            }

            if (msg instanceof GenericClientMessage.Propose){
                GenericClientMessage.Propose cast = (GenericClientMessage.Propose) msg;
                try {
                    clientChan.put(new ClientRequest(cast, ostream));
                } catch (InterruptedException e) {
                    System.out.println("Generic Client Message Interrupted");
                }
            }
        }
    }

    public void sendPeerMessage(int toId, @NotNull Object msg){
        if (toId < peerSize && beaconHandler.check(toId)){
            try {
                ObjectOutputStream ostream = new ObjectOutputStream(peerWriteBuffer[toId]);
                ostream.writeObject(msg);
                ostream.flush();
                ostream.reset();
            } catch (IOException e) {
                System.out.println("Paxos Message send faliure: "+e.getMessage());
            }
        }
    }

    public void broadcastPeerMessage(@NotNull Object msg){
        for (int i = 0; i < peerSize; i++) {
            if (i != netServiceId){
                sendPeerMessage(i, msg);
            }
        }
    }
}
