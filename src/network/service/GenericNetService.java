package network.service;

import com.sun.istack.internal.NotNull;
import client.ClientRequest;
import javafx.util.Pair;
import logger.PaxosLogger;
import network.message.protocols.Distinguishable;
import network.message.protocols.GenericClientMessage;
import network.message.protocols.GenericPaxosMessage;
import network.service.module.ConnectionModule;
import network.service.module.HeartBeatModule;
import network.service.peer.BasicPeerMessageReceiver;
import network.service.peer.BasicPeerMessageSender;
import network.service.peer.PeerMessageReceiver;
import network.service.peer.PeerMessageSender;

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

    public static final int DEFAULT_TO_CLIENT_PORT = 41020;
    private int toClientPort;

    private Socket[] peers;

    private PeerMessageSender sender;
    private PeerMessageReceiver receiver;

    public static final int DEFAULT_BEACON_INTERVAL = 800;
    private int beaconItv;
    private ConnectionModule cModule;

    private ExecutorService listenService;

    private BlockingQueue<ClientRequest> clientChan;
    private BlockingQueue<GenericPaxosMessage> paxosChan;
    private List<Pair<Distinguishable, BlockingQueue>> channels;
    private boolean onRunning;

    private PaxosLogger logger;

    public GenericNetService(int thisId, int toClientPort,
                             @NotNull BlockingQueue<ClientRequest> clientChan,
                             @NotNull BlockingQueue<GenericPaxosMessage> paxosChan,
                             @NotNull PaxosLogger logger){
        netServiceId = thisId;
        this.toClientPort = toClientPort;
        onRunning = false;
        channels = new ArrayList<>();
        this.clientChan = clientChan;
        this.paxosChan = paxosChan;

        this.beaconItv = DEFAULT_BEACON_INTERVAL;

        this.logger = logger;
    }

    @Override
    protected void finalize() throws Throwable {
        if (listenService != null)
            listenService.shutdown();
        onRunning = false;
        super.finalize();
    }

    public PeerMessageSender getPeerMessageSender() {
        return sender;
    }

    public void connect(@NotNull String[] addr, @NotNull int[] port) throws InterruptedException {
        assert addr.length == port.length;

        peerSize = addr.length;
        peerAddrList = addr;
        peerPortList = port;

        peers = new Socket[peerSize];
        cModule = new HeartBeatModule(netServiceId, peerSize);

        CountDownLatch latch = new CountDownLatch(2);

        ExecutorService service = Executors.newCachedThreadPool();
        service.execute(() -> connectToPeers(latch));
        service.execute(() -> waitingForPeers(latch));


        latch.await();

        sender = new BasicPeerMessageSender(netServiceId, peerSize, peers, cModule, logger);
        receiver = new BasicPeerMessageReceiver(netServiceId, sender, cModule, paxosChan, channels);

        onRunning = true;

        if (listenService == null)
            listenService = Executors.newCachedThreadPool();

        for (int i = 0; i < peerSize; i++) {
            if (i != netServiceId){
                Socket socket = peers[i];
                listenService.execute(() -> receiver.listenToPeers(socket));
            }
        }

        service.execute(this::beacon);

        service.shutdown();
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
                cModule.init(i);
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
                cModule.init(remoteId);
                System.out.println("Successfully Connected: from "+remoteId+" to "+netServiceId);
            }
        } catch (IOException e) {
            System.out.println("Cannot establish ServerSocket, abort: "+e.getMessage());
        } finally {
            latch.countDown();
        }
    }

    public void registerChannel(Distinguishable signal, BlockingQueue chan){
        channels.add(new Pair<>(signal, chan));
    }

    private void beacon(){
        while (onRunning){
            logger.log(false, cModule.toString());
            sender.broadcastPeerMessage(cModule.makeBeacon(System.currentTimeMillis()));
            try {
                Thread.sleep(beaconItv);
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            }
        }
    }

    public void watch(){
        if (onRunning){
            ServerSocket server = null;
            try {
                server = new ServerSocket(toClientPort);
            } catch (IOException e) {
                System.out.println("server cannot establish");
                return;
            }

            while (onRunning){
                Socket client;
                try {
                    client = server.accept();
                } catch (IOException e) {
                    System.out.println("client connection failed");
                    continue;
                }
                listenService.execute(() -> listenToClient(client));
            }
        }
    }

    private void listenToClient(@NotNull Socket socket){
        while (onRunning){
            Object msg;
            try {
                msg = (new ObjectInputStream(socket.getInputStream())).readObject();
            } catch (IOException|ClassNotFoundException e) {
                continue;
            }

            if (msg instanceof GenericClientMessage.Propose){
                GenericClientMessage.Propose cast = (GenericClientMessage.Propose) msg;

                sendClientMessage(socket, new GenericClientMessage.ackPropose(cast));   // ack to client
                System.out.println("PROPOSE "+cast.exec);
                try {
                    clientChan.put(new ClientRequest(cast, socket));
                    System.out.println(System.currentTimeMillis()+" client chan size " + clientChan.size());
                } catch (InterruptedException e) {
                    System.out.println("Generic Client Message Interrupted");
                }
            }
        }
    }

    synchronized public void sendClientMessage(@NotNull Socket client, @NotNull Object msg){
        try {
            OutputStream socketStream = client.getOutputStream();
            ObjectOutputStream ostream = new ObjectOutputStream(socketStream);
            ostream.writeObject(msg);
            ostream.flush();
            socketStream.flush();
        } catch (IOException e) {
            System.out.println("Client Message send faliure: "+e.getMessage());
        }
    }
}
