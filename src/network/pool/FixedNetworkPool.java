package network.pool;

import javafx.util.Pair;
import network.message.Registration;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author : Swimiltylers
 * @version : 2018/12/30 19:29
 */
public class FixedNetworkPool {

    public class Server{
        private final int poolSize;
        private TreeMap<String, Pair<InetAddress, Integer>> pool;
        private String serverId;
        private Pair<InetAddress, Integer> comServerAddr;

        DatagramSocket reg;

        public static final int MAX_REG_LENGTH = 1024;

        Server(int port, String serverId, Pair<InetAddress, Integer> comServerAddr, int poolSize) throws SocketException {
            this.poolSize = poolSize;
            this.pool = new TreeMap<>();
            this.serverId = serverId;
            this.comServerAddr = comServerAddr;

            reg = new DatagramSocket(port);
        }

        @Override
        protected void finalize() throws Throwable {
            reg.close();
            super.finalize();
        }

        public Set<Pair<InetAddress, Integer>> init(int expireTimeMillis) throws InterruptedException, ExecutionException, TimeoutException {
            Callable<Set<Pair<InetAddress, Integer>>> initTask = new Callable<Set<Pair<InetAddress, Integer>>>() {
                @Override
                public Set<Pair<InetAddress, Integer>> call() throws Exception {
                    byte[] buffer = new byte[MAX_REG_LENGTH];
                    Set<Pair<InetAddress, Integer>> comAddressBook = new HashSet<>();
                    DatagramPacket pack = new DatagramPacket(buffer, buffer.length);

                    while(pool.size() < poolSize){
                        reg.receive(pack);
                        Registration msg = Registration.getInstance(pack.getData(), pack.getOffset(), pack.getLength());
                        if (!pool.containsKey(msg.fromName)){
                            pool.put(msg.fromName, new Pair<>(pack.getAddress(), pack.getPort()));
                            comAddressBook.add(new Pair<InetAddress, Integer>(InetAddress.getByName(msg.comAddr), msg.comPort));
                        }
                    }

                    return comAddressBook;
                }
            };

            ExecutorService executorService = Executors.newSingleThreadExecutor();
            Future<Set<Pair<InetAddress, Integer>>> future = executorService.submit(initTask);

            Set<Pair<InetAddress, Integer>> comAddrBook;

            try {
                comAddrBook = future.get(expireTimeMillis, TimeUnit.MILLISECONDS);
            } finally {
                executorService.shutdownNow();
            }

            byte[] buf = Registration.getInstance(serverId, comServerAddr.getKey().getHostName(), comServerAddr.getValue()).toBytes();

            pool.forEach((k,v)-> {
                try {
                    reg.send(new DatagramPacket(buf, buf.length, v.getKey(), v.getValue()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            return comAddrBook;
        }
    }

    public class Client{
        private String clientId;
        private Set<Pair<InetAddress, Integer>> regServerAddr;

        private Pair<InetAddress, Integer> comClientAddr;
        private Set<Pair<InetAddress, Integer>> comServerAddr;

        DatagramSocket reg;

        Client(String clientId, Pair<InetAddress, Integer> comClientAddr, Set<Pair<InetAddress, Integer>> regServers) throws SocketException {
            this.clientId = clientId;
            this.regServerAddr = regServers;
            this.comClientAddr = comClientAddr;

            comServerAddr = new HashSet<>();
            reg = new DatagramSocket();
        }

        Client(int port, String clientId, Pair<InetAddress, Integer> comClientAddr, Set<Pair<InetAddress, Integer>> regServers) throws SocketException {
            this.clientId = clientId;
            this.regServerAddr = regServers;
            this.comClientAddr = comClientAddr;

            comServerAddr = new HashSet<>();
            reg = new DatagramSocket(port);
        }

        @Override
        protected void finalize() throws Throwable {
            reg.close();
            super.finalize();
        }

        // TODO: client init
    }
}
