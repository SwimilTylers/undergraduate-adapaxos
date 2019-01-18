package network.service;

import com.sun.istack.internal.NotNull;
import javafx.util.Pair;
import network.message.protocols.RegistrationProtocol;

import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;

/**
 * @author : Swimiltylers
 * @version : 2019/1/18 10:47
 */
abstract public class ObjectUdpNetService<ComMessage_t> implements NetService<ComMessage_t>{
    public static final int COM_BUFFER_SIZE = 4096;
    public static final int REG_BUFFER_SIZE = COM_BUFFER_SIZE;

    protected String netServiceName;

    protected Set<Pair<InetAddress, Integer>> regNetPool;
    protected DatagramSocket regSocket;

    protected ObjectInputStream regIn;
    protected ByteArrayInputStream regInBuffer;
    protected DatagramPacket regInPack;

    protected ObjectOutputStream regOut;
    protected ByteArrayOutputStream regOutBuffer;


    protected Set<Pair<InetAddress, Integer>> comNetPool;
    protected DatagramSocket comSocket;

    protected ObjectInputStream comIn;
    protected ByteArrayInputStream comInBuffer;
    protected DatagramPacket comInPack;

    protected ObjectOutputStream comOut;
    protected ByteArrayOutputStream comOutBuffer;

    public ObjectUdpNetService(@NotNull String netId, final int localRegPort, final int localComPort) throws IOException {
        regSocket = new DatagramSocket(localRegPort);
        netServiceName = netId;

        byte[] inBuffer = new byte[REG_BUFFER_SIZE];
        regInBuffer = new ByteArrayInputStream(inBuffer);
        regIn = new ObjectInputStream(regInBuffer);
        regInPack = new DatagramPacket(inBuffer, inBuffer.length);

        regOutBuffer = new ByteArrayOutputStream(REG_BUFFER_SIZE);
        regOut = new ObjectOutputStream(regOutBuffer);

        comSocket = new DatagramSocket(localComPort);

        inBuffer = new byte[COM_BUFFER_SIZE];
        comInBuffer = new ByteArrayInputStream(inBuffer);
        comIn = new ObjectInputStream(comInBuffer);
        comInPack = new DatagramPacket(inBuffer, inBuffer.length);

        comOutBuffer = new ByteArrayOutputStream(COM_BUFFER_SIZE);
        comOut = new ObjectOutputStream(comOutBuffer);
    }

    @Override
    protected void finalize() throws Throwable {
        regIn.close();
        regOut.close();

        regSocket.close();
        super.finalize();
    }

    public void send (boolean isReg, @NotNull Object message, @NotNull InetAddress addr, int port) throws IOException {
        if (isReg){
            regOut.writeObject(message);
            regSocket.send(new DatagramPacket(regOutBuffer.toByteArray(), regOutBuffer.size(), addr, port));

            regOut.reset();
        }
        else{
            comOut.writeObject(message);
            comSocket.send(new DatagramPacket(comOutBuffer.toByteArray(), comOutBuffer.size(), addr, port));

            comOut.reset();
        }
    }

    public void sendAll(boolean isReg, @NotNull Object message) throws IOException {
        Set<Pair<InetAddress, Integer>> pool = isReg ? regNetPool : comNetPool;
        regOut.writeObject(message);
        byte[] sendBytes = regOutBuffer.toByteArray();

        pool.forEach(a-> {
            try {
                regSocket.send(new DatagramPacket(sendBytes, sendBytes.length, a.getKey(), a.getValue()));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public Pair<Object, Pair<InetAddress, Integer>> fetch (boolean isReg) throws IOException, ClassNotFoundException {
        if (isReg){
            regIn.reset();
            regSocket.receive(regInPack);

            return new Pair<>(regIn.readObject(), new Pair<>(regInPack.getAddress(), regInPack.getPort()));
        }
        else{
            comIn.reset();
            comSocket.receive(comInPack);

            return new Pair<>(comIn.readObject(), new Pair<>(comInPack.getAddress(), comInPack.getPort()));
        }
    }

    @Override
    public void putDepartureObject(ComMessage_t message, InetAddress addr, int port) throws IOException {
        send(false, message, addr, port);
    }

    @Override
    public void putBroadcastObject(ComMessage_t message) throws IOException {
        sendAll(false, message);
    }

    @Override
    public Pair<ComMessage_t, Pair<InetAddress, Integer>> getArrivalObject() throws IOException, ClassNotFoundException {
        Pair<Object, Pair<InetAddress, Integer>> t = fetch(false);
        return new Pair<>((ComMessage_t)t.getKey(), t.getValue());
    }

    public static class Server<T> extends ObjectUdpNetService<T>{
        public Server(@NotNull String netId, final int localRegPort, final int localComPort) throws IOException {
            super(netId, localRegPort, localComPort);
        }

        public void initFixedNetPool(final int acceptorNum, final int expireMillis)
                throws InterruptedException, ExecutionException, TimeoutException {

            ExecutorService executorService = Executors.newSingleThreadExecutor();
            Future<Pair<Set<Pair<InetAddress, Integer>>, Set<Pair<InetAddress, Integer>>>> addr = executorService.submit(() -> {
                Set<String> acceptorName = new HashSet<>();
                Set<Pair<InetAddress, Integer>> l_regNetPool = new HashSet<>();
                Set<Pair<InetAddress, Integer>> l_comNetPool = new HashSet<>();
                RegistrationProtocol reply = new RegistrationProtocol(netServiceName, comSocket.getInetAddress(), comSocket.getPort());

                while (acceptorNum > l_comNetPool.size()){
                    Pair<Object, Pair<InetAddress, Integer>> info = fetch(true);
                    RegistrationProtocol msg = (RegistrationProtocol) info.getKey();

                    if (!acceptorName.contains(msg.getSenderName())){
                        acceptorName.add(msg.getSenderName());
                        l_regNetPool.add(info.getValue());
                        l_comNetPool.add(msg.getAnotherChannel());
                        super.send(true, reply, info.getValue().getKey(), info.getValue().getValue());
                    }
                }

                return new Pair<>(l_comNetPool, l_regNetPool);
            });

            try {
                Pair<Set<Pair<InetAddress, Integer>>, Set<Pair<InetAddress, Integer>>> res =
                        addr.get(expireMillis, TimeUnit.MILLISECONDS);
                comNetPool = res.getKey();
                regNetPool = res.getValue();
            } finally {
                executorService.shutdown();
            }
        }
    }

    public static class Client<T> extends ObjectUdpNetService<T>{

        public static final int DEFAULT_CLIENT_WAIT_INTERVAL = 20;
        public static final int DEFAULT_CLIENT_WAIT_EPOCH = 5;

        private int regWaitingIntervalMillis;
        private int regWaitingEpoch;

        public Client(@NotNull String netId, final int localRegPort, final int localComPort) throws IOException {
            super(netId, localRegPort, localComPort);
            regWaitingIntervalMillis = DEFAULT_CLIENT_WAIT_INTERVAL;
            regWaitingEpoch = DEFAULT_CLIENT_WAIT_EPOCH;
        }

        public void setRegNetPool(Set<Pair<InetAddress, Integer>> regNetPool) {
            this.regNetPool = regNetPool;
        }

        public void initSelfExistence(final int expireMillis)
                throws InterruptedException, TimeoutException, ExecutionException, IOException {

            RegistrationProtocol reg = new RegistrationProtocol(netServiceName,
                    new Pair<>(comSocket.getInetAddress(), comSocket.getPort()));

            for (int i = 0; i < regWaitingEpoch; i++) {
                super.sendAll(true, reg);
                Thread.sleep(regWaitingIntervalMillis);
            }

            ExecutorService executorService = Executors.newSingleThreadExecutor();
            Future<Set<Pair<InetAddress, Integer>>> ret = executorService.submit(() -> {
                int expectedResponseNum = regNetPool.size();

                Set<String> responderName = new HashSet<>();
                Set<Pair<InetAddress, Integer>> respAddr = new HashSet<>();

                while (expectedResponseNum > respAddr.size()){
                    RegistrationProtocol msg = (RegistrationProtocol) super.fetch(true).getKey();

                    if (!responderName.contains(msg.getSenderName())){
                        responderName.add(msg.getSenderName());
                        respAddr.add(msg.getAnotherChannel());
                    }
                }

                return respAddr;
            });

            try {
                comNetPool = ret.get(expireMillis, TimeUnit.MILLISECONDS);
            } finally {
                executorService.shutdown();
            }
        }
    }
}