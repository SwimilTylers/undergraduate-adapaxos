package network.service;

import com.sun.istack.internal.NotNull;
import javafx.util.Pair;
import network.message.protocols.RegistrationProtocol;

import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;

/**
 * @author : Swimiltylers
 * @version : 2019/1/18 10:47
 */
@Deprecated
abstract public class ObjectUdpNetService<ComMessage_t> implements NetService<ComMessage_t>{
    public static final int COM_BUFFER_SIZE = 4096;
    public static final int REG_BUFFER_SIZE = COM_BUFFER_SIZE;

    protected String netServiceName;

    protected Set<Pair<InetAddress, Integer>> regNetPool;
    protected DatagramSocket regSocket;
    protected int regPort;

    protected ByteArrayInputStream regInBuffer;
    protected DatagramPacket regInPack;

    protected ObjectOutputStream regOut;
    protected ByteArrayOutputStream regOutBuffer;
    protected final byte[] regOutBufferPrefix;


    protected Set<Pair<InetAddress, Integer>> comNetPool;
    protected DatagramSocket comSocket;
    protected int comPort;

    protected ByteArrayInputStream comInBuffer;
    protected DatagramPacket comInPack;

    protected ObjectOutputStream comOut;
    protected ByteArrayOutputStream comOutBuffer;
    protected final byte[] comOutBufferPrefix;

    public ObjectUdpNetService(@NotNull String netId, final int localRegPort, final int localComPort) throws IOException {
        netServiceName = netId;

        regSocket = new DatagramSocket(localRegPort);
        regPort = localRegPort;

        byte[] inBuffer = new byte[REG_BUFFER_SIZE];
        regInBuffer = new ByteArrayInputStream(inBuffer);
        regInPack = new DatagramPacket(inBuffer, inBuffer.length);

        regOutBuffer = new ByteArrayOutputStream(REG_BUFFER_SIZE);
        regOut = new ObjectOutputStream(regOutBuffer);
        regOutBufferPrefix = regOutBuffer.toByteArray();

        comSocket = new DatagramSocket(localComPort);
        comPort = localComPort;

        byte[] c_inBuffer = new byte[COM_BUFFER_SIZE];
        comInBuffer = new ByteArrayInputStream(c_inBuffer);
        comInPack = new DatagramPacket(c_inBuffer, c_inBuffer.length);

        comOutBuffer = new ByteArrayOutputStream(COM_BUFFER_SIZE);
        comOut = new ObjectOutputStream(comOutBuffer);
        comOutBufferPrefix = comOutBuffer.toByteArray();
    }

    @Override
    protected void finalize() throws Throwable {
        regOut.close();
        comOut.close();

        regSocket.close();
        comSocket.close();
        super.finalize();
    }

    public void send (boolean isReg, @NotNull Object message, @NotNull InetAddress addr, int port) throws IOException {
        if (isReg){
            regOut.writeObject(message);
            regOut.flush();
            byte[] sendBytes = regOutBuffer.toByteArray();
            regSocket.send(new DatagramPacket(sendBytes, sendBytes.length, addr, port));

            regOut.reset();
            regOutBuffer.reset();
            regOutBuffer.write(regOutBufferPrefix); // BufferPrefix is necessary prefix for any OutputBuffer working for ObjectOutputBuffer
        }
        else{
            comOut.writeObject(message);
            comOut.flush();
            byte[] sendBytes = comOutBuffer.toByteArray();
            comSocket.send(new DatagramPacket(sendBytes, sendBytes.length, addr, port));

            comOut.reset();
            comOutBuffer.reset();
            comOutBuffer.write(comOutBufferPrefix); // BufferPrefix is necessary prefix for any OutputBuffer working for ObjectOutputBuffer
        }
    }

    public void sendAll(boolean isReg, @NotNull Object message) throws IOException {
        if (isReg){
            regOut.writeObject(message);
            regOut.flush();
            byte[] sendBytes = regOutBuffer.toByteArray();

            regNetPool.forEach(a -> {
                try {
                    regSocket.send(new DatagramPacket(sendBytes, sendBytes.length, a.getKey(), a.getValue()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            regOut.reset();
            regOutBuffer.reset();
            regOutBuffer.write(regOutBufferPrefix);
        }
        else{
            comOut.writeObject(message);
            comOut.flush();
            byte[] sendBytes = comOutBuffer.toByteArray();

            comNetPool.forEach(a -> {
                try {
                    comSocket.send(new DatagramPacket(sendBytes, sendBytes.length, a.getKey(), a.getValue()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            comOut.reset();
            comOutBuffer.reset();
            comOutBuffer.write(comOutBufferPrefix);
        }
    }

    public Pair<Object, Pair<InetAddress, Integer>> fetch (boolean isReg) throws IOException, ClassNotFoundException {
        if (isReg){
            regInBuffer.reset();
            regSocket.receive(regInPack);

            Pair<Object, Pair<InetAddress, Integer>> t;
            try (ObjectInputStream regIn = new ObjectInputStream(regInBuffer)) {
                t = new Pair<>(regIn.readObject(), new Pair<>(regInPack.getAddress(), regInPack.getPort()));
            }
            return t;

        }
        else{
            comInBuffer.reset();
            comSocket.receive(comInPack);

            Pair<Object, Pair<InetAddress, Integer>> t;
            try (ObjectInputStream comIn = new ObjectInputStream(comInBuffer)) {
                t = new Pair<>(comIn.readObject(), new Pair<>(comInPack.getAddress(), comInPack.getPort()));
            }
            return t;
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
                // TODO: has it to be localhost ?
                RegistrationProtocol reply = new RegistrationProtocol(netServiceName, InetAddress.getLocalHost().getHostAddress(), comPort);

                while (acceptorNum > l_comNetPool.size()){
                    Pair<Object, Pair<InetAddress, Integer>> info = fetch(true);
                    RegistrationProtocol msg = (RegistrationProtocol) info.getKey();
                    Pair<InetAddress, Integer> anotheraddr = new Pair<>(
                            InetAddress.getByName(msg.getAnotherChannel().getKey()),
                            msg.getAnotherChannel().getValue()
                    );

                    if (!acceptorName.contains(msg.getSenderName())){
                        acceptorName.add(msg.getSenderName());
                        l_regNetPool.add(info.getValue());
                        l_comNetPool.add(anotheraddr);
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

        public static final int DEFAULT_CLIENT_WAIT_INTERVAL = 200;
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
            // TODO: has it to be localhost ?
            RegistrationProtocol reg = new RegistrationProtocol(netServiceName,
                    new Pair<>(InetAddress.getLocalHost().getHostAddress(), comPort));

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
                    Pair<InetAddress, Integer> anotherAddr = new Pair<>(
                            InetAddress.getByName(msg.getAnotherChannel().getKey()),
                            msg.getAnotherChannel().getValue()
                    );

                    if (!responderName.contains(msg.getSenderName())){
                        responderName.add(msg.getSenderName());
                        respAddr.add(anotherAddr);
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
