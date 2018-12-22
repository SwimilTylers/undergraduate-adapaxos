package channel;

import javafx.util.Pair;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

// TODO: logger is not complete

/**
 * Created with IntelliJ IDEA.
 * User: Swimiltylers
 * Date: 2018/12/21
 * Time: 23:45
 */
public class demo {
    static class Proposer implements Runnable {
        private static Logger logger = Logger.getLogger(demo.Proposer.class);

        private DatagramSocket serverSocket;
        private byte[] data = new byte[1024];

        private int totalAccNum;
        private ArrayList<Pair<InetAddress, Integer>> acceptors = new ArrayList<>();

        private int currentPNum;
        private boolean ifStart = false;
        private int consensus;


        Proposer(int startPNum, int port, int totalAccNum) throws SocketException, FileNotFoundException {
            this.totalAccNum = totalAccNum;
            this.currentPNum = startPNum;

            serverSocket = new DatagramSocket(port);
        }

        @Override
        protected void finalize() throws Throwable {
            serverSocket.close();
            super.finalize();
        }

        @Override
        public void run() {
            DatagramPacket packet = new DatagramPacket(data, data.length);

            // register acceptors
            while (totalAccNum > acceptors.size()){
                try {
                    serverSocket.receive(packet);
                } catch (IOException e) {
                    logger.error(e);
                    e.printStackTrace();
                }
                String info = new String(data, 0, packet.getLength());
                logger.error("[Proposer] REGISTER "+info);

                acceptors.add(new Pair<>(packet.getAddress(), packet.getPort()));
            }

            // stage 1: prepare<n>
            for (Pair<InetAddress, Integer> acc:acceptors
                 ) {
                try {
                    byte[] reply = ("PREPARE <"+currentPNum+">").getBytes();
                    serverSocket.send(new DatagramPacket(reply, reply.length, acc.getKey(), acc.getValue()));
                } catch (IOException e) {
                    logger.error(e);
                    e.printStackTrace();
                }
            }

            // stage 1.5: wait for the majority response
            boolean ifDecide = false;
            int receAccNum = 0;
            ifStart = false;
            int maxRecePNum = 0;
            while (!ifDecide){
                try {
                    serverSocket.receive(packet);
                } catch (IOException e) {
                    logger.error(e);
                    e.printStackTrace();
                }
                String[] ack = new String(data, 0, packet.getLength()).split(" ");
                if (ack[0].equals("ACK")){
                    if (receAccNum < totalAccNum/2){

                        String[] triple = ack[1].substring(1, ack[1].length()-1).split(",");

                        int chosen = Integer.valueOf(triple[1]);
                        int chosenPNum = Integer.valueOf(triple[2]);

                        if (!ifStart || chosenPNum > maxRecePNum){
                            consensus = chosen;
                            maxRecePNum = chosenPNum;
                            ifStart = true;
                        }


                        ++receAccNum;
                    }
                    else
                        ifDecide = true;
                }
            }

            // stage 2: decide
            for (Pair<InetAddress, Integer> acc:acceptors
            ) {
                try {
                    byte[] reply = ("ACCEPT <"+currentPNum+","+consensus+">").getBytes();
                    serverSocket.send(new DatagramPacket(reply, reply.length, acc.getKey(), acc.getValue()));
                } catch (IOException e) {
                    logger.error(e);
                    e.printStackTrace();
                }
            }
        }
    }

    static class Acceptor implements Runnable {
        private static Logger logger = Logger.getLogger(demo.Acceptor.class);

        private DatagramSocket clientSocket;
        private Pair<InetAddress, Integer> proposer;
        private String name;

        private byte[] data = new byte[1024];

        int input;
        boolean ifStart = false;
        int maxPNum;
        int decision, decPNum;

        Acceptor(String name, int input, String toIp, int toPort) throws UnknownHostException, SocketException, FileNotFoundException {
            InetAddress inet = InetAddress.getByName(toIp);
            clientSocket = new DatagramSocket();
            this.name = name;
            proposer = new Pair<>(inet, toPort);
            this.input = input;
            decision = input;

            logger.error("["+name+"] INPUT "+input);
        }

        @Override
        protected void finalize() throws Throwable {
            clientSocket.close();
            super.finalize();
        }

        @Override
        public void run() {
            try {
                DatagramPacket init = new DatagramPacket(name.getBytes(), name.length(), proposer.getKey(), proposer.getValue());
                clientSocket.send(init);
            } catch (IOException e) {
                logger.error(e);
                e.printStackTrace();
            }

            DatagramPacket packet = new DatagramPacket(data, data.length);
            boolean ifDecide = false;
            while (!ifDecide){
                try {
                    clientSocket.receive(packet);
                } catch (IOException e) {
                    logger.error(e);
                    e.printStackTrace();
                }

                int msglen = packet.getLength();
                String msg = new String(packet.getData(), 0, msglen);
                logger.error("["+name+"] get "+ msg);

                String[] analysis = msg.split(" ");
                if (analysis[0].equals("PREPARE")){
                    int pNum = Integer.valueOf(analysis[1].substring(1, analysis[1].length()-1));
                    if (!ifStart || pNum >= maxPNum){
                        ifStart = true;
                        maxPNum = pNum;

                        byte[] ack = ("ACK <"+pNum+","+decision+","+decPNum+">").getBytes();
                        DatagramPacket ackpack = new DatagramPacket(ack, ack.length, proposer.getKey(), proposer.getValue());
                        try {
                            clientSocket.send(ackpack);
                        } catch (IOException e) {
                            logger.error(e);
                            e.printStackTrace();
                        }
                    }
                }
                else if (analysis[0].equals("ACCEPT")){
                    String[] toNum = analysis[1].substring(1, analysis[1].length()-1).split(",");
                    int pNum = Integer.valueOf(toNum[0]);
                    int consensus = Integer.valueOf(toNum[1]);
                    if (pNum >= maxPNum){
                        decision = consensus;
                        decPNum = pNum;

                        byte[] ack = ("ACCEPTED <"+pNum+","+decision+">").getBytes();
                        DatagramPacket ackpack = new DatagramPacket(ack, ack.length, proposer.getKey(), proposer.getValue());
                        try {
                            clientSocket.send(ackpack);
                        } catch (IOException e) {
                            logger.error(e);
                            e.printStackTrace();
                        }

                        ifDecide = true;
                    }
                }
            }
        }
    }

    private static final int accCount = 5;
    private static final String proposerIP = "localhost";
    private static final int proposerPort = 10010;
    private static Logger logger = Logger.getLogger(demo.class);

    public static void main(String[] args) throws SocketException, UnknownHostException, FileNotFoundException {
        System.out.println("[Global] start");
        ExecutorService exec = Executors.newCachedThreadPool();

        Proposer proposer = new Proposer(0, proposerPort, accCount);
        exec.execute(proposer);

        for (int i = 0; i < accCount; i++) {
            Acceptor acceptor = new Acceptor("ACC-"+i, new Random().nextInt(64), proposerIP, proposerPort);
            exec.execute(acceptor);
        }

        exec.shutdown();
        System.out.println("[Global] emitted");
    }
}
