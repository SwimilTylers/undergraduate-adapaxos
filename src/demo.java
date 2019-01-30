import agent.Acceptor;
import agent.Proposer;
import javafx.util.Pair;
import network.service.GenericNetService;
import network.service.ObjectUdpNetService;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.net.*;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;

// TODO: logger is not complete

/**
 * Created with IntelliJ IDEA.
 * User: Swimiltylers
 * Date: 2018/12/21
 * Time: 23:45
 */
public class demo {
    public static class netServiceTest{
        final static int[][] serverPorts = {{55432, 23091}, {55511, 46001}};
        final static int[][] clientPorts = {{32213, 42232}, {60991, 56771}};

        final static String[] serverId = {"server0", "server1"};
        final static String[] clientIds = {"client0", "client1"};

        static Set<Pair<InetAddress, Integer>> serverRegIP = null;
        static {
            try {
                serverRegIP = new HashSet<>();
                serverRegIP.add(new Pair<InetAddress, Integer>(InetAddress.getByName("localhost"), serverPorts[0][0]));
                serverRegIP.add(new Pair<InetAddress, Integer>(InetAddress.getByName("localhost"), serverPorts[1][0]));
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }

        public static void test0() throws IOException {
            ObjectUdpNetService.Server<Integer> server0 = new ObjectUdpNetService.Server<>(serverId[0], serverPorts[0][0], serverPorts[0][1]);
            ObjectUdpNetService.Server<Integer> server1 = new ObjectUdpNetService.Server<>(serverId[1], serverPorts[1][0], serverPorts[1][1]);
            ObjectUdpNetService.Client<Integer> client0 = new ObjectUdpNetService.Client<>(clientIds[0], clientPorts[0][0], clientPorts[0][1]);
            ObjectUdpNetService.Client<Integer> client1 = new ObjectUdpNetService.Client<>(clientIds[1], clientPorts[1][0], clientPorts[1][1]);

            client0.setRegNetPool(serverRegIP);
            client1.setRegNetPool(serverRegIP);

            ExecutorService executorService = Executors.newCachedThreadPool();
            executorService.execute(() -> {
                try {
                    server0.initFixedNetPool(clientIds.length, 1000);
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    e.printStackTrace();
                }
            });
            executorService.execute(() -> {
                try {
                    server1.initFixedNetPool(clientIds.length, 1000);
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    e.printStackTrace();
                }
            });
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        client0.initSelfExistence(1000);
                    } catch (InterruptedException | TimeoutException | ExecutionException | IOException e) {
                        e.printStackTrace();
                    }
                }
            });
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        client1.initSelfExistence(1000);
                    } catch (InterruptedException | TimeoutException | ExecutionException | IOException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }

    public static class paxosPhaseTest{
        static Proposer<Integer> proposer0 = getProposer(netServiceTest.serverPorts[0][0], netServiceTest.serverPorts[0][1]);
        static Proposer<Integer> proposer1 = getProposer(netServiceTest.serverPorts[1][0], netServiceTest.serverPorts[1][1]);
        static Acceptor<Integer> acceptor0 = getAcceptor(4, netServiceTest.clientPorts[0][0], netServiceTest.clientPorts[0][1]);
        static Acceptor<Integer> acceptor1 = getAcceptor(5, netServiceTest.clientPorts[1][0], netServiceTest.clientPorts[1][1]);

        static Runnable paxos0 = () -> {
            ExecutorService exe = Executors.newCachedThreadPool();
            exe.execute(() -> {
                try {
                    proposer0.initNetService(netServiceTest.serverId[0], netServiceTest.clientIds.length, 1000);
                    long pNum = proposer0.newProposalNum();
                    Pair<Long, Integer> stat = proposer0.fetchLatestChosenProposal(pNum, 0, 1000);
                    proposer0.decideCertainProposal(pNum, 0, stat.getValue());
                } catch (IOException | InterruptedException | ExecutionException | TimeoutException e) {
                    e.printStackTrace();
                }
            });
            exe.execute(() -> {
                try {
                    acceptor0.initNetService2Proposer(netServiceTest.clientIds[0], netServiceTest.serverRegIP, 1000);
                    acceptor0.workingOnCertainIssue(0);
                } catch (IOException | InterruptedException | ExecutionException | TimeoutException e) {
                    e.printStackTrace();
                }
            });
            exe.shutdown();
        };

        static Runnable paxos1 = () -> {
            ExecutorService exe = Executors.newCachedThreadPool();
            exe.execute(() -> {
                try {
                    proposer1.initNetService(netServiceTest.serverId[1], netServiceTest.clientIds.length, 1000);
                } catch (IOException | InterruptedException | ExecutionException | TimeoutException e) {
                    e.printStackTrace();
                }
            });
            exe.execute(() -> {
                try {
                    acceptor1.initNetService2Proposer(netServiceTest.clientIds[1], netServiceTest.serverRegIP, 1000);
                    acceptor1.workingOnCertainIssue(0);
                } catch (IOException | InterruptedException | ExecutionException | TimeoutException e) {
                    e.printStackTrace();
                }
            });
            exe.shutdown();
        };

        public static Proposer<Integer> getProposer(int localRegPort, int localComPort){
            Proposer<Integer> p = new Proposer<>();
            p.setLocalRegPort(localRegPort);
            p.setLocalComPort(localComPort);

            return p;
        }

        public static Acceptor<Integer> getAcceptor(Integer init, int localRegPort, int localComPort){
            Acceptor<Integer> a = new Acceptor<>();
            a.setInitDecision(init);
            a.setLocalRegPort(localRegPort);
            a.setLocalComPort(localComPort);

            return a;
        }

        public static void test0(){
            ExecutorService executorService = Executors.newCachedThreadPool();
            executorService.execute(paxos0);
            executorService.execute(paxos1);
            executorService.shutdown();
        }
    }

    public static Logger logger;
    static {
        logger = Logger.getLogger(demo.class);
        BasicConfigurator.configure();
    }

    static class NetServiceTesting{
        static String[] addr = {"localhost", "localhost", "localhost", "localhost", "localhost"};
        static int[] port = {14231, 14251, 14271, 15231, 15251};

        static void test0(){
            ExecutorService service = Executors.newCachedThreadPool();

            for (int i = 0; i < addr.length; i++) {
                int id = i;
                service.execute(() -> {
                    GenericNetService netService = new GenericNetService(id, new ArrayBlockingQueue(10), new ArrayBlockingQueue<>(10));
                    try {
                        netService.connect(addr, port);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });
            }

            service.shutdown();
        }
    }

    public static void main(String[] args) {
        NetServiceTesting.test0();
    }
}
