import agent.SingleAcceptor;
import agent.SingleProposer;
import client.FileIteratorClient;
import instance.PaxosInstance;
import instance.store.InstanceStore;
import instance.store.OffsetIndexStore;
import javafx.util.Pair;
import logger.NaiveLogger;
import network.service.GenericNetService;
import network.service.ObjectUdpNetService;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import rsm.BasicPaxosSMR;
import rsm.DiskPaxosSMR;
import rsm.GenericPaxosSMR;

import java.io.IOException;
import java.net.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
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
        static SingleProposer<Integer> proposer0 = getProposer(netServiceTest.serverPorts[0][0], netServiceTest.serverPorts[0][1]);
        static SingleProposer<Integer> proposer1 = getProposer(netServiceTest.serverPorts[1][0], netServiceTest.serverPorts[1][1]);
        static SingleAcceptor<Integer> acceptor0 = getAcceptor(4, netServiceTest.clientPorts[0][0], netServiceTest.clientPorts[0][1]);
        static SingleAcceptor<Integer> acceptor1 = getAcceptor(5, netServiceTest.clientPorts[1][0], netServiceTest.clientPorts[1][1]);

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

        public static SingleProposer<Integer> getProposer(int localRegPort, int localComPort){
            SingleProposer<Integer> p = new SingleProposer<>();
            p.setLocalRegPort(localRegPort);
            p.setLocalComPort(localComPort);

            return p;
        }

        public static SingleAcceptor<Integer> getAcceptor(Integer init, int localRegPort, int localComPort){
            SingleAcceptor<Integer> a = new SingleAcceptor<>();
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

        static void emit(ExecutorService service, int id){
            service.execute(() -> {
                GenericNetService netService = new GenericNetService(id, GenericNetService.DEFAULT_TO_CLIENT_PORT, new ArrayBlockingQueue<>(10), new ArrayBlockingQueue<>(10), new NaiveLogger(0));
                try {
                    netService.connect(addr, port);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }

        static void test0(){
            ExecutorService service = Executors.newCachedThreadPool();
            for (int i = 0; i < addr.length; i++)
                emit(service, i);

            service.shutdown();
        }

        static void test1(){
            ExecutorService service = Executors.newCachedThreadPool();
            Random random = new Random();
            for (int i = 0; i < addr.length; i++) {
                emit(service, i);
                try {
                    Thread.sleep(random.nextInt(50000)+10000);
                } catch (InterruptedException ignored) {}
            }

            service.shutdown();
        }

        static void test2(){
            ExecutorService service = Executors.newCachedThreadPool();

            for (int i = 0; i < addr.length; i++) {
                int id = i;
                service.execute(() -> {
                    GenericNetService netService = new GenericNetService(id, GenericNetService.DEFAULT_TO_CLIENT_PORT, new ArrayBlockingQueue<>(10), new ArrayBlockingQueue<>(10), new NaiveLogger(0));
                    try {
                        netService.connect(addr, port);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    if (id == 0){
                        netService.getPeerMessageSender().broadcastPeerMessage("world");
                        netService.getPeerMessageSender().broadcastPeerMessage("peer");
                    }
                    else {
                        netService.getPeerMessageSender().sendPeerMessage(0, "hello");
                    }
                });
            }
        }
    }

    static class GenericPaxosSMRTesting{
        static void emit(ExecutorService service, int id){
            GenericPaxosSMR rsm = new BasicPaxosSMR(id, NetServiceTesting.addr, NetServiceTesting.port);
            service.execute(rsm);
        }

        static void test0(){
            ExecutorService service = Executors.newCachedThreadPool();
            for (int i = 0; i < NetServiceTesting.addr.length; i++) {
                emit(service, i);
            }
            FileIteratorClient client = new FileIteratorClient("kepas");
            try {
                client.connect("localhost", GenericNetService.DEFAULT_TO_CLIENT_PORT);
            } catch (IOException e) {
                return;
            }
            service.execute(client);
        }
    }

    static class DiskPaxosSMRTesting{
        static void emit(ExecutorService service, int id){
            GenericPaxosSMR rsm = new DiskPaxosSMR(id, NetServiceTesting.addr, NetServiceTesting.port);
            service.execute(rsm);
        }

        static void test0(){
            ExecutorService service = Executors.newCachedThreadPool();
            for (int i = 0; i < NetServiceTesting.addr.length; i++) {
                emit(service, i);
            }
            FileIteratorClient client = new FileIteratorClient("eros");
            try {
                client.connect("localhost", GenericNetService.DEFAULT_TO_CLIENT_PORT);
            } catch (IOException e) {
                return;
            }
            service.execute(client);
        }

        static void test1(){
            test0();
        }

        static void test2(){
            OffsetIndexStore store = new OffsetIndexStore("disk-4");

            PaxosInstance fetch_0 = store.fetch(0, 0);
            PaxosInstance fetch_1 = store.fetch(0, 1);
            PaxosInstance fetch_2 = store.fetch(0, 2);

            System.out.println(Arrays.toString(fetch_0.cmds));
            System.out.println(Arrays.toString(fetch_1.cmds));
            System.out.println(Arrays.toString(fetch_2.cmds));
        }
    }

    static class OffsetIndexStoreTesting{
        static InstanceStore store = new OffsetIndexStore("demo");
        static PaxosInstance inst_0;
        static PaxosInstance inst_1;
        static PaxosInstance inst_2;

        static {
            inst_0 = new PaxosInstance();
            inst_0.crtInstBallot = 0;

            inst_1 = new PaxosInstance();
            inst_1.crtInstBallot = 1;

            inst_2 = new PaxosInstance();
            inst_2.crtInstBallot = 2;
        }

        static void test0(){
            System.out.println(store.isExist(0,0));
        }

        static void test1(){
            store.store(0, 0, new PaxosInstance());
            System.out.println(store.isExist(0,0));
        }

        static void test2(){
            store.store(0, 0, inst_0);
            store.store(0, 1, inst_1);
            store.store(0, 2, inst_2);

            System.out.println(store.isExist(0, 0));
            System.out.println(store.isExist(0, 4));
            System.out.println(store.isExist(1, 2));

            test4();
        }

        static void test3(){
            PaxosInstance instance = new PaxosInstance();
            instance.crtInstBallot = 10;

            System.out.println(store.isExist(0, 2));

            store.store(0, 1, instance);

            test4();
        }

        static void test4(){
            PaxosInstance fetch_0 = store.fetch(0, 0);
            PaxosInstance fetch_1 = store.fetch(0, 1);
            PaxosInstance fetch_2 = store.fetch(0, 2);

            System.out.println(fetch_0.crtInstBallot+"\t"+fetch_1.crtInstBallot+"\t"+fetch_2.crtInstBallot);
        }
    }

    public static void main(String[] args) {
        //NetServiceTesting.test2();
        //GenericPaxosSMRTesting.test0();
        DiskPaxosSMRTesting.test1();
        //OffsetIndexStoreTesting.test4();

        //Date date = new Date();
        //final String format = "[%tF %<tT:%<tL %<tz][p%08d][%s][leaderId=%d, inst_no=%d][\"%s\"]%n";
        //System.out.print(String.format(format, date, 0, "PREPARE", 0, 0, "send"));
    }
}
