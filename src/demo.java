import client.FileIteratorClient;
import instance.StaticPaxosInstance;
import instance.store.InstanceStore;
import instance.store.OffsetIndexStore;
import instance.store.PseudoRemoteInstanceStore;
import instance.store.TaggedOffsetIndexStore;
import logger.NaiveLogger;
import network.service.GenericNetService;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import rsm.AdaPaxosRSM;
import rsm.BasicPaxosRSM;
import rsm.DiskPaxosRSM;
import rsm.GenericPaxosSMR;
import utils.AdaPaxosParameters;
import utils.NetworkConfiguration;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.*;

// TODO: logger is not complete

/**
 * Created with IntelliJ IDEA.
 * User: Swimiltylers
 * Date: 2018/12/21
 * Time: 23:45
 */
public class demo {
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
                        netService.getPeerMessageSender().broadcastPeerMessage("sender");
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
            GenericPaxosSMR rsm = new BasicPaxosRSM(id, NetServiceTesting.addr, NetServiceTesting.port);
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
            GenericPaxosSMR rsm = new DiskPaxosRSM(id, NetServiceTesting.addr, NetServiceTesting.port);
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

            StaticPaxosInstance fetch_0 = (StaticPaxosInstance) store.fetch(0, 0);
            StaticPaxosInstance fetch_1 = (StaticPaxosInstance) store.fetch(0, 1);
            StaticPaxosInstance fetch_2 = (StaticPaxosInstance) store.fetch(0, 2);

            System.out.println(Arrays.toString(fetch_0.requests));
            System.out.println(Arrays.toString(fetch_1.requests));
            System.out.println(Arrays.toString(fetch_2.requests));
        }
    }

    static class OffsetIndexStoreTesting{
        static InstanceStore store = new OffsetIndexStore("demo");
        static StaticPaxosInstance inst_0;
        static StaticPaxosInstance inst_1;
        static StaticPaxosInstance inst_2;

        static {
            inst_0 = new StaticPaxosInstance();
            inst_0.crtInstBallot = 0;

            inst_1 = new StaticPaxosInstance();
            inst_1.crtInstBallot = 1;

            inst_2 = new StaticPaxosInstance();
            inst_2.crtInstBallot = 2;
        }

        static void test0(){
            System.out.println(store.isExist(0,0));
        }

        static void test1(){
            store.store(0, 0, new StaticPaxosInstance());
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
            StaticPaxosInstance instance = new StaticPaxosInstance();
            instance.crtInstBallot = 10;

            System.out.println(store.isExist(0, 2));

            store.store(0, 1, instance);

            test4();
        }

        static void test4(){
            StaticPaxosInstance fetch_0 = (StaticPaxosInstance) store.fetch(0, 0);
            StaticPaxosInstance fetch_1 = (StaticPaxosInstance) store.fetch(0, 1);
            StaticPaxosInstance fetch_2 = (StaticPaxosInstance) store.fetch(0, 2);

            System.out.println(fetch_0.crtInstBallot+"\t"+fetch_1.crtInstBallot+"\t"+fetch_2.crtInstBallot);
        }
    }

    static class Synchronized{
        public synchronized void method1(){
            System.out.println("Method 1 start");
            try {
                System.out.println("Method 1 execute");
                method3();
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Method 1 end");
        }

        public synchronized void method2(){
            System.out.println("Method 2 start");
            try {
                System.out.println("Method 2 execute");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Method 2 end");
        }

        public synchronized void method3(){
            System.out.println("Method 3 start");
            try {
                System.out.println("Method 3 execute");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Method 3 end");
        }

        public static void main(String[] args) {
            final Synchronized test = new Synchronized();

            new Thread(test::method1).start();

            new Thread(test::method2).start();
        }
    }

    public static class AdaPaxosRSMTesting{
        static InstanceStore[] stores = new InstanceStore[]{
                new TaggedOffsetIndexStore(AdaPaxosParameters.RSM.DEFAULT_LOCAL_STORAGE_PREFIX+0),
                new TaggedOffsetIndexStore(AdaPaxosParameters.RSM.DEFAULT_LOCAL_STORAGE_PREFIX+1),
                new TaggedOffsetIndexStore(AdaPaxosParameters.RSM.DEFAULT_LOCAL_STORAGE_PREFIX+2),
                new TaggedOffsetIndexStore(AdaPaxosParameters.RSM.DEFAULT_LOCAL_STORAGE_PREFIX+3),
                new TaggedOffsetIndexStore(AdaPaxosParameters.RSM.DEFAULT_LOCAL_STORAGE_PREFIX+4)
        };

        static {
            //deleteDir("store");
        }

        static boolean deleteDir(String dirPath){
            File file = new File(dirPath);

            if (file.exists()) {
                if (file.isFile())
                    return file.delete();
                else {
                    File[] files = file.listFiles();
                    if (files == null)
                        return file.delete();
                    else {
                        for (File value : files) deleteDir(value.getAbsolutePath());
                        return file.delete();
                    }
                }
            }
            else {
                return true;
            }
        }

        static NetworkConfiguration netConfig = new NetworkConfiguration(NetServiceTesting.addr, NetServiceTesting.port, 0);

        static void test0(){
            ExecutorService service = Executors.newCachedThreadPool();
            for (int i = 0; i < 5; i++) {
                int serverId = i;
                service.execute(() -> {
                    try {
                        AdaPaxosRSM rsm = AdaPaxosRSM.makeInstance(serverId, 0, 5,
                                new PseudoRemoteInstanceStore(serverId, stores, AdaPaxosParameters.RSM.DEFAULT_INSTANCE_SIZE),
                                new GenericNetService(serverId),
                                serverId == 0
                        );
                        rsm.link(netConfig, 4470+serverId*2);
                        rsm.agent();
                        rsm.routine();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }

            FileIteratorClient client = new FileIteratorClient("tic");
            try {
                client.connect("localhost", 4470);
            } catch (IOException e) {
                e.printStackTrace();
            }
            service.execute(client);
        }

        static void test1(int totalNum){
            ExecutorService service = Executors.newCachedThreadPool();
            for (int i = 0; i < totalNum; i++) {
                int serverId = i;
                service.execute(() -> {
                    try {
                        AdaPaxosRSM rsm = AdaPaxosRSM.makeInstance(serverId, 0, 5,
                                new PseudoRemoteInstanceStore(serverId, stores, AdaPaxosParameters.RSM.DEFAULT_INSTANCE_SIZE),
                                new GenericNetService(serverId),
                                serverId == 0
                        );
                        rsm.link(new NetworkConfiguration(Arrays.copyOfRange(NetServiceTesting.addr, 0, totalNum), Arrays.copyOfRange(NetServiceTesting.port, 0, totalNum), 0), 4470+serverId*2);
                        rsm.agent();
                        rsm.routine();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }
        }

        static void test2(){
            ExecutorService service = Executors.newCachedThreadPool();
            for (int i = 0; i < 5; i++) {
                int serverId = i;
                service.execute(() -> {
                    try {
                        AdaPaxosRSM rsm = AdaPaxosRSM.makeInstance(serverId, 0, 5,
                                new PseudoRemoteInstanceStore(serverId, stores, AdaPaxosParameters.RSM.DEFAULT_INSTANCE_SIZE),
                                new GenericNetService(serverId),
                                serverId == 0
                        );
                        rsm.link(netConfig, 4470+serverId*2);
                        rsm.agent();
                        if (serverId != 0)
                            rsm.routine();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }
        }
    }

    public static void main(String[] args) {
        //NetServiceTesting.test2();
        //GenericPaxosSMRTesting.test0();
        //DiskPaxosSMRTesting.test1();
        //AdaPaxosRSMTesting.deleteDir("store");
        //AdaPaxosRSMTesting.test0();
        AdaPaxosRSMTesting.test1(5);
        //AdaPaxosRSMTesting.test2();
        //OffsetIndexStoreTesting.test4();
        //Synchronized.main(args);
        //Date date = new Date();
        //final String format = "[%tF %<tT:%<tL %<tz][p%08d][%s][leaderId=%d, inst_no=%d][\"%s\"]%n";
        //System.out.print(String.format(format, date, 0, "PREPARE", 0, 0, "send"));
    }
}
