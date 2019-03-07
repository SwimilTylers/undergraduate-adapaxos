import client.FileIteratorClient;
import instance.PaxosInstance;
import instance.store.InstanceStore;
import instance.store.OffsetIndexStore;
import logger.NaiveLogger;
import network.service.GenericNetService;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import rsm.BasicPaxosRSM;
import rsm.DiskPaxosRSM;
import rsm.GenericPaxosSMR;

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
