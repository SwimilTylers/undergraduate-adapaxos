package agent;

import javafx.util.Pair;
import network.message.ComPaxosMessage;
import network.message.PaxosMessageFactory;
import network.message.RegPaxosMessage;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

/**
 * @author : Swimiltylers
 * @version : 2019/1/3 10:16
 */
public class Proposer<Proposal> {
    private static Logger logger = Logger.getLogger(Proposer.class);

    private String name;
    private DatagramSocket pcom;

    public static final int PCOM_BUFFER_SIZE = 1024;
    public static final int PREG_BUFFER_SIZE = PCOM_BUFFER_SIZE;
    private final byte[] pcomBuffer;

    private Set<Pair<InetAddress, Integer>> acceptors;
    public static final long INVALID_PROPOSAL_NUM = 0L;

    private boolean ifStart = false;
    private int consensus;


    Proposer(int localComPort, String name) throws SocketException {
        this.name = name;
        pcom = new DatagramSocket(localComPort);
        pcomBuffer = new byte[PCOM_BUFFER_SIZE];
    }

    @Override
    protected void finalize() throws Throwable {
        pcom.close();
        super.finalize();
    }

    public void initFixedNetPool(final int localRegPort, final int acceptorNum, final int expireMillis) throws SocketException, InterruptedException, ExecutionException, TimeoutException {
        DatagramSocket rcom = new DatagramSocket(localRegPort);
        DatagramPacket rpack = new DatagramPacket(new byte[PREG_BUFFER_SIZE], PREG_BUFFER_SIZE);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<Set<Pair<InetAddress, Integer>>> addr = executorService.submit(new Callable<Set<Pair<InetAddress, Integer>>>() {
            @Override
            public Set<Pair<InetAddress, Integer>> call() throws Exception {
                Set<String> acceptorName = new HashSet<>();
                List<Pair<InetAddress, Integer>> acceptorAddr = new ArrayList<>();

                while (acceptorNum < acceptorAddr.size()){
                    rcom.receive(rpack);
                    RegPaxosMessage msg = (RegPaxosMessage)PaxosMessageFactory.readFromPacket(rpack);

                    if (!acceptorName.contains(msg.getSenderName())){
                        acceptorName.add(msg.getSenderName());
                        acceptorAddr.add(msg.getInfo());
                        // TODO: 改用pcom信道，但是传输的信息格式不正确
                        pcom.send(new DatagramPacket("ack".getBytes(), 3));
                    }
                }

                return new HashSet<>(acceptorAddr);
            }
        });

        try {
            acceptors = addr.get(expireMillis, TimeUnit.MILLISECONDS);
        } finally {
            executorService.shutdown();
        }
    }

    public long generateProposalNum(){
        return System.currentTimeMillis();
    }

    public void broadcasting(byte[] rawMessage){
        acceptors.forEach(k-> {
            try {
                pcom.send(new DatagramPacket(rawMessage, rawMessage.length, k.getKey(), k.getValue()));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public Pair<Long, Proposal> fetchLatestChosenProposal(final long currentProposalNum, final int expireMillis)
            throws InterruptedException, ExecutionException, TimeoutException {
        // TODO: paxos 报文格式存在问题
        final byte[] message = PaxosMessageFactory.writeToBytes(name, new ComPaxosMessage());

        broadcasting(message);

        DatagramPacket cpack = new DatagramPacket(pcomBuffer, pcomBuffer.length);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<Pair<Long, Proposal>> ret = executorService.submit(new Callable<Pair<Long, Proposal>>() {
            @Override
            public Pair<Long, Proposal> call() throws Exception {
                long currentMaxPNum = INVALID_PROPOSAL_NUM;
                Proposal currentMaxPNumProposal = null;

                int currentAck = 0;

                while (currentAck < acceptors.size()/2){
                    pcom.receive(cpack);
                    // TODO: paxos 报文读取方式有问题
                    ComPaxosMessage<Proposal> response = (ComPaxosMessage<Proposal>)PaxosMessageFactory.readFromPacket(cpack);
                    if (response.getProposalNum() == currentProposalNum){
                        if (response.getMaxChosenProposalNum() >= currentMaxPNum){
                            currentMaxPNum = response.getMaxChosenProposalNum();
                            currentMaxPNumProposal = response.getProposal();
                        }
                        ++currentAck;
                    }
                }

                return new Pair<>(currentMaxPNum, currentMaxPNumProposal);
            }
        });

        Pair<Long, Proposal> latestChosenProposal = null;
        try {
            latestChosenProposal = ret.get(expireMillis, TimeUnit.MILLISECONDS);
        } finally {
            executorService.shutdown();
        }

        return latestChosenProposal;
    }

    public void decideCertainProposal(final long currentProposalNum, Proposal thisProposal){
        // TODO: paxos 报文格式存在问题
        ComPaxosMessage<Proposal> acceptMessage = new ComPaxosMessage<>();
        acceptMessage.setType(ComPaxosMessage.ComPaxosMessageType.ACCEPT);
        acceptMessage.setProposal(thisProposal);
        final byte[] message = PaxosMessageFactory.writeToBytes(name, acceptMessage);

        broadcasting(message);
    }
}
