package agent;

import com.sun.istack.internal.NotNull;
import javafx.util.Pair;
import network.demo;
import network.message.ComPaxosMessage;
import network.message.PaxosMessageFactory;
import network.message.RegPaxosMessage;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

/**
 * @author : Swimiltylers
 * @version : 2019/1/3 10:16
 */
public class Acceptor<Proposal> {
    private static Logger logger = Logger.getLogger(Acceptor.class);

    private String aname;
    private DatagramSocket acom;

    public static final int ACOM_BUFFER_SIZE = 1024;
    public static final int AREG_BUFFER_SIZE = ACOM_BUFFER_SIZE;
    private final byte[] acomBuffer;

    private Set<Pair<InetAddress, Integer>> proposers;
    public static final long INVALID_PROPOSAL_NUM = Proposer.INVALID_PROPOSAL_NUM;

    private Pair<Long, Proposal> acceptedProposal;

    public Acceptor(final int localPort, String name) throws SocketException {
        this.aname = name;
        acom = new DatagramSocket(localPort);
        acomBuffer = new byte[ACOM_BUFFER_SIZE];
    }

    @Override
    protected void finalize() throws Throwable {
        acom.close();
        super.finalize();
    }

    public Set<Pair<InetAddress, Integer>> initSelfExistence(
            final int epoch, final int waitIntervalMillis, final int expireMillis, @NotNull Set<Pair<InetAddress, Integer>> dest) throws InterruptedException, TimeoutException, ExecutionException {

        // TODO: paxos 通信格式存在问题
        RegPaxosMessage reg = new RegPaxosMessage();
        byte[] buffer = PaxosMessageFactory.writeToBytes(aname, reg);

        for (int i = 0; i < epoch; i++) {
            dest.forEach(k-> {
                try {
                    acom.send(new DatagramPacket(buffer, buffer.length, k.getKey(), k.getValue()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            Thread.sleep(waitIntervalMillis);
        }

        DatagramPacket rpack = new DatagramPacket(new byte[AREG_BUFFER_SIZE], AREG_BUFFER_SIZE);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<Set<Pair<InetAddress, Integer>>> ret = executorService.submit(new Callable<Set<Pair<InetAddress, Integer>>>() {
            @Override
            public Set<Pair<InetAddress, Integer>> call() throws Exception {
                int expectedResponseNum = dest.size();
                Set<String> responderName = new HashSet<>();
                List<Pair<InetAddress, Integer>> responderAddr = new ArrayList<>();

                while (expectedResponseNum > responderAddr.size()){
                    acom.receive(rpack);
                    RegPaxosMessage msg = (RegPaxosMessage)PaxosMessageFactory.readFromPacket(rpack);

                    if (!responderName.contains(msg.getSenderName())){
                        responderName.add(msg.getSenderName());
                        responderAddr.add(new Pair<>(rpack.getAddress(), rpack.getPort()));
                    }
                }

                return new HashSet<>(responderAddr);
            }
        });

        Set<Pair<InetAddress, Integer>> responders = null;

        try {
            responders = ret.get(expireMillis, TimeUnit.MILLISECONDS);
        } finally {
            executorService.shutdown();
        }

        return responders;
    }

    protected synchronized Pair<Long, Proposal> getLatestAcceptedProposal() throws InterruptedException {
        // TODO: need to check PriorityBlockingQueue
        if (acceptedProposal == null)
            return null;
        else
            return acceptedProposal;
    }

    protected synchronized void acceptProposal(long pNum, Proposal correspondProposal){
        // TODO: need to check PriorityBlockingQueue
        acceptedProposal = new Pair<>(pNum, correspondProposal);
    }

    protected class NonParallelWorkingThread implements Runnable{

        @Override
        public void run() {
            DatagramPacket apack = new DatagramPacket(acomBuffer, acomBuffer.length);

            long currentProposalNum = INVALID_PROPOSAL_NUM;

            while (true){
                if (Thread.interrupted())
                    break;
                else{
                    try {
                        acom.receive(apack);
                        ComPaxosMessage<Proposal> msg = (ComPaxosMessage<Proposal>)PaxosMessageFactory.readFromPacket(apack);

                        if (currentProposalNum <= msg.getProposalNum()){
                            currentProposalNum = msg.getProposalNum();
                            if (msg.getType() == ComPaxosMessage.ComPaxosMessageType.PREPARE){
                                // TODO: paxos 通讯协议不成熟
                                Pair<Long, Proposal> latestAcceptedProposal = getLatestAcceptedProposal();
                                ComPaxosMessage<Proposal> response = new ComPaxosMessage<>();
                                if (latestAcceptedProposal != null) {
                                    response.setMaxChosenProposalNum(latestAcceptedProposal.getKey());
                                    response.setProposal(latestAcceptedProposal.getValue());
                                }
                                else{
                                    response.setMaxChosenProposalNum(INVALID_PROPOSAL_NUM);
                                }
                                byte[] bresponse = PaxosMessageFactory.writeToBytes(aname, response);
                                acom.send(new DatagramPacket(bresponse, bresponse.length, apack.getAddress(), apack.getPort()));
                            }
                            else if (msg.getType() == ComPaxosMessage.ComPaxosMessageType.ACCEPT){
                                acceptProposal(msg.getProposalNum(), msg.getProposal());
                            }
                        }
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public void initSingleWorkingThread(){
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            executorService.submit(new NonParallelWorkingThread());
        } finally {
            executorService.shutdown();
        }
    }
}
