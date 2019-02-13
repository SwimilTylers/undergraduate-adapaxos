package agent;

import com.sun.istack.internal.NotNull;
import javafx.util.Pair;
import network.message.protocols.PaxosProposalProtocol;
import network.message.protocols.PaxosTimestampedProposalProtocol;
import network.service.NetService;
import network.service.ObjectUdpNetService;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.*;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.*;

/**
 * @author : Swimiltylers
 * @version : 2019/1/3 10:16
 */
@Deprecated
public class Acceptor<Proposal> {
    private static Logger logger = Logger.getLogger(Proposer.class);
    public static final int DEFAULT_ACCEPTOR_REG_PORT = 7501;
    public static final int DEFAULT_ACCEPTOR_COM_PORT = 5578;

    public static final int DEFAULT_ACCEPTOR_INFOREG_PORT = 40010;
    public static final int DEFAULT_ACCEPTOR_INFOCOM_PORT = 40020;

    private volatile String m_agentName;
    private NetService<PaxosTimestampedProposalProtocol> m_netService2Proposer;
    private NetService<PaxosTimestampedProposalProtocol> m_netService2Learner;

    /* These parameters configure proposer-acceptor communication */
    private int m_localRegPort = DEFAULT_ACCEPTOR_REG_PORT;
    private int m_localComPort = DEFAULT_ACCEPTOR_COM_PORT;

    /* These parameters configure learner-acceptor communication */
    private int m_localInfoRegPort = DEFAULT_ACCEPTOR_INFOREG_PORT;
    private int m_localInfoComPort = DEFAULT_ACCEPTOR_INFOCOM_PORT;

    private PriorityQueue<Pair<Long, Proposal>> proposalHistory = new PriorityQueue<>((a,b)->b.getKey().compareTo(a.getKey()));

    private Proposal m_initDecision;

    public Acceptor(){}

    public Acceptor(Proposal initDecision){
        m_initDecision = initDecision;
    }

    private NetService<PaxosTimestampedProposalProtocol> initNetServiceInternal(
            @NotNull String netId, @NotNull Set<Pair<InetAddress, Integer>> regNetPool,
            int regPort, int comPort, int expireMillis)
            throws InterruptedException, ExecutionException, TimeoutException, IOException {
        // TODO: 多线程不安全！
        m_agentName = netId;

        ObjectUdpNetService.Client<PaxosTimestampedProposalProtocol> netService =
                new ObjectUdpNetService.Client<>(netId, regPort, comPort);
        netService.setRegNetPool(regNetPool);
        netService.initSelfExistence(expireMillis);

        return netService;
    }

    public void initNetService2Proposer(@NotNull String netId, @NotNull Set<Pair<InetAddress, Integer>> regProposerNetPool, int expireMillis)
            throws IOException, InterruptedException, ExecutionException, TimeoutException {
        m_netService2Proposer = initNetServiceInternal(netId, regProposerNetPool, m_localRegPort, m_localComPort, expireMillis);
    }

    public void initNetService2Learner(@NotNull String netId, @NotNull Set<Pair<InetAddress, Integer>> regLearnerNetPool, int expireMillis)
            throws IOException, InterruptedException, ExecutionException, TimeoutException {
        m_netService2Learner = initNetServiceInternal(netId, regLearnerNetPool, m_localInfoRegPort, m_localInfoComPort, expireMillis);
    }

    public void workingOnCertainIssue(long iNum){
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            executorService.execute(() -> {
                long maxPNum = proposalHistory.isEmpty() ? PaxosTimestampedProposalProtocol.PNUM_NO_SUCH_HISTORY : proposalHistory.peek().getKey();
                while (!Thread.interrupted()) {
                    try {
                        Pair<PaxosTimestampedProposalProtocol, Pair<InetAddress, Integer>> info = m_netService2Proposer.getArrivalObject();
                        PaxosTimestampedProposalProtocol msg = info.getKey();

                        if (msg.getIssueNum() == iNum) {
                            if (msg.getPNum() >= maxPNum) {
                                maxPNum = msg.getPNum();
                                if (msg.getProposalType() == PaxosProposalProtocol.PROPOSAL_TYPE.PROPOSAL_PREPARE) {
                                    Pair<Long, Proposal> ack = proposalHistory.isEmpty()
                                            ? new Pair<>(PaxosTimestampedProposalProtocol.PNUM_NO_SUCH_HISTORY, m_initDecision)
                                            : proposalHistory.peek();
                                    PaxosTimestampedProposalProtocol reply = PaxosTimestampedProposalProtocol.makeAck(
                                            m_agentName,
                                            msg.getPNum(),
                                            iNum,
                                            ack.getKey(),
                                            ack.getValue()
                                    );
                                    m_netService2Proposer.putDepartureObject(reply, info.getValue().getKey(), info.getValue().getValue());
                                }
                                else if (msg.getProposalType() == PaxosProposalProtocol.PROPOSAL_TYPE.PROPOSAL_ACCEPT) {
                                    Proposal chosenOne = PaxosTimestampedProposalProtocol.resoluteAccept(msg);
                                    proposalHistory.add(new Pair<>(msg.getPNum(), chosenOne));
                                    if (m_netService2Learner != null){
                                        PaxosTimestampedProposalProtocol accepted = PaxosTimestampedProposalProtocol.makeAccepted(
                                                m_agentName,
                                                msg.getPNum(),
                                                iNum,
                                                chosenOne
                                        );
                                        m_netService2Learner.putBroadcastObject(accepted);
                                    }
                                }
                            }
                            /* upon receiving a deprecated proposal */
                            else {
                                PaxosTimestampedProposalProtocol warning = PaxosTimestampedProposalProtocol.makeKill(
                                        m_agentName,
                                        maxPNum,
                                        iNum
                                );
                                m_netService2Proposer.putDepartureObject(warning, info.getValue().getKey(), info.getValue().getValue());
                            }
                        } else {
                            // TODO: iNum不同未处理
                            assert msg.getIssueNum() == iNum;
                        }
                    } catch (IOException | ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            });
        } finally {
            executorService.shutdown();
        }
    }

    public void setInitDecision(Proposal initDecision) {
        this.m_initDecision = initDecision;
    }

    public Proposal getInitDecision() {
        return m_initDecision;
    }

    public int getLocalRegPort() {
        return m_localRegPort;
    }

    public int getLocalComPort() {
        return m_localComPort;
    }

    public int getLocalInfoRegPort() {
        return m_localInfoRegPort;
    }

    public int getLocalInfoComPort() {
        return m_localInfoComPort;
    }

    public void setLocalRegPort(int localRegPort) {
        this.m_localRegPort = localRegPort;
    }

    public void setLocalComPort(int localComPort) {
        this.m_localComPort = localComPort;
    }

    public void setLocalInfoRegPort(int localInfoRegPort) {
        this.m_localInfoRegPort = localInfoRegPort;
    }

    public void setLocalInfoComPort(int localInfoComPort) {
        this.m_localInfoComPort = localInfoComPort;
    }
}
