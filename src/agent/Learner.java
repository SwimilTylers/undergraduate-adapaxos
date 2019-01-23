package agent;

import com.sun.istack.internal.NotNull;
import javafx.util.Pair;
import network.message.protocols.PaxosProposalProtocol;
import network.message.protocols.PaxosTimestampedProposalProtocol;
import network.service.NetService;
import network.service.ObjectUdpNetService;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author : Swimiltylers
 * @version : 2019/1/3 10:16
 */
public class Learner<Proposal> {
    private static Logger logger = Logger.getLogger(Proposer.class);
    public static final int DEFAULT_LEARNER_INFOREG_PORT = 40009;
    public static final int DEFAULT_LEARNER_INFOCOM_PORT = 40019;

    private String m_agentName;
    private int m_acceptorSize;
    private NetService<PaxosTimestampedProposalProtocol> m_netService2Acceptor;

    private int m_localInfoRegPort = DEFAULT_LEARNER_INFOREG_PORT;
    private int m_localInfoComPort = DEFAULT_LEARNER_INFOCOM_PORT;

    public void initNetService2Acceptor(@NotNull String netId, int acceptorNum, int expireMillis)
            throws IOException, InterruptedException, ExecutionException, TimeoutException {
        m_agentName = netId;
        m_acceptorSize = acceptorNum;

        ObjectUdpNetService.Server<PaxosTimestampedProposalProtocol> netService =
                new ObjectUdpNetService.Server<>(netId, m_localInfoRegPort, m_localInfoComPort);
        netService.initFixedNetPool(acceptorNum, expireMillis);

        m_netService2Acceptor = netService;
    }

    public boolean ifSufficient(AcceptedKnowledge knowledge){
        return knowledge.size() > m_acceptorSize/2 && knowledge.diverse() == 1;
    }

    class AcceptedKnowledge{
        private Map<Proposal, Integer> kMap = new HashMap<>();
        private Proposal lastProposal = null;
        private Set<String> source = new HashSet<>();

        void add(String source_name, @NotNull Proposal proposal){
            if (!source.contains(source_name)){
                source.add(source_name);
                lastProposal = proposal;
                if (kMap.containsKey(proposal)){
                    int old = kMap.get(proposal);
                    kMap.replace(proposal, old + 1);
                }
                else
                    kMap.put(proposal, 1);
            }
        }

        int size(){
            return source.size();
        }

        int diverse() {
            return kMap.size();
        }

        public Proposal getLastProposal() {
            return lastProposal;
        }
    }

    public Pair<Long, Proposal> waitingForConsensus(long iNum, int expireMillis) throws InterruptedException, ExecutionException, TimeoutException {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<Pair<Long, AcceptedKnowledge>> ret = executorService.submit(() -> {
            Map<Long, AcceptedKnowledge> totalKMap = new HashMap<>();
            Pair<Long, AcceptedKnowledge> kSum = null;

            while (kSum == null){
                PaxosTimestampedProposalProtocol msg = m_netService2Acceptor.getArrivalObject().getKey();
                if (msg.getIssueNum() == iNum){
                    if (msg.getProposalType() == PaxosProposalProtocol.PROPOSAL_TYPE.PROPOSAL_ACCEPTED){
                        Proposal proposal = PaxosTimestampedProposalProtocol.resoluteAccepted(msg);
                        if (!totalKMap.containsKey(msg.getPNum()))
                            totalKMap.put(msg.getPNum(), new AcceptedKnowledge());

                        AcceptedKnowledge rKnow = totalKMap.get(msg.getPNum());
                        rKnow.add(msg.getSenderName(), proposal);
                        if (ifSufficient(rKnow))
                            kSum = new Pair<>(msg.getPNum(), rKnow);
                    }
                }
                else{
                    // TODO: iNum 不同如何处理
                    assert msg.getIssueNum() == iNum;
                }
            }

            return kSum;
        });

        Pair<Long, AcceptedKnowledge> knowledge = null;

        try {
            knowledge = ret.get(expireMillis, TimeUnit.MILLISECONDS);
        } finally {
            executorService.shutdown();
        }

        return new Pair<>(knowledge.getKey(), knowledge.getValue().getLastProposal());
    }

    public int getLocalInfoRegPort() {
        return m_localInfoRegPort;
    }

    public void setLocalInfoRegPort(int m_localInfoRegPort) {
        this.m_localInfoRegPort = m_localInfoRegPort;
    }

    public int getLocalInfoComPort() {
        return m_localInfoComPort;
    }

    public void setLocalInfoComPort(int m_localInfoComPort) {
        this.m_localInfoComPort = m_localInfoComPort;
    }
}
