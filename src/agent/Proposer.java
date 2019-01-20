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
public class Proposer<Proposal> {
    private static Logger logger = Logger.getLogger(Proposer.class);
    public static final int DEFAULT_PROPOSER_REG_PORT = 7500;
    public static final int DEFAULT_PROPOSER_COM_PORT = 5586;

    private String m_agentName;
    private int m_acceptorSize;
    private NetService<PaxosTimestampedProposalProtocol> m_netService;

    private int m_localRegPort = DEFAULT_PROPOSER_REG_PORT;
    private int m_localComPort = DEFAULT_PROPOSER_COM_PORT;

    public void initNetService(@NotNull String netId, int acceptorNum, int expireMillis)
            throws IOException, InterruptedException, ExecutionException, TimeoutException {
        m_agentName = netId;
        m_acceptorSize = acceptorNum;

        ObjectUdpNetService.Server<PaxosTimestampedProposalProtocol> netService =
                new ObjectUdpNetService.Server<>(netId, m_localRegPort, m_localComPort);
        netService.initFixedNetPool(acceptorNum, expireMillis);

        m_netService = netService;
    }

    public long newProposalNum(){
        return System.currentTimeMillis();
    }

    public boolean ifSufficient(Set<String> recv_acceptors){
        return recv_acceptors.size() > m_acceptorSize/2;
    }

    public Pair<Long, Proposal> fetchLatestChosenProposal(final long pNum, final long iNum, final int expireMillis)
            throws InterruptedException, ExecutionException, TimeoutException, IOException {

        /* sending PREPARE<pNum> */

        m_netService.putBroadcastObject(PaxosTimestampedProposalProtocol.makePrepare(m_agentName, pNum, iNum));

        /* receiving ACK<pNum, vPNum, v>
        *  waiting for responses from the majority of acceptors
        * */

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<List<Pair<Long, Proposal>>> ret = executorService.submit(() -> {
            List<Pair<Long, Proposal>> acks = new ArrayList<>();
            Set<String> acceptors = new HashSet<>();

            while (!ifSufficient(acceptors)){
                PaxosTimestampedProposalProtocol ack = m_netService.getArrivalObject().getKey();

                if (ack.getIssueNum() == iNum){
                    if (ack.getPNum() > pNum){
                        acks.clear();
                        break;
                    }
                    else if(ack.getPNum() == pNum
                            && ack.getProposalType() == PaxosProposalProtocol.PROPOSAL_TYPE.PROPOSAL_ACK
                            && !acceptors.contains(ack.getSenderName())){
                        acceptors.add(ack.getSenderName());
                        Pair<Long, Proposal> info = PaxosTimestampedProposalProtocol.resoluteAck(ack);
                        acks.add(info);
                    }
                }
                else{
                    // TODO: iNum 不同的情况下的处理方式
                    assert ack.getIssueNum() == iNum;
                }
            }

            return acks;
        });

        List<Pair<Long, Proposal>> acks = null;
        try {
             acks = ret.get(expireMillis, TimeUnit.MILLISECONDS);
        } finally {
            executorService.shutdown();
        }


        /* this proposal is deprecated, due to receiving PROPOSAL_KILL message */
        if (acks.isEmpty())
            return null;
        /* find the latest chosen proposal */
        else
            return Collections.max(acks, (a,b)->b.getKey().compareTo(a.getKey()));
    }

    public void decideCertainProposal(final long pNum, final long iNum, Proposal decision) throws IOException {
        PaxosTimestampedProposalProtocol msg = PaxosTimestampedProposalProtocol.makeAccept(m_agentName, pNum, iNum, decision);
        m_netService.putBroadcastObject(msg);
    }

    public int getLocalRegPort() {
        return m_localRegPort;
    }

    public void setLocalRegPort(int localRegPort) {
        this.m_localRegPort = localRegPort;
    }

    public int getLocalComPort() {
        return m_localComPort;
    }

    public void setLocalComPort(int localComPort) {
        this.m_localComPort = localComPort;
    }
}
