package rsm;

import agent.Acceptor;
import agent.Learner;
import agent.Proposer;
import com.sun.istack.internal.NotNull;
import network.pool.FixedPoolDescriptor;

import java.io.IOException;
import java.util.concurrent.*;

/**
 * @author : Swimiltylers
 * @version : 2019/1/20 16:17
 */

@Deprecated
public class SimpleRSM<Consensus> {
    private Proposer<Consensus> m_proposer;
    private Acceptor<Consensus> m_acceptor;
    private Learner<Consensus> m_learner;

    private FixedPoolDescriptor m_poolDescriptor;

    private String m_pName;
    private String m_aName;
    private String m_lName;

    public static final int RSM_PROPOSER_WAITING_TIME = 1000;
    public static final int RSM_ACCEPTOR_WAITING_TIME = 1000;
    public static final int RSM_LEARNER_WAITING_TIME = 1000;

    private int m_pRegWaiting = RSM_PROPOSER_WAITING_TIME;
    private int m_aRegWaiting = RSM_ACCEPTOR_WAITING_TIME;
    private int m_lRegWaiting = RSM_LEARNER_WAITING_TIME;

    public SimpleRSM(@NotNull FixedPoolDescriptor descriptor, @NotNull String proposer, @NotNull String acceptor, @NotNull String learner){
        m_poolDescriptor = descriptor;
        m_pName = proposer;
        m_aName = acceptor;
        m_lName = learner;
    }

    protected void initProposer(){
        m_proposer = new Proposer<>();
        m_proposer.setLocalRegPort(m_poolDescriptor.fetchRegPort(m_pName));
        m_proposer.setLocalComPort(m_poolDescriptor.fetchComPort(m_pName));
    }

    protected void initAcceptor(Consensus initDecision){
        m_acceptor = new Acceptor<>();

        m_acceptor.setInitDecision(initDecision);
        m_acceptor.setLocalRegPort(m_poolDescriptor.fetchRegPort(m_aName));
        m_acceptor.setLocalComPort(m_poolDescriptor.fetchComPort(m_aName));

        m_acceptor.setLocalInfoRegPort(m_poolDescriptor.fetchInfoRegPort(m_aName));
        m_acceptor.setLocalInfoComPort(m_poolDescriptor.fetchInfoComPort(m_aName));
    }

    protected void initLearner(){
        m_learner = new Learner<>();

        m_learner.setLocalInfoRegPort(m_poolDescriptor.fetchInfoRegPort(m_lName));
        m_learner.setLocalInfoComPort(m_poolDescriptor.fetchInfoComPort(m_lName));
    }

    public void initPaxos(Consensus localInitDecision){
        initProposer();
        initAcceptor(localInitDecision);
        initLearner();
    }

    public void buildFixedNet(int expireMillis){

        CountDownLatch latch = new CountDownLatch(4);
        ExecutorService executorService = Executors.newCachedThreadPool();

        executorService.execute(() -> {
            executorService.execute(() -> {
                try {
                    m_proposer.initNetService(m_pName, m_poolDescriptor.getAcceptorSize(), m_pRegWaiting);
                    latch.countDown();
                } catch (IOException | InterruptedException | ExecutionException | TimeoutException e) {
                    e.printStackTrace();
                }
            });

            executorService.execute(() -> {
                try {
                    m_acceptor.initNetService2Proposer(m_aName, m_poolDescriptor.fetchProposerAddresses(), m_aRegWaiting);
                    latch.countDown();
                } catch (IOException | InterruptedException | ExecutionException | TimeoutException e) {
                    e.printStackTrace();
                }
            });

            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        m_acceptor.initNetService2Learner(m_aName, m_poolDescriptor.fetchLearnerAddresses(), m_aRegWaiting);
                        latch.countDown();
                    } catch (IOException | InterruptedException | ExecutionException | TimeoutException e) {
                        e.printStackTrace();
                    }
                }
            });

            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        m_learner.initNetService2Acceptor(m_lName, m_poolDescriptor.getAcceptorSize(), m_lRegWaiting);
                        latch.countDown();
                    } catch (IOException | InterruptedException | ExecutionException | TimeoutException e) {
                        e.printStackTrace();
                    }
                }
            });
        });
    }
}
