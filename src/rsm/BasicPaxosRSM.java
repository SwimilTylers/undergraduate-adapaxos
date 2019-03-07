package rsm;

import agent.acceptor.Acceptor;
import agent.acceptor.GenericAcceptor;
import agent.learner.GenericLearner;
import agent.learner.Learner;
import agent.proposer.GenericProposer;
import agent.proposer.Proposer;
import client.ClientRequest;
import network.message.protocols.GenericPaxosMessage;

import java.util.concurrent.TimeUnit;

/**
 * @author : Swimiltylers
 * @version : 2019/2/20 15:33
 */
public class BasicPaxosRSM extends GenericPaxosSMR{
    private Proposer proposer;
    private Acceptor acceptor;
    private Learner learner;

    public BasicPaxosRSM(int id, String[] addr, int[] port) {
        super(id, addr, port);
    }

    @Override
    protected void agentDeployment() {
        proposer = new GenericProposer(serverId, peerSize, instanceSpace, net.getPeerMessageSender(), restoredRequestList);
        acceptor = new GenericAcceptor(instanceSpace, net.getPeerMessageSender());
        learner = new GenericLearner(serverId, peerSize, instanceSpace, net.getPeerMessageSender(), restoredRequestList, logger);
    }

    @Override
    protected void clientConversation() {
        ClientRequest[] compact = null;
        try {
            compact = compactChan.poll(clientComWaiting, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (compact != null)
            proposer.handleRequests(compact);
    }

    @Override
    protected void peerConversation(){
        GenericPaxosMessage msg;
        try {
            msg = pMessage.poll(peerComWaiting, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            System.out.println("Unsuccessfully message taking");
            return;
        }

        if (msg != null) {
            if (msg instanceof GenericPaxosMessage.Prepare) {
                GenericPaxosMessage.Prepare cast = (GenericPaxosMessage.Prepare) msg;
                logger.logPrepare(cast.inst_no, cast, "handle");
                acceptor.handlePrepare(cast);
                logger.logPrepare(cast.inst_no, cast, "exit handle");
            } else if (msg instanceof GenericPaxosMessage.ackPrepare) {
                GenericPaxosMessage.ackPrepare cast = (GenericPaxosMessage.ackPrepare) msg;
                logger.logAckPrepare(cast.inst_no, cast, "handle");
                proposer.handleAckPrepare(cast);
                logger.logAckPrepare(cast.inst_no, cast, "exit handle");
            } else if (msg instanceof GenericPaxosMessage.Accept) {
                GenericPaxosMessage.Accept cast = (GenericPaxosMessage.Accept) msg;
                logger.logAccept(cast.inst_no, cast, "handle");
                acceptor.handleAccept(cast);
                logger.logAccept(cast.inst_no, cast, "exit handle");
            } else if (msg instanceof GenericPaxosMessage.ackAccept) {
                GenericPaxosMessage.ackAccept cast = (GenericPaxosMessage.ackAccept) msg;
                logger.logAckAccept(cast.inst_no, cast, "handle");
                learner.handleAckAccept(cast);
                logger.logAckAccept(cast.inst_no, cast, "exit handle");
            } else if (msg instanceof GenericPaxosMessage.Commit) {
                GenericPaxosMessage.Commit cast = (GenericPaxosMessage.Commit) msg;
                logger.logCommit(cast.inst_no, cast, "handle");
                learner.handleCommit(cast);
                logger.logCommit(cast.inst_no, cast, "exit handle");
            } else if (msg instanceof GenericPaxosMessage.Restore) {
                GenericPaxosMessage.Restore cast = (GenericPaxosMessage.Restore) msg;
                logger.logRestore(cast.inst_no, cast, "handle");
                handleRestore(cast);
                logger.logRestore(cast.inst_no, cast, "exit handle");
            }
        }
    }
}
