package network.message.protocols;

import javafx.util.Pair;

import java.io.Serializable;

/**
 * @author : Swimiltylers
 * @version : 2019/1/4 0:23
 */
public class PaxosTimestampedProposalProtocol extends PaxosProposalProtocol<Long, Object> implements Serializable {
    private static final long serialVersionUID = 9216770728138537120L;

    protected long m_issue;
    public static final long PNUM_NO_SUCH_HISTORY = 0L;

    PaxosTimestampedProposalProtocol(){}

    PaxosTimestampedProposalProtocol(long timestamp,  String senderName, long issue_n, Object content, PROPOSAL_TYPE pType){
        super(timestamp, senderName, content, pType);
        m_issue = issue_n;
    }

    public long getIssueNum() {
        return m_issue;
    }

    public static long resolutePrepare(PaxosTimestampedProposalProtocol proposal){
        return proposal.getPNum();
    }

    public static PaxosTimestampedProposalProtocol makePrepare(String senderName, long pnum, long inum){
        return new PaxosTimestampedProposalProtocol(
                pnum,
                senderName,
                inum,
                "Prepare phase",
                PROPOSAL_TYPE.PROPOSAL_PREPARE);
    }

    @SuppressWarnings("unchecked")
    public static <T> Pair<Long, T> resoluteAck(PaxosTimestampedProposalProtocol proposal){
        if (proposal.m_proposal == PROPOSAL_TYPE.PROPOSAL_ACK){
            return (Pair<Long, T>)proposal.m_proposalContent;
        }
        else
            return null;
    }

    public static PaxosTimestampedProposalProtocol makeAck(String senderName, long pnum, long inum,
                                                           long lastChosenPnum, Object chosenObject){
        return new PaxosTimestampedProposalProtocol(
                pnum,
                senderName,
                inum,
                new Pair<>(lastChosenPnum, chosenObject),
                PROPOSAL_TYPE.PROPOSAL_ACK);
    }

    @SuppressWarnings("unchecked")
    public static <T> T resoluteAccept(PaxosTimestampedProposalProtocol proposal){
        if (proposal.m_proposal == PROPOSAL_TYPE.PROPOSAL_ACCEPT){
            return (T)proposal.m_proposalContent;
        }
        else
            return null;
    }

    public static PaxosTimestampedProposalProtocol makeAccept(String senderName, long pnum, long inum,
                                                              Object decisionObject){
        return new PaxosTimestampedProposalProtocol(
                pnum,
                senderName,
                inum,
                decisionObject,
                PROPOSAL_TYPE.PROPOSAL_ACCEPT
        );
    }

    @SuppressWarnings("unchecked")
    public static <T> T resoluteAccepted(PaxosTimestampedProposalProtocol proposal){
        if (proposal.m_proposal == PROPOSAL_TYPE.PROPOSAL_ACCEPTED){
            return (T)proposal.m_proposalContent;
        }
        else
            return null;
    }

    public static PaxosTimestampedProposalProtocol makeAccepted(String senderName, long pnum, long inum,
                                                                Object decisionObject){
        return new PaxosTimestampedProposalProtocol(
                pnum,
                senderName,
                inum,
                decisionObject,
                PROPOSAL_TYPE.PROPOSAL_ACCEPTED
        );
    }

    @Override
    public int compareTo(PaxosProposalProtocol<Long, ?> a) {
        return Long.compare(this.m_pNum, a.m_pNum);
    }
}
