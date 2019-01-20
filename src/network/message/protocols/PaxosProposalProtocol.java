package network.message.protocols;

import com.sun.istack.internal.NotNull;

import java.io.Serializable;

/**
 * @author : Swimiltylers
 * @version : 2019/1/4 0:07
 */
abstract public class PaxosProposalProtocol<pNum_t, pContent_t> implements Serializable {
    private static final long serialVersionUID = 4595615130447762998L;

    protected pNum_t m_pNum;
    protected String m_senderName;
    protected pContent_t m_proposalContent;

    public enum PROPOSAL_TYPE {
        PROPOSAL_PREPARE, PROPOSAL_ACK, PROPOSAL_ACCEPT, PROPOSAL_ACCEPTED, PROPOSAL_KILL
    }

    protected PROPOSAL_TYPE m_proposal;

    PaxosProposalProtocol(){}

    public PaxosProposalProtocol(@NotNull pNum_t pNum, String senderName, pContent_t content, PROPOSAL_TYPE pType){
        m_pNum = pNum;
        m_senderName = senderName;
        m_proposalContent = content;
        m_proposal = pType;
    }

    abstract public int compareTo(PaxosProposalProtocol<pNum_t, ?> a);

    public pNum_t getPNum() {
        return m_pNum;
    }

    public String getSenderName() {
        return m_senderName;
    }

    public pContent_t getProposalContent() {
        return m_proposalContent;
    }

    public PROPOSAL_TYPE getProposalType() {
        return m_proposal;
    }
}
