package network.message;

/**
 * @author : Swimiltylers
 * @version : 2019/1/3 12:09
 */
public class ComPaxosMessage<Proposal> extends PaxosMessage<String>{
    private Proposal proposal;
    private long proposalNum;
    private long maxChosenProposalNum;

    public enum ComPaxosMessageType{
        PREPARE, ACK, ACCEPT
    };

    private ComPaxosMessageType type;

    public ComPaxosMessageType getType() {
        return type;
    }

    public long getMaxChosenProposalNum() {
        return maxChosenProposalNum;
    }

    public long getProposalNum() {
        return proposalNum;
    }

    public Proposal getProposal() {
        return proposal;
    }

    public void setMaxChosenProposalNum(long maxChosenProposalNum) {
        this.maxChosenProposalNum = maxChosenProposalNum;
    }

    public void setProposal(Proposal proposal) {
        this.proposal = proposal;
    }

    public void setProposalNum(long proposalNum) {
        this.proposalNum = proposalNum;
    }

    public void setType(ComPaxosMessageType type) {
        this.type = type;
    }

    @Override
    protected byte[] toRawInfo(String info) {
        return new byte[0];
    }

    @Override
    protected String fromRawInfo(byte[] raw) {
        return null;
    }

    @Override
    public String getInfo() {
        return null;
    }

    @Override
    public void SetInfo(String msg) {

    }
}
