package network.message.protocols;

import java.io.Serializable;

/**
 * @author : Swimiltylers
 * @version : 2019/1/28 21:36
 */
public class GenericClientMessage implements Serializable {

    private static final long serialVersionUID = -7915017716144760675L;

    public static class Propose extends GenericClientMessage{
        private static final long serialVersionUID = -79189041677334143L;
        public final String exec;

        public Propose(String exec) {
            this.exec = exec;
        }
    }

    public static class ackPropose extends GenericClientMessage{
        private static final long serialVersionUID = 1361465008679327734L;
        public final long pTimestamp;

        public ackPropose(Propose propose){
            pTimestamp = 0;
        }
    }

    public static class Updated extends GenericClientMessage{
        private static final long serialVersionUID = 2472143475442675021L;
        public final String exec;

        public Updated(Propose propose){
            exec = propose.exec;
        }
    }
}
