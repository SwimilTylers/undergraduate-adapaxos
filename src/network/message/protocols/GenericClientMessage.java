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

    public static class Reply extends GenericClientMessage{

    }
}
