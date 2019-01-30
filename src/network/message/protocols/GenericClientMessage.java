package network.message.protocols;

import java.io.Serializable;

/**
 * @author : Swimiltylers
 * @version : 2019/1/28 21:36
 */
public class GenericClientMessage implements Serializable {

    private static final long serialVersionUID = -7915017716144760675L;

    public static class Propose extends GenericClientMessage{

    }

    public static class Reply extends GenericClientMessage{

    }
}
