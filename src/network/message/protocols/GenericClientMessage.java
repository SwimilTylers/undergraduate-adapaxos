package network.message.protocols;

import com.sun.istack.internal.NotNull;
import instance.ClientRequest;

import java.io.Serializable;

/**
 * @author : Swimiltylers
 * @version : 2019/1/28 21:36
 */
public class GenericClientMessage implements Serializable {

    private static final long serialVersionUID = -7915017716144760675L;

    public static ClientRequest extractRequests(@NotNull  GenericClientMessage message){
        return null;
    }

    public static class Propose extends GenericClientMessage{

    }

    public static class WrappedClientRequest extends Propose{

    }

    public static class Reply extends GenericClientMessage{

    }
}
