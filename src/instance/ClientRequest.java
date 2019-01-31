package instance;

import com.sun.istack.internal.NotNull;
import network.message.protocols.GenericClientMessage;

import java.io.BufferedOutputStream;

/**
 * @author : Swimiltylers
 * @version : 2019/1/29 12:24
 */
public class ClientRequest {
    public final String exec;
    public final BufferedOutputStream to;

    public ClientRequest(@NotNull GenericClientMessage.Propose proposal, @NotNull BufferedOutputStream ostream){
        exec = proposal.exec;
        to = ostream;
    }
}
