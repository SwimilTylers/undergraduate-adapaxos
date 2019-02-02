package client;

import com.sun.istack.internal.NotNull;
import network.message.protocols.GenericClientMessage;

import java.io.BufferedOutputStream;
import java.net.Socket;

/**
 * @author : Swimiltylers
 * @version : 2019/1/29 12:24
 */
public class ClientRequest {
    public final String exec;
    public final Socket to;

    public ClientRequest(@NotNull GenericClientMessage.Propose proposal, @NotNull Socket socket){
        exec = proposal.exec;
        to = socket;
    }
}
