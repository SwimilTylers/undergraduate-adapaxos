package client;

import com.sun.istack.internal.NotNull;
import network.message.protocols.GenericClientMessage;

import java.io.Serializable;
import java.net.Socket;

/**
 * @author : Swimiltylers
 * @version : 2019/1/29 12:24
 */
public class ClientRequest implements Serializable {
    private static final long serialVersionUID = 6199390168731306554L;
    public final String exec;
    public final String clientSocketDescription;
    private transient final Socket clientSocket;

    public ClientRequest(@NotNull GenericClientMessage.Propose proposal, @NotNull Socket socket){
        exec = proposal.exec;
        clientSocket = socket;
        clientSocketDescription = socket.toString();
    }

    public Socket getLocalClientSocket() {
        return clientSocket;
    }

    @Override
    public String toString() {
        final String format = "[%s][desc=\"%s\"]";
        return String.format(format, clientSocketDescription, exec);
    }
}
