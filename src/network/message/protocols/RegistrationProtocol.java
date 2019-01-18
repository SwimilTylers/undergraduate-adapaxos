package network.message.protocols;

import com.sun.istack.internal.NotNull;
import javafx.util.Pair;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

/**
 * @author : Swimiltylers
 * @version : 2019/1/4 0:05
 */
public class RegistrationProtocol implements Serializable {
    private static final long serialVersionUID = 4505516181213774144L;

    public String getSenderName() {
        return m_senderName;
    }

    public Pair<InetAddress, Integer> getAnotherChannel() {
        return m_anotherChannel;
    }

    protected String m_senderName;
    protected Pair<InetAddress, Integer> m_anotherChannel;

    RegistrationProtocol(){}

    public RegistrationProtocol(@NotNull String senderName,
                                @NotNull Pair<InetAddress, Integer> anotherChannel){
        this.m_senderName = senderName;
        this.m_anotherChannel = anotherChannel;
    }

    public RegistrationProtocol(@NotNull String senderName,
                                InetAddress anotherChannel_inet, int anotherChannel_port){
        this.m_senderName = senderName;
        this.m_anotherChannel = new Pair<>(anotherChannel_inet, anotherChannel_port);
    }

    public RegistrationProtocol(@NotNull String senderName,
                                String anotherChannel_inet, int anotherChannel_port)
            throws UnknownHostException {
        this.m_senderName = senderName;
        this.m_anotherChannel = new Pair<>(InetAddress.getByName(anotherChannel_inet), anotherChannel_port);
    }
}
