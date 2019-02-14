package logger;

import client.ClientRequest;
import com.sun.istack.internal.NotNull;
import network.message.protocols.GenericPaxosMessage;

/**
 * @author : Swimiltylers
 * @version : 2019/2/2 17:40
 */
public interface PaxosLogger {
    void error(String error);
    void warn(String warn);
    void log(String log, boolean isOnScreen);

    void logPeerNet(int fromId, int toId, @NotNull String desc);
    void logClientNet(@NotNull String client, @NotNull String desc);

    void logPrepare(int inst_no, @NotNull GenericPaxosMessage.Prepare msg, @NotNull String supplement);
    void logAckPrepare(int inst_no, @NotNull GenericPaxosMessage.ackPrepare ack, @NotNull String supplement);
    void logAccept(int inst_no, @NotNull GenericPaxosMessage.Accept msg, @NotNull String supplement);
    void logAckAccept(int inst_no, @NotNull GenericPaxosMessage.ackAccept ack, @NotNull String supplement);
    void logRestore(int inst_no, @NotNull GenericPaxosMessage.Restore msg, @NotNull String supplement);
    void logCommit(int inst_no, @NotNull GenericPaxosMessage.Commit msg, @NotNull String supplement);
    void logExecute(@NotNull ClientRequest request);
}
