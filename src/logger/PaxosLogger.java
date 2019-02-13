package logger;

import client.ClientRequest;

/**
 * @author : Swimiltylers
 * @version : 2019/2/2 17:40
 */
public interface PaxosLogger {
    void error(String error);
    void warn(String warn);
    void log(String log);
    void commit(int inst_no, ClientRequest[] brief);
}
