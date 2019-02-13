package logger;

import client.ClientRequest;
import com.sun.istack.internal.NotNull;

import java.io.FileWriter;
import java.io.IOException;

/**
 * @author : Swimiltylers
 * @version : 2019/2/2 17:39
 */
public class NaiveLogger implements PaxosLogger {

    private int id;
    private FileWriter cWriter;

    public NaiveLogger(int serverId) {
        id = serverId;
        try {
            cWriter = new FileWriter("logs/server-"+id+"-commit.log", false);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void error(String error) {
        System.out.println(error);
    }

    @Override
    public void warn(String warn) {
        System.out.println(warn);
    }

    @Override
    public void log(String log) {
        System.out.println(log);
    }

    @Override
    public void commit(int inst_no, @NotNull ClientRequest[] brief) {
        try {
            cWriter.write(System.currentTimeMillis() + "\t COMMIT "+ brief.length);
            for (ClientRequest clientRequest : brief) {
                cWriter.write("\n\t" + clientRequest.exec);
            }
            cWriter.write("\n");
            cWriter.flush();
        } catch (IOException ignored) {}
    }
}
