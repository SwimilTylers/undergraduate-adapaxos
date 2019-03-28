package logger;

import client.ClientRequest;
import instance.PaxosInstance;
import javafx.util.Pair;
import network.message.protocols.GenericPaxosMessage;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * @author : Swimiltylers
 * @version : 2019/3/27 21:53
 */
public class DummyLogger implements PaxosLogger {
    private NaiveLogger internal;
    private BlockingQueue<Pair<String, String>> queue;

    public DummyLogger(int id){
        queue = new ArrayBlockingQueue<>(1024);
        internal = new NaiveLogger(id);
        new Thread(() -> {
            while (true){
                try {
                    Pair<String, String> recv = queue.take();
                    internal.record(false, recv.getKey(), recv.getValue());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    @Override
    public void error(String error) {

    }

    @Override
    public void warn(String warn) {

    }

    @Override
    public void record(boolean isOnScreen, String suffix, String record) {
        try {
            queue.put(new Pair<>(suffix, record));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void log(boolean isOnScreen, String log) {

    }

    @Override
    public void logFormatted(boolean isOnScreen, String... logs) {

    }

    @Override
    public void logPeerNet(int fromId, int toId, String desc) {

    }

    @Override
    public void logClientNet(String client, String desc) {

    }

    @Override
    public void logPrepare(int inst_no, GenericPaxosMessage.Prepare msg, String supplement) {

    }

    @Override
    public void logAckPrepare(int inst_no, GenericPaxosMessage.ackPrepare ack, String supplement) {

    }

    @Override
    public void logAccept(int inst_no, GenericPaxosMessage.Accept msg, String supplement) {

    }

    @Override
    public void logAckAccept(int inst_no, GenericPaxosMessage.ackAccept ack, String supplement) {

    }

    @Override
    public void logRestore(int inst_no, GenericPaxosMessage.Restore msg, String supplement) {

    }

    @Override
    public void logCommit(int inst_no, GenericPaxosMessage.Commit msg, String supplement) {

    }

    @Override
    public void logExecute(ClientRequest request) {

    }
}
