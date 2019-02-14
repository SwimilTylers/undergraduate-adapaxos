package logger;

import client.ClientRequest;
import com.sun.istack.internal.NotNull;
import network.message.protocols.GenericPaxosMessage;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;

/**
 * @author : Swimiltylers
 * @version : 2019/2/2 17:39
 */
public class NaiveLogger implements PaxosLogger {

    private int id;
    private FileWriter cWriter;

    /* pWriter works for:
    *   this::logPrepare, this::logAckPrepare, this::logAccept,
    *   this::logAckAccept, this::logRestore, this::logCommit */

    private FileWriter pWriter;

    public NaiveLogger(int serverId) {
        id = serverId;
        try {
            cWriter = new FileWriter("logs/server-"+id+"-commit.log", false);
            pWriter = new FileWriter("logs/server-"+id+"-paxos.log", false);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void imm_toFile(FileWriter writer, String entry){
        try {
            writer.write(entry);
            writer.flush();
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
    public void logPrepare(int inst_no, GenericPaxosMessage.Prepare msg, String supplement) {
        Date date = new Date();
        String prepareFormat = "[%tF %<tT:%<tL %<tz][p%08d][PREPARE][leaderId=%d, inst_no=%d][\"%s\"]%n";
        String message = String.format(prepareFormat, date, msg.inst_ballot, msg.leaderId, inst_no, supplement);
        System.out.print(message);
        imm_toFile(pWriter, message);
    }

    @Override
    public void logAckPrepare(int inst_no, GenericPaxosMessage.ackPrepare ack, String supplement) {
        Date date = new Date();
        String ackPrepareFormat = "[%tF %<tT:%<tL %<tz][p%08d][ack:PREPARE][leaderId=%d, inst_no=%d][\"%s\"]%n";
        String message = String.format(ackPrepareFormat, date, ack.inst_ballot, ack.ack_leaderId, inst_no, supplement);
        System.out.print(message);
        imm_toFile(pWriter, message);
    }

    @Override
    public void logAccept(int inst_no, GenericPaxosMessage.Accept msg, String supplement) {
        Date date = new Date();
        String acceptFormat = "[%tF %<tT:%<tL %<tz][p%08d][ACCEPT][leaderId=%d, inst_no=%d, cmd_length=%d][\"%s\"]%n";
        String message = String.format(acceptFormat, date, msg.inst_ballot, msg.leaderId, inst_no, msg.cmds.length, supplement);
        System.out.print(message);
        imm_toFile(pWriter, message);
    }

    @Override
    public void logAckAccept(int inst_no, GenericPaxosMessage.ackAccept ack, String supplement) {
        Date date = new Date();
        String ackAcceptFormat = "[%tF %<tT:%<tL %<tz][p%08d][ack:Accept][leaderId=%d, inst_no=%d, cmd_length=%d][\"%s\"]%n";
        String message = String.format(ackAcceptFormat, date, ack.inst_ballot, ack.ack_leaderId, inst_no, ack.cmds.length, supplement);
        System.out.print(message);
        imm_toFile(pWriter, message);
    }

    @Override
    public void logRestore(int inst_no, GenericPaxosMessage.Restore msg, String supplement) {
        Date date = new Date();
        String restoreFormat = "[%tF %<tT:%<tL %<tz][RESTORE][inst_no=%d][\"%s\"]%n";
        String message = String.format(restoreFormat, date, inst_no, supplement);
        System.out.print(message);
        imm_toFile(pWriter, message);
    }

    @Override
    public void logCommit(int inst_no, @NotNull GenericPaxosMessage.Commit msg, @NotNull String supplement) {
        Date date = new Date();
        String commitFormat = "[%tF %<tT:%<tL %<tz][p%08d][COMMIT][leaderId=%d, inst_no=%d, cmd_length=%d][\"%s\"]%n";
        String message = String.format(commitFormat, date, msg.inst_ballot, msg.leaderId, inst_no, msg.cmds.length, supplement);
        System.out.print(message);
        imm_toFile(pWriter, message);

        imm_toFile(cWriter, message);
        for (ClientRequest request:msg.cmds) {
            imm_toFile(cWriter, "\t"+request.toString()+"\n");
        }
    }

    @Override
    public void logExecute(ClientRequest request) {

    }
}
