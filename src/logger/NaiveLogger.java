package logger;

import client.ClientRequest;
import com.sun.istack.internal.NotNull;
import javafx.util.Pair;
import network.message.protocols.GenericPaxosMessage;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

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

    /* lWriter works for: */
    private FileWriter lWriter;

    /* nWriter works for: */
    private FileWriter nWriter;

    private FileWriter connWriter;

    private FileWriter diagWriter;

    public NaiveLogger(int serverId) {
        id = serverId;
        try {
            cWriter = new FileWriter("logs/server-"+id+".commit.log", false);
            pWriter = new FileWriter("logs/server-"+id+".paxos.log", false);
            lWriter = new FileWriter("logs/server-"+id+".log", false);
            nWriter = new FileWriter("logs/server-"+id+".net.log", false);
            connWriter = new FileWriter("logs/server-"+id+".heartbeat", false);
            diagWriter = new FileWriter("logs/server-"+id+".diagnosis", false);
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
    public void record(boolean isOnScreen, String suffix, String record) {
        if (isOnScreen)
            System.out.print(record);
        if (suffix.equals("hb"))
            imm_toFile(connWriter, record);
        else if (suffix.equals("diag"))
            imm_toFile(diagWriter, record);
    }

    @Override
    public void log(boolean isOnScreen, String log) {
        if (isOnScreen)
            System.out.print(log);
        imm_toFile(lWriter, log);
    }

    @Override
    public void logFormatted(boolean isOnScreen, String... logs) {
        Date date = new Date();
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("[%tF %<tT:%<tL %<tz]", date));
        for (String log :
                logs) {
            builder.append("[").append(log).append("]");
        }
        builder.append("\n");
        log(isOnScreen, builder.toString());
    }

    @Override
    public void logPeerNet(int fromId, int toId, String desc) {
        /*
        Date date = new Date();
        String netFormat = "[%tF %<tT:%<tL %<tz][network:peers][from=%d, to=%d][\"%s\"]%n";
        String message = String.format(netFormat, date, fromId, toId, desc);
        System.out.print(message);
        imm_toFile(nWriter, message);

        log(false, message);
        */
    }

    @Override
    public void logClientNet(String client, String desc) {
        Date date = new Date();
        String netFormat = "[%tF %<tT:%<tL %<tz][network:client][client=\"%s\"][\"%s\"]%n";
        String message = String.format(netFormat, date, client, desc);
        System.out.print(message);
        imm_toFile(nWriter, message);

        log(false, message);
    }

    @Override
    public void logPrepare(int inst_no, GenericPaxosMessage.Prepare msg, String supplement) {
        Date date = new Date();
        String prepareFormat = "[%tF %<tT:%<tL %<tz][p%08d][PREPARE][leaderId=%d, inst_no=%d][\"%s\"]%n";
        String message = String.format(prepareFormat, date, msg.inst_ballot, msg.leaderId, inst_no, supplement);
        //System.out.print(message);
        imm_toFile(pWriter, message);

        log(false, message);
    }

    @Override
    public void logAckPrepare(int inst_no, GenericPaxosMessage.ackPrepare ack, String supplement) {
        Date date = new Date();
        String ackPrepareFormat = "[%tF %<tT:%<tL %<tz][p%08d][makeAck:PREPARE][leaderId=%d, inst_no=%d][\"%s\"]%n";
        String message = String.format(ackPrepareFormat, date, ack.inst_ballot, ack.ack_leaderId, inst_no, supplement);
        //System.out.print(message);
        imm_toFile(pWriter, message);

        log(false, message);
    }

    @Override
    public void logAccept(int inst_no, GenericPaxosMessage.Accept msg, String supplement) {
        Date date = new Date();
        String acceptFormat = "[%tF %<tT:%<tL %<tz][p%08d][ACCEPT][leaderId=%d, inst_no=%d, cmd_length=%d][\"%s\"]%n";
        String message = String.format(acceptFormat, date, msg.inst_ballot, msg.leaderId, inst_no, msg.cmds.length, supplement);
        //System.out.print(message);
        imm_toFile(pWriter, message);

        log(false, message);
    }

    @Override
    public void logAckAccept(int inst_no, GenericPaxosMessage.ackAccept ack, String supplement) {
        Date date = new Date();
        String ackAcceptFormat = "[%tF %<tT:%<tL %<tz][p%08d][makeAck:Accept][leaderId=%d, inst_no=%d, cmd_length=%d][\"%s\"]%n";
        String message = String.format(ackAcceptFormat, date, ack.inst_ballot, ack.ack_leaderId, inst_no, ack.cmds.length, supplement);
        //System.out.print(message);
        imm_toFile(pWriter, message);

        log(false, message);
    }

    @Override
    public void logRestore(int inst_no, GenericPaxosMessage.Restore msg, String supplement) {
        Date date = new Date();
        String restoreFormat = "[%tF %<tT:%<tL %<tz][RESTORE][inst_no=%d][\"%s\"]%n";
        String message = String.format(restoreFormat, date, inst_no, supplement);
        //System.out.print(message);
        imm_toFile(pWriter, message);

        log(false, message);
    }

    @Override
    public void logCommit(int inst_no, @NotNull GenericPaxosMessage.Commit msg, @NotNull String supplement) {
        Date date = new Date();
        String commitFormat = "[%tF %<tT:%<tL %<tz][p%08d][COMMIT][leaderId=%d, inst_no=%d, cmd_length=%d][\"%s\"]%n";
        String message = String.format(commitFormat, date, msg.inst_ballot, msg.leaderId, inst_no, msg.cmds.length, supplement);
        //System.out.print(message);
        imm_toFile(pWriter, message);

        log(false, message);

        if (supplement.equals("settled")) {
            imm_toFile(cWriter, message);
            for (ClientRequest request : msg.cmds) {
                imm_toFile(cWriter, "\t" + request.toString() + "\n");
            }
        }
    }

    @Override
    public void logExecute(ClientRequest request) {

    }
}
