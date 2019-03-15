package instance.maintenance;

import client.ClientRequest;
import com.sun.istack.internal.NotNull;
import javafx.util.Pair;

import java.io.Serializable;
import java.util.*;

/**
 * @author : Swimiltylers
 * @version : 2019/2/14 18:43
 */
public class HistoryMaintenance implements Serializable {
    private static final long serialVersionUID = -2373815502534795800L;
    private int maxRecvLeaderId;
    private int maxRecvInstBallot;
    public boolean HOST_RESTORE;      // decide whether to restore local.proposals
    public ClientRequest[] reservedCmds;       // latest proposals
    private Set<Pair<Integer, Integer>> received;   // in case of replicated receipt

    private HistoryMaintenance(int initLeaderId, int initInstBallot, ClientRequest[] initCmds){
        maxRecvLeaderId = initLeaderId;
        maxRecvInstBallot = initInstBallot;
        reservedCmds = initCmds;

        HOST_RESTORE = true;

        received = new HashSet<>();
        received.add(new Pair<>(initLeaderId, initInstBallot));
    }

    /* this initiator is designed for restore-late case */
    private HistoryMaintenance(@NotNull Queue<ClientRequest> restoredProposals, int initLeaderId, int initInstBallot, ClientRequest[] initCmds){
        maxRecvLeaderId = -1;
        maxRecvInstBallot = -1;
        reservedCmds = null;

        if (initCmds != null)
            restoredProposals.addAll(Arrays.asList(initCmds));

        HOST_RESTORE = false;

        received = new HashSet<>();
        received.add(new Pair<>(initLeaderId, initInstBallot));
    }

    private void record(@NotNull Queue<ClientRequest> restoredProposals, int leaderId, int instBallot, ClientRequest[] cmds){
        if (!received.contains(new Pair<>(leaderId, instBallot))){
            received.add(new Pair<>(leaderId, instBallot));

            HOST_RESTORE = true;

            if (leaderId > maxRecvLeaderId
                    || (leaderId == maxRecvLeaderId && instBallot > maxRecvInstBallot)){
                if (reservedCmds != null)
                    restoredProposals.addAll(Arrays.asList(reservedCmds));

                maxRecvLeaderId = leaderId;
                maxRecvInstBallot = instBallot;
                reservedCmds = cmds;

            }
            else if (cmds != null)
                restoredProposals.addAll(Arrays.asList(cmds));
        }

    }

    private void restore(@NotNull Queue<ClientRequest> restoredProposals, int leaderId, int instBallot, ClientRequest[] cmds){
        if (!received.contains(new Pair<>(leaderId, instBallot))){
            received.add(new Pair<>(leaderId, instBallot));

            if (cmds != null)
                restoredProposals.addAll(Arrays.asList(cmds));
        }
    }

    public enum RESTORE_TYPE{
        EARLY, LATE
    }

    public static HistoryMaintenance restoreHelper(HistoryMaintenance oldUnit, RESTORE_TYPE type,
                                                   @NotNull Queue<ClientRequest> restoredProposals,
                                                   int leaderId, int instBallot, ClientRequest[] cmds){

        if (type == RESTORE_TYPE.EARLY){
            if (oldUnit == null)
                /* watch out for the constructor
                 * it is a restore-early-style one */
                oldUnit = new HistoryMaintenance(leaderId, instBallot, cmds);
            else
                oldUnit.record(restoredProposals, leaderId, instBallot, cmds);

            return oldUnit;
        }
        else {
            if (oldUnit == null)
                /* watch out for the constructor
                 * it is a restore-late-style one */
                oldUnit = new HistoryMaintenance(restoredProposals, leaderId, instBallot, cmds);
            else
                oldUnit.restore(restoredProposals, leaderId, instBallot, cmds);

            return oldUnit;
        }
    }
}
