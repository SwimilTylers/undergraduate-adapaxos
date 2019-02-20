package instance.maintenance;

import client.ClientRequest;
import com.sun.istack.internal.NotNull;
import javafx.util.Pair;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    public HistoryMaintenance(int initLeaderId, int initInstBallot, ClientRequest[] initCmds){
        maxRecvLeaderId = initLeaderId;
        maxRecvInstBallot = initInstBallot;
        reservedCmds = initCmds;

        HOST_RESTORE = true;

        received = new HashSet<>();
        received.add(new Pair<>(initLeaderId, initInstBallot));
    }

    /* this initiator is designed for restore-late case */
    public HistoryMaintenance(@NotNull List<ClientRequest> restoredProposals, int initLeaderId, int initInstBallot, ClientRequest[] initCmds){
        maxRecvLeaderId = -1;
        maxRecvInstBallot = -1;
        reservedCmds = null;

        if (initCmds != null)
            restoredProposals.addAll(Arrays.asList(initCmds));

        HOST_RESTORE = false;

        received = new HashSet<>();
        received.add(new Pair<>(initLeaderId, initInstBallot));
    }

    public void record(@NotNull List<ClientRequest> restoredProposals, int leaderId, int instBallot, ClientRequest[] cmds){
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

    public void restore(@NotNull List<ClientRequest> restoredProposals, int leaderId, int instBallot, ClientRequest[] cmds){
        if (!received.contains(new Pair<>(leaderId, instBallot))){
            received.add(new Pair<>(leaderId, instBallot));

            if (cmds != null)
                restoredProposals.addAll(Arrays.asList(cmds));
        }
    }
}
