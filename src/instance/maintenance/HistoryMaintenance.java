package instance.maintenance;

import client.ClientRequest;
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
    public boolean HOST_RESTORE;
    public ClientRequest[] reservedCmds;
    private Set<Pair<Integer, Integer>> received;

    public HistoryMaintenance(int initLeaderId, int initInstBallot, ClientRequest[] initCmds){
        maxRecvLeaderId = initLeaderId;
        maxRecvInstBallot = initInstBallot;
        reservedCmds = initCmds;

        HOST_RESTORE = true;

        received = new HashSet<>();
        received.add(new Pair<>(initLeaderId, initInstBallot));
    }

    /* this initiator is designed for restore-late case */
    public HistoryMaintenance(List<ClientRequest> restoredProposals, int initLeaderId, int initInstBallot, ClientRequest[] initCmds){
        maxRecvLeaderId = -1;
        maxRecvInstBallot = -1;
        reservedCmds = null;

        if (initCmds != null)
            restoredProposals.addAll(Arrays.asList(initCmds));

        HOST_RESTORE = false;

        received = new HashSet<>();
        received.add(new Pair<>(initLeaderId, initInstBallot));
    }

    public void record(List<ClientRequest> restoredProposals, int leaderId, int instBallot, ClientRequest[] cmds){
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

    public void restore(List<ClientRequest> restoredProposals, int leaderId, int instBallot, ClientRequest[] cmds){
        if (!received.contains(new Pair<>(leaderId, instBallot))){
            received.add(new Pair<>(leaderId, instBallot));

            if (cmds != null)
                restoredProposals.addAll(Arrays.asList(cmds));
        }
    }
}
