package rsm;

import instance.store.RemoteInstanceStore;
import logger.NaiveLogger;
import logger.PaxosLogger;
import network.message.protocols.AdaPaxosMessage;
import network.message.protocols.LeaderElectionMessage;
import network.service.GenericNetService;
import network.service.module.controller.BipolarStateDecider;
import network.service.module.controller.BipolarStateReminder;
import utils.AdaAgents;
import utils.AdaPaxosParameters;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author : Swimiltylers
 * @version : 2019/5/3 14:15
 */
public class StaticPaxosRSM extends AdaPaxosRSM{
    protected StaticPaxosRSM(int serverId, boolean initAsLeader, PaxosLogger logger) {
        super(serverId, initAsLeader, logger);
    }

    @SuppressWarnings("unchecked")
    public static StaticPaxosRSM makeInstance(final int id, final int epoch, final int peerSize, RemoteInstanceStore remoteStore, GenericNetService net, boolean initFsync){
        StaticPaxosRSM rsm = new StaticPaxosRSM(id, false, new NaiveLogger(id));
        rsm.netConnectionBuild(net, net.getConnectionModule(), peerSize)
                .instanceSpaceBuild(AdaPaxosParameters.RSM.DEFAULT_INSTANCE_SIZE, epoch << 16, 0)
                .instanceStorageBuild(remoteStore, initFsync, AdaPaxosParameters.RSM.DEFAULT_INSTANCE_SIZE)
                .messageChanBuild(AdaPaxosParameters.RSM.DEFAULT_MESSAGE_SIZE, AdaPaxosParameters.RSM.DEFAULT_MESSAGE_SIZE, AdaPaxosParameters.RSM.DEFAULT_MESSAGE_SIZE, AdaPaxosParameters.RSM.DEFAULT_INSTANCE_SIZE, AdaPaxosParameters.RSM.DEFAULT_INSTANCE_SIZE);
        return rsm;
    }

    @Override
    public void routine(Runnable... supplement){
        routineOnRunning = new AtomicBoolean(true);
        ExecutorService routines = Executors.newCachedThreadPool();
        routines.execute(() -> routine_grsBatch(500));
        //routines.execute(()-> routine_batch(1000, GenericPaxosSMR.DEFAULT_REQUEST_COMPACTING_SIZE));
        routines.execute(() -> routine_monitor(20, 70, 3, 10));
        routines.execute(this::routine_response);
        //routines.execute(() -> routine_backup(5000));
        routines.execute(() -> routine_leadership(2000));
        routines.shutdown();
    }

    @Override
    protected void routine_monitor(int monitorItv, int expire, int stability, int decisionDelay) {
        final int bare_majority = (peerSize+1)/2;

        while (routineOnRunning.get()){
            if (!asLeader.get()) {
                try {
                    if (!asLeader.get() && !recovery.onLeaderElection()){      // carry out leader detection
                        if (recovery.isLeaderSurvive(expire)){

                            /* at the first sight out timeout, follower should flush all on-memory instances to disk */
                            logger.record(false, "diag", "[" + System.currentTimeMillis() + "][leader failure][test=1]\n");

                            Thread.sleep(decisionDelay);

                            if (recovery.isLeaderSurvive(expire)){  // leader crash confirmed, running into FAST_MODE
                                if (!routineOnRunning.get())
                                    break;

                                if (!forceFsync.get()){   // FAST_MODE before leader crashed
                                    long leToken = AdaAgents.newToken();
                                    recovery.markLeaderElection(false, leToken);
                                    int ticket = maxReceivedInstance.get();
                                    leProvider.provide(new LeaderElectionMessage.LEOffer(serverId, leToken, ticket));
                                    logger.record(true, "diag", "[" + System.currentTimeMillis() + "][leader failure][test=2][confirmed][RECOVERED, token="+leToken+", ticket="+ ticket +"]\n");
                                }
                                else {  // SLOW_MODE before leader crashed
                                    long leToken = AdaAgents.newToken();
                                    recovery.markLeaderElection(true, leToken);
                                    int ticket = fsyncInitInstance.get();
                                    leProvider.provide(new LeaderElectionMessage.LEOffer(serverId, leToken, ticket));
                                    logger.record(true, "diag", "[" + System.currentTimeMillis() + "][leader failure][test=2][confirmed][RECOVERING, token="+leToken+", ticket="+ ticket +"]\n");
                                    memorySynchronize(leToken); // fetch up-to-date information from disk
                                }
                            }
                        }
                    }
                } catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    protected void thread_bipolar(final BipolarStateReminder reminder, final BipolarStateDecider decider){
        int lastState = decider.decide();
        int lastEpoch = reminder.remind(), crtEpoch;
        while ((crtEpoch = reminder.remind()) >= 0){
            if (crtEpoch != lastEpoch) {
                lastEpoch = crtEpoch;
                int crtState = decider.decide();
                if (lastState != crtState) {
                    lastState = crtState;
                    if (crtState == 0) {
                        logger.record(false, "diag", "[" + System.currentTimeMillis() + "][state change][1->0]\n");
                        routineOnRunning.set(false);
                        asLeader.set(false);
                    } else {
                        logger.record(false, "diag", "[" + System.currentTimeMillis() + "]" + "[state change][0->1][tkt=" + fsyncInitInstance.get() + ",init_lid=" + nConfig.initLeaderId + "]\n");
                        instanceSpaceBuild(instanceSpace.length(), reminder.remind() << 16, 0);
                        agent(leProvider);

                        long leToken = AdaAgents.newToken();
                        recovery.markLeaderElection(forceFsync.get(), leToken);
                        leProvider.provide(new LeaderElectionMessage.LEOffer(serverId, leToken, 0));

                        logger.record(true, "diag", "[" + System.currentTimeMillis() + "][recovery][confirmed][RECOVERING, token=" + leToken + "]\n");
                        if (forceFsync.get())
                            memorySynchronize(leToken); // fetch up-to-date information from disks
                        else
                            peerSynchronize(leToken); // fetch up-to-date information from peers

                        routine(supplementRoutines);
                    }
                }
                else if (crtState == 1){
                    crtInstBallot.set(reminder.remind() << 16);
                }
            }
        }
    }
}
