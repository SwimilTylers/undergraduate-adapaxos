package network.service.module.controller;

import client.grs.GlobalRequestStatistics;

import java.io.*;
import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author : Swimiltylers
 * @version : 2019/4/19 22:20
 */
public class GlobalBipolarController extends GlobalLeaderElectionController{
    private AtomicReference<int[]> bipolarArray;
    private AtomicInteger stateCount;
    private AtomicBoolean onRunning;

    private GlobalRequestStatistics grs;

    public GlobalBipolarController(int peerSize, int offerSize, int initLeader, GlobalRequestStatistics grs){
        super(peerSize, offerSize, initLeader);
        this.grs = grs;
        bipolarArray = new AtomicReference<>();
        onRunning = new AtomicBoolean(true);
        stateCount = new AtomicInteger(0);
    }

    @Override
    protected void finalize() throws Throwable {
        onRunning.set(false);
        stateCount.set(-1);
        super.finalize();
    }

    public void controlledByFile(final File source){
        try {
            BufferedReader reader = new BufferedReader(new FileReader(source));
            String str = "";
            while (onRunning.get() && (str = reader.readLine()) != null){
                String[] pair = str.split("[\\[\\]]");
                stateCount.updateAndGet(i -> {
                    int[] on = new int[peerSize];
                    Arrays.fill(on, 0);
                    Scanner scanner = new Scanner(pair[1]);
                    while (scanner.hasNext())
                        on[scanner.nextInt()] = 1;
                    System.out.println("[controller][bipolar="+Arrays.toString(on)+"]");

                    stateUpdate(on);
                    bipolarArray.set(on);

                    return ++i;
                });

                Thread.sleep(Integer.valueOf(pair[3]));
            }
            if (grs != null) {
                FileWriter writer = new FileWriter("conclusion.txt", false);
                String conclusion = grs.makeConclusion(10000);
                writer.write("conclusion = " + System.currentTimeMillis() + "\n\n");
                writer.write(conclusion);
                writer.write("\n\n============================================\n\n\n");
                writer.flush();
                writer.close();
                System.out.println("concluded");
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public BipolarStateDecider getDecider(int netServiceId){
        return () -> bipolarArray.get()[netServiceId];
    }

    public BipolarStateReminder getReminder(int machineId){
        return () -> stateCount.get();
    }
}
