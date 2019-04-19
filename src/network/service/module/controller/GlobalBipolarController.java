package network.service.module.controller;

import network.service.sender.BipolarSenderDecider;

import java.io.*;
import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author : Swimiltylers
 * @version : 2019/4/19 22:20
 */
public class GlobalBipolarController {
    private int peerSize;
    private AtomicReference<int[]> bipolarArray;
    private AtomicBoolean onRunning;

    public GlobalBipolarController(int peerSize){
        this.peerSize = peerSize;
        bipolarArray = new AtomicReference<>();
        onRunning = new AtomicBoolean(true);
    }

    @Override
    protected void finalize() throws Throwable {
        onRunning.set(false);
        super.finalize();
    }

    public void controlledByFile(final File source){
        try {
            BufferedReader reader = new BufferedReader(new FileReader(source));
            String str = "";
            while (onRunning.get() && (str = reader.readLine()) != null){
                String[] pair = str.split("[\\[\\]]");
                //System.out.println(Arrays.toString(pair));
                int[] on = new int[peerSize];
                Arrays.fill(on, 0);
                Scanner scanner = new Scanner(pair[1]);
                while (scanner.hasNext())
                    on[scanner.nextInt()] = 1;
                System.out.println("[controller][bipolar="+Arrays.toString(on)+"]");
                bipolarArray.set(on);
                Thread.sleep(Integer.valueOf(pair[3]));
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public BipolarSenderDecider getDecider(int netServiceId){
        return () -> bipolarArray.get()[netServiceId];
    }
}
