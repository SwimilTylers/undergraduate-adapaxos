package network.service.handler;

import com.sun.istack.internal.NotNull;
import network.message.protocols.GenericBeacon;

/**
 * @author : Swimiltylers
 * @version : 2019/1/28 21:09
 */
public class GenericBeaconHandler {
    private int total;
    private boolean[] isAlive;

    public GenericBeaconHandler(int N){
        total = N;
        isAlive = new boolean[N];
    }

    synchronized public void alive(int i){
        isAlive[i] = true;
    }

    synchronized public boolean check(int i){
        return isAlive[i];
    }

    synchronized public GenericBeacon handle(@NotNull GenericBeacon beacon){
        return null;
    }
}
