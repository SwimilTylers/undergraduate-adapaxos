package network.service.module.simulator;

import java.util.Random;

/**
 * @author : Swimiltylers
 * @version : 2019/3/2 10:21
 */
public class DelayedSimulator implements SimulatorModule{
    private final int DELAY_UPPER_BOUND;
    private final int DELAY_LOWER_BOUND;
    private final Random rnd;

    public DelayedSimulator(int delay_upper_bound, int delay_lower_bound) {
        DELAY_UPPER_BOUND = delay_upper_bound;
        DELAY_LOWER_BOUND = delay_lower_bound;
        rnd = new Random();
    }

    @Override
    public boolean crush() {
        return false;
    }

    @Override
    public boolean lost(int toId) {
        return false;
    }

    @Override
    public void delay(int toId) {
        try {
            Thread.sleep(DELAY_LOWER_BOUND + rnd.nextInt(DELAY_UPPER_BOUND - DELAY_LOWER_BOUND));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Object byzantine(int toId, Object msg) {
        return msg;
    }
}
