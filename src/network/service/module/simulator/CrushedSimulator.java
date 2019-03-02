package network.service.module.simulator;

/**
 * @author : Swimiltylers
 * @version : 2019/3/2 14:54
 */
public class CrushedSimulator implements SimulatorModule{
    @Override
    public boolean crush() {
        return true;
    }

    @Override
    public boolean lost(int toId) {
        return false;
    }

    @Override
    public void delay(int toId) {

    }

    @Override
    public Object byzantine(int toId, Object msg) {
        return null;
    }
}
