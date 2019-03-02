package network.service.module.simulator;

/**
 * @author : Swimiltylers
 * @version : 2019/2/26 20:55
 */
public interface SimulatorModule {
    boolean crush();
    boolean lost(int toId);
    void delay(int toId);
    Object byzantine(int toId, Object msg);
}
