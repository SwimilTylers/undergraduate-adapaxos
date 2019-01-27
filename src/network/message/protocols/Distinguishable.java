package network.message.protocols;

/**
 * @author : Swimiltylers
 * @version : 2019/1/27 14:03
 */

@FunctionalInterface
public interface Distinguishable {
    boolean meet(Object o);
}
