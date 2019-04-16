package agent.recovery;

/**
 * @author : Swimiltylers
 * @version : 2019/4/16 21:40
 */

@FunctionalInterface
public interface LeaderElectionResultUpdater {
    void update(long token, int leaderId);
}
