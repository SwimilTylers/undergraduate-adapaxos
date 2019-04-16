package agent.recovery;

/**
 * @author : Swimiltylers
 * @version : 2019/4/13 22:19
 */

@FunctionalInterface
public interface VacantInstanceUpdater {
    void update(long token, int vacant_no);
}
