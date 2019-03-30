package agent.learner;

/**
 * @author : Swimiltylers
 * @version : 2019/3/30 21:47
 */
@FunctionalInterface
public interface CommitUpdater {
    void update(int commit_no);
}
