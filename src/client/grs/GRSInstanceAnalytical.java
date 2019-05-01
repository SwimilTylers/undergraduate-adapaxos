package client.grs;

import instance.InstanceStatus;
import javafx.util.Pair;

import java.util.List;

/**
 * @author : Swimiltylers
 * @version : 2019/4/29 19:24
 */
public interface GRSInstanceAnalytical {
    List<Pair<String, InstanceStatus>> analyze();
}
