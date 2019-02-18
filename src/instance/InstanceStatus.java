package instance;

import java.io.Serializable;

/**
 * @author : Swimiltylers
 * @version : 2019/2/14 18:35
 */
public enum InstanceStatus implements Serializable {
    PREPARING, PREPARED, ACCEPTED, COMMITTED
}
