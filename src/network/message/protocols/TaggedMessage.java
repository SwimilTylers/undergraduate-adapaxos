package network.message.protocols;

import java.io.Serializable;

/**
 * @author : Swimiltylers
 * @version : 2019/3/27 21:38
 */
public class TaggedMessage implements Serializable {
    private static final long serialVersionUID = -4721383249994025033L;
    public final long tag;
    public final Object load;

    public TaggedMessage(long tag, Object load) {
        this.tag = tag;
        this.load = load;
    }
}
