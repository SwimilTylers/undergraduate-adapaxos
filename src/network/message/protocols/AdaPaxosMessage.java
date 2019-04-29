package network.message.protocols;

import java.io.Serializable;

/**
 * @author : Swimiltylers
 * @version : 2019/3/8 13:46
 */
public class AdaPaxosMessage implements Serializable {
    private static final long serialVersionUID = 948019606028744210L;

    public static class StateTransformation extends AdaPaxosMessage{
        private static final long serialVersionUID = 8912551268626823875L;
        public final boolean fsync;
        public final int upto;

        public StateTransformation(boolean fsync, int upto) {
            this.fsync = fsync;
            this.upto = upto;
        }

        @Override
        public String toString() {
            return "[STATE TRANS][fsync="+fsync+",upto="+upto+"]";
        }
    }

    public static class SyncInitFsync extends AdaPaxosMessage{
        private static final long serialVersionUID = -9122235921968320752L;
        public final int initFsync;

        public SyncInitFsync(int initFsync) {
            this.initFsync = initFsync;
        }

        @Override
        public String toString() {
            return "[INIT_FSYNC][initFsync="+initFsync+"]";
        }
    }


}
