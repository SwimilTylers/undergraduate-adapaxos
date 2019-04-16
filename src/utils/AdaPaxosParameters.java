package utils;

/**
 * @author : Swimiltylers
 * @version : 2019/3/24 15:09
 */
public class AdaPaxosParameters {
    public static class RSM{
        public static final int DEFAULT_BATCH_CHAN_SIZE = 1;
        public static final int DEFAULT_INSTANCE_SIZE = 1024;
        public static final String DEFAULT_LOCAL_STORAGE_PREFIX = "disk-";
        public static final int DEFAULT_MESSAGE_SIZE = 32;
        public static final int DEFAULT_LINK_STABLE_WAITING = 500;
    }
}
