package client.grs;

import instance.InstanceStatus;

/**
 * @author : Swimiltylers
 * @version : 2019/4/28 20:05
 */
public interface GRSMessageReporter {
    void report(String request, InstanceStatus status);
}
