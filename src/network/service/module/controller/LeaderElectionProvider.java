package network.service.module.controller;

import network.message.protocols.LeaderElectionMessage;

/**
 * @author : Swimiltylers
 * @version : 2019/4/27 14:51
 */

@FunctionalInterface
public interface LeaderElectionProvider {
    void provide(LeaderElectionMessage.LEOffer offer);
}
