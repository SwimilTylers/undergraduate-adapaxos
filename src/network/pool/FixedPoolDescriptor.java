package network.pool;

import javafx.util.Pair;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;

/**
 * @author : Swimiltylers
 * @version : 2019/1/21 10:40
 */
@Deprecated
public class FixedPoolDescriptor {
    public int fetchRegPort(String id){
        return 0;
    }

    public int fetchComPort(String id){
        return 0;
    }

    public int fetchInfoRegPort(String id){
        return 0;
    }

    public int fetchInfoComPort(String id){
        return 0;
    }

    public int getAcceptorSize(){
        return 0;
    }

    public Set<Pair<InetAddress, Integer>> fetchProposerAddresses(){
        return new HashSet<>();
    }

    public Set<Pair<InetAddress, Integer>> fetchLearnerAddresses(){
        return new HashSet<>();
    }
}
