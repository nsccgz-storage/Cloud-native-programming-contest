package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.Map;

public class SSDqueue2 {
    

    void recovery(){
        
    }

    public long append(String topicName, int queueId, ByteBuffer data){
        // meta append
        
        // data append

        // update data -> meta data
    }
    
    public Map<Integer, ByteBuffer> getRange(Long offset, int fetchNum) 
        // get meta

        // read data
    }


}
