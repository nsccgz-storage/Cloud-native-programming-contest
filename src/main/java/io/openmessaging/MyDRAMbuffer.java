package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class MyDRAMbuffer {
    private static final Logger log = Logger.getLogger(MyDRAMbuffer.class);

    int capacity;
    int slotSize;
    int slotNum;
    Queue<Integer> addrPool;

    ByteBuffer dirBuffer;

    public MyDRAMbuffer(){
        slotNum = 1542;
        // slotNum = 10;
        slotSize = 1024 * 17; // 17 KiB
        capacity = slotSize * slotNum; // 17 KiB * 1542 * 40 = 1.002 GiB

        dirBuffer = ByteBuffer.allocateDirect(capacity);

        addrPool = new LinkedList<>();
        for(int i=0; i<slotNum; i++){
            addrPool.offer(i);
        }

    }
    public int put(ByteBuffer data){
        if(addrPool.isEmpty()){
            // log.info("full from dram buffer!");
            return -1;
        }
        int addr = addrPool.poll();
        int dataSize = data.remaining();
        dirBuffer.limit(addr * slotSize + dataSize);
        dirBuffer.position(addr * slotSize);
        
        dirBuffer.put(data);
        return addr;
    }

    public void free(int addr){
        addrPool.offer(addr);
    }

    public void read(ByteBuffer dst, int addr, int dataSize){ // 默认 dst 是外面分配好的
        int startAddr = addr * slotSize;
        dirBuffer.limit(startAddr + dataSize);
        dirBuffer.position(startAddr);  
        dst.put(dirBuffer);

        // 默认 free
        free(addr);
    }

    public ByteBuffer read(int addr, int dataSize){ // 默认 dst 是外面分配好的
        int startAddr = addr * slotSize;
       
        ByteBuffer res = dirBuffer.duplicate();
        res.limit(startAddr + dataSize);
        res.position(startAddr);  
        // res = res.slice();
        // 默认 free
        free(addr);

        return res;
    }

}
