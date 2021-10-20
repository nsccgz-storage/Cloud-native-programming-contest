package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class MyDRAMbuffer {
    private static final Logger log = Logger.getLogger(MyDRAMbuffer.class);
     
    DRAMbuffer buffer0; // small
    DRAMbuffer buffer1; // large

    public MyDRAMbuffer(){
        buffer0 = new DRAMbuffer(1024, 8 * 1024); // 
        buffer1= new DRAMbuffer(1024, 17 * 1024);
    }
    
    public int put(ByteBuffer data){
        // 负数代表是从 buffer1 分配的，其中从为了使 -1 表示不能分配，所以负值 = - 原本值 - 2
        if(data.remaining() <= buffer0.slotSize){
            int addr = buffer0.put(data);
            if(addr != -1) return addr;
        }
        int addr = buffer1.put(data);
        return  addr == -1 ? addr : -addr - 2;
    }
    public ByteBuffer read(int addr, int dataSize){
        if(addr == -1) return null;
        return addr < 0 ? buffer1.read(-(addr + 2), dataSize) : buffer0.read(addr, dataSize);
    }

    class DRAMbuffer{
        ByteBuffer dirBuffer;
        int capacity;
        int slotSize;
        int slotNum;
        Queue<Integer> addrPool;
        public DRAMbuffer(int slotNum, int slotSize){
            this.slotNum = slotNum;
            // slotNum = 10;
            this.slotSize = slotSize; // 17 KiB
            this.capacity = slotSize * slotNum; // 17 KiB * 1542 * 40 = 1.002 GiB
    
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
}
