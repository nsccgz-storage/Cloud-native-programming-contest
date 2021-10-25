package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Vector;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class MyDRAMbuffer {
    private static final Logger log = Logger.getLogger(MyDRAMbuffer.class);

    int[] addr2buffer;
    DRAMbuffer[] bufferArray;


    public String toString() {
        StringBuilder str = new StringBuilder("");
        for (int i = 0; i < bufferArray.length; i++) {
            str.append(" |buffer").append(i).append(": ").append(bufferArray[i].addrPool.size());
        }
        return str.toString();
    }
    public MyDRAMbuffer(){

        addr2buffer = new int[]{0, 800, 1600, 2400, 3400};
        bufferArray = new DRAMbuffer[]{
                new DRAMbuffer(800, 4 * 1024),
                new DRAMbuffer(800, 8 * 1024),
                new DRAMbuffer(800, 12 * 1024),
                new DRAMbuffer(1000 , 17 * 1024)};
    }
    
    public int put(ByteBuffer data){
        // 地址展开:
        int dataSize = data.remaining();
        int addr = -1;
        for(int i=0; i<4; i++){
            if(dataSize <= bufferArray[i].slotSize){
                addr = bufferArray[i].put(data);
                if(addr != -1) return addr + addr2buffer[i];
            }
        }
        return addr;
    }
    public ByteBuffer read(int addr, int dataSize){
        if(addr == -1) return null;
        for(int i=0 ;i<4; i++){
            if(addr < addr2buffer[i+1]){
                return bufferArray[i].read(addr - addr2buffer[i], dataSize);
            }
        }
        return null;
    }

    class DRAMbuffer{
        ByteBuffer dirBuffer;
        int capacity;
        int slotSize;
        int slotNum;
        RingArray addrPool;
        public DRAMbuffer(int slotNum, int slotSize){
            this.slotNum = slotNum;
            this.slotSize = slotSize;
            this.capacity = slotSize * slotNum;
    
            dirBuffer = ByteBuffer.allocateDirect(capacity);

            addrPool = new RingArray(slotNum);
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
    
        public ByteBuffer read(int addr, int dataSize){
            int startAddr = addr * slotSize;
           
            ByteBuffer res = dirBuffer.duplicate();
            res.limit(startAddr + dataSize);
            res.position(startAddr);
            // 默认 free
            free(addr);
    
            return res;
        }
    }

    class RingArray{
        int size;
        int[] addr;
        int head;
        int tail;
        public RingArray(int s){
            size = s+1;
            addr = new int[s+1];
            for(int i=0; i<s; i++){
                addr[i] = i;
            }
            head = 0;
            tail = s;
        }

        boolean isEmpty(){
            return head == tail;
        }

        int poll(){ // 没有边界检查
            int ret = addr[head];
            head = (head + 1) % size;
            return ret;
        }

        void offer(int v){
            addr[tail] = v;
            tail = (tail + 1) % size;
        }

        int size(){
            return (head + size - tail) % size;
        }
    }
}
