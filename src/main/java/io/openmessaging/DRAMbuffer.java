package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
// 暂时没有找到很好的声明超大规模数组的方法
// 所以只能先暂时实现小的
public class DRAMbuffer {
    AtomicInteger freeAddr;
    byte[] dramArray; 
    int capacity;
    public DRAMbuffer(){
        int bufferSize = 1 * 1024 * 1024 * 1024;
        dramArray = new byte[bufferSize];
        capacity = bufferSize;
        freeAddr = new AtomicInteger(0);
    }
    public Block allocate(){
        int blockSize = 1*1024*1024;
        int startAddr = this.freeAddr.getAndAdd(blockSize);
        if(startAddr > capacity) return null;
        return new Block(startAddr, blockSize);
    }

    public class Block{
        int startAddr;
        int endAddr;
        // int freeAddr;

        public Block(int startAddr, int blockSize){
            this.startAddr = startAddr;
            this.endAddr = this.startAddr + blockSize;
            // this.freeAddr = startAddr;  
        }
        public int put(ByteBuffer data){
            
            int dataSize = data.remaining();
            if(dataSize > endAddr - startAddr) return -1; // 不够了，直接放弃写

            // 把数据 data 写入进 dramArray 的从 startAddr, startAddr + dataSize
            byte[] tmp = data.array();
            for(int i=startAddr; i<startAddr + dataSize; i++){
                dramArray[i] = tmp[i-startAddr];
            }
            startAddr += dataSize;
            return startAddr - dataSize;
        }
        public int read(ByteBuffer dst, int dataSize, int startAddr){
            if(startAddr < 0) return -1;
            int position = dst.position();
            byte[] tmp = dst.array();
            for(int i=position; i<dataSize; i++){
                tmp[i] = dramArray[startAddr + i];
            }
            return 0;
        }
        public String toString(){
            return "startAddr: " + this.startAddr + " endAddr： " + this.endAddr; 
        }
    }
}
