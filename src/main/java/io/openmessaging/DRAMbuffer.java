package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

// 暂时没有找到很好的声明超大规模数组的方法
// 所以只能先暂时实现小的
public class DRAMbuffer {
    private static final Logger log = Logger.getLogger(DRAMbuffer.class);
    AtomicInteger freeAddr;
    // byte[] dramArray; 
    ByteBuffer dirByteBuffer;
    int capacity;
    public DRAMbuffer(){
        Level logLevel = Level.DEBUG;
        log.setLevel(logLevel);

        // 改用堆外内存分配
        // 由于不允许使用 unsafe 的分配方式，只能使用 directoryBuffer
        int bufferSize =  1 << 30 ; //  先分配 1 GiB
        // dramArray = new byte[bufferSize];
        dirByteBuffer = ByteBuffer.allocateDirect(bufferSize);
        capacity = bufferSize;
        freeAddr = new AtomicInteger(0);
    }
    public Block allocate(){
        int blockSize = 1 << 20; // 1 MiB
        int startAddr = this.freeAddr.getAndAdd(blockSize);
        if(startAddr > capacity) return null;
        ByteBuffer locaBuffer = dirByteBuffer.duplicate();
        locaBuffer.position(startAddr);
        locaBuffer.limit(startAddr + blockSize);
        locaBuffer = locaBuffer.slice();
        return new Block(blockSize, locaBuffer);
    }

    public class Block{
        int startAddr;
        int endAddr;
        // int freeAddr;
        private ByteBuffer localByteBuffer;

        public Block(int blockSize, ByteBuffer localByteBuffer){
            this.startAddr = 0;
            this.endAddr = this.startAddr + blockSize;
            this.localByteBuffer = localByteBuffer;
            // this.freeAddr = startAddr;  
        }
        public int put(ByteBuffer data){
            int dataSize = data.remaining();
            if(dataSize > endAddr - startAddr) return -1; // 不够了，直接放弃写

            // 把数据 data 写入进 dramArray 的从 startAddr, startAddr + dataSize
            // byte[] tmp = data.array();
            // for(int i=startAddr; i<startAddr + dataSize; i++){
            //     // dramArray[i] = tmp[i-startAddr];
            // }
            // log.debug("position: " + startAddr + " limit:  " + startAddr + dataSize);
            localByteBuffer.position(startAddr);
            localByteBuffer.limit(startAddr + dataSize);

            localByteBuffer.put(data);
            startAddr += dataSize;
            return startAddr - dataSize;
        }

        public int read(ByteBuffer dst, int dataSize, int startAddr){
            if(startAddr < 0) return -1;
            // int position = dst.position();
            // byte[] tmp = dst.array();
            // for(int i=position; i<dataSize; i++){
            //    // tmp[i] = dramArray[startAddr + i];
            // }
            localByteBuffer.position(startAddr);
            localByteBuffer.limit(startAddr + dataSize);
            dst.put(localByteBuffer);

            return 0;
        }
        public String toString(){
            return "startAddr: " + this.startAddr + " endAddr： " + this.endAddr; 
        }
    }
}
