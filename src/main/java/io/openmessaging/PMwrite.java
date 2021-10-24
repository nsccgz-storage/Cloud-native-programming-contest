package io.openmessaging;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.lang.ThreadLocal;
import com.intel.pmem.llpl.MemoryPool;


public class PMwrite {
    public static final Logger log = Logger.getLogger(PMwrite.class);
    int maxThreadNum;
    
    ExecutorService backgroundDoubleWriteThread;

    public long totalCapacity;
    public MemoryPool pool;
    public PMBlockPool pmBlockPool;

    PMwrite(String pmDataFile){
        log.setLevel(Level.INFO);
        // log.setLevel(Level.DEBUG);

        totalCapacity = 60L * 1024 * 1024 * 1024;
        pool = MemoryPool.createPool(pmDataFile, totalCapacity);
        pmBlockPool = new PMBlockPool(totalCapacity);

        // threadId < 50
        maxThreadNum = 50;
        // backgroundDoubleWriteThread = Executors.newSingleThreadExecutor();
        backgroundDoubleWriteThread = Executors.newFixedThreadPool(4);
       
    }
    public class PMBlock {
        public long addr;
        public int capacity;

        PMBlock(long a, int c) {
            addr = a;
            capacity = c;
        }
    }

    public class PMBlockPool {
        // 分为两个阶段
        // 阶段1：直接通过移动偏移量的方法申请内存
        // 依然是分大池子小池子的情况
        // 定长块管理，单个块的大小
        public int blockSize;
        // 线程内可以存放这么多个块
        public int threadLocalBlockNum;
        // 总容量
        public long totalCapacity;

        // 阶段1
        public int bigBlockSize;
        // 大池子
        public AtomicLong atomicGlobalFreeOffset;
        // 小池子
        public ThreadLocal<Long> threadLocalBigBlockStartAddr;
        public ThreadLocal<Integer> threadLocalBigBlockFreeOffset;
        PMBlockPool(long capacity) {
            totalCapacity = capacity;

            blockSize = 4*1024*1024; // 4MiB
            threadLocalBlockNum = 64;
            bigBlockSize = threadLocalBlockNum * blockSize; // 64*4MiB = 256MiB

            // 初始化阶段1
            // 大池子
            atomicGlobalFreeOffset = new AtomicLong();
            atomicGlobalFreeOffset.set(0L);
            // 小池子
            threadLocalBigBlockFreeOffset = new ThreadLocal<>();
            threadLocalBigBlockStartAddr = new ThreadLocal<>();
        }

        public PMBlock allocate() {
            if (threadLocalBigBlockStartAddr.get() == null || threadLocalBigBlockFreeOffset.get() >= bigBlockSize) {
                // 本地没有大块，或者大块满了
                if (atomicGlobalFreeOffset.get() >= totalCapacity) {
                    // 阶段1可以结束了
                    return null;
                }
                // 分配新的大块
                long bigBlockStartAddr = atomicGlobalFreeOffset.getAndAdd(bigBlockSize);
                threadLocalBigBlockStartAddr.set(bigBlockStartAddr);
                threadLocalBigBlockFreeOffset.set(0);
            }
            int freeOffset = threadLocalBigBlockFreeOffset.get();
            long addr = threadLocalBigBlockStartAddr.get() + freeOffset;
            threadLocalBigBlockFreeOffset.set(freeOffset + blockSize);

            return new PMBlock(addr, blockSize);
        }

    }
}
