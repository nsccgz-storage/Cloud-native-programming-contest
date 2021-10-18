package io.openmessaging;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.StandardOpenOption;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.io.RandomAccessFile;
import java.util.concurrent.Future;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.IntUnaryOperator;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.ArrayDeque;
import java.util.ArrayList;

import org.apache.log4j.spi.LoggerFactory;

import io.openmessaging.LSMessageQueue.MyByteBufferPool;
import io.openmessaging.SSDqueue.HotData;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.lang.ThreadLocal;
import java.util.concurrent.Callable;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.Math;
import java.text.Format;
import java.util.concurrent.TimeoutException;

import java.util.concurrent.locks.Condition;
import java.nio.MappedByteBuffer;
import java.util.Deque;

import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryBlock;
import com.intel.pmem.llpl.MemoryPool;


import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;
import java.lang.reflect.Field;



import java.util.Comparator;

public class PMDoubleWrite {
    public static final Logger log = Logger.getLogger(PMDoubleWrite.class);
	public static final Unsafe UNSAFE;
	static {
	    try {
		Field field = Unsafe.class.getDeclaredField("theUnsafe");
		field.setAccessible(true);
		UNSAFE = (Unsafe) field.get(null);
	    } catch (Exception e) {
		throw new RuntimeException(e);
	    }
	}
    public static void copyFromDirectBufferToPM(DirectBuffer srcBuf, MemoryPool dstPool, long PMAddr, int length){
        // TODO: 实现DirectBuffer 复制到 PM （llpl原来不支持这个，不过可以自己实现）
        // long addr = srcBuf.address();
        // Field pmAddrField = dstPool.getClass().getDeclaredField("poolAddress");
        // pmAddrField.setAccessible(true);
        // long pmPoolAddr = pmAddrField.get(dstPool);
        // UNSAFE.copyMemory(srcBase, srcOffset, destBase, destOffset, bytes);
        return ;
    }

    //TODO: 是否会有cache伪共享问题？，是否要加padding
    public class ThreadData{
        ByteBuffer buf[];
        int curBuf;
        PMBlock block;
        boolean isFinished;
        Future<Integer> backgroundDoubleWriteFuture;
        ThreadData(){
            buf = new ByteBuffer[2];
            curBuf = 0;
            buf[0] = ByteBuffer.allocate(4*1024*1024);  // 4MB
            buf[1] = ByteBuffer.allocate(4*1024*1024);
            isFinished = false;
        }

    }
    ThreadData[]  threadDatas;
    int maxThreadNum;
    ExecutorService backgroundDoubleWriteThread;

    public long totalCapacity;
    public MemoryPool pool;
    public PMBlockPool pmBlockPool;



    PMDoubleWrite(String pmDataFile){
        log.setLevel(Level.INFO);
        // log.setLevel(Level.DEBUG);

        totalCapacity = 60L * 1024 * 1024 * 1024;
        pool = MemoryPool.createPool(pmDataFile, totalCapacity);
        pmBlockPool = new PMBlockPool(totalCapacity);

        // threadId < 50
        maxThreadNum = 50;
        threadDatas = new ThreadData[maxThreadNum];
        for (int i = 0; i < maxThreadNum; i++){
            threadDatas[i] = new ThreadData();
        }
        // backgroundDoubleWriteThread = Executors.newSingleThreadExecutor();
        backgroundDoubleWriteThread = Executors.newFixedThreadPool(4);
       
    }

    public long doubleWrite(int threadId, ByteBuffer data){
        // 双写，返回PM地址
        ThreadData td = threadDatas[threadId]; // 一个线程内的数据结构？
        if (td.isFinished == true){ 
            return -1;
        }
        
        // 判断是否有空位写，如果没有，触发刷盘任务并切换buf
        if (td.buf[td.curBuf].remaining() < data.remaining()){
            // 确认刷PM任务完成
            if (td.backgroundDoubleWriteFuture != null){
                while (td.backgroundDoubleWriteFuture.isDone() != true){
                    try {
                        Thread.sleep(1);
                    } catch (Exception e){
                        e.printStackTrace();
                    }
                }
                td.backgroundDoubleWriteFuture = null;
            }
            // 触发刷盘任务，异步调用block的刷盘函数
            final PMBlock backgroundBlock = td.block;
            final int flushBuf = td.curBuf;
            td.backgroundDoubleWriteFuture = backgroundDoubleWriteThread.submit(new Callable<Integer>(){
                @Override
                public Integer call() throws Exception {
                    pool.copyFromByteArrayNT(td.buf[flushBuf].array(), 0, backgroundBlock.addr , backgroundBlock.capacity);
                    return 0;
                }
            });

            // 申请新block
            // 如果没法申请了，就停了，不双写了
            // TODO: 没法申请了，那就直接粗暴地写入 DRAM 吧！反正 6G 的资源没用满
            td.block = pmBlockPool.allocate();
            if (td.block == null){
                td.isFinished = true;
                log.info("the pm is full!!");
                return -1;
            }
            // 切换缓冲区
            // td.curBuf = (td.curBuf + 1 ) % 2;
            td.curBuf = (td.curBuf + 1) & 1;
            td.buf[td.curBuf].clear();
        }
        if (td.block == null){
            td.block = pmBlockPool.allocate();
            if (td.block == null){ // 满了
                td.isFinished = true;
                log.info("the pm is full!!");
                return -1;
            }
        }
        // 获得当前地址
        ByteBuffer buf = td.buf[td.curBuf];
        long pmAddr = td.block.addr + buf.position();
        buf.put(data);
        return pmAddr;

    }

    public void shutdown(){
        // 把目前的buffer全部写到PM到，并且结束双写
        for (int i = 0; i < maxThreadNum; i++){
            ThreadData td = threadDatas[i];
            if (td.block != null){
                td.isFinished = true;
                log.info("shutdown the double write for thread " + i);
                // 确认刷PM任务完成
                if (td.backgroundDoubleWriteFuture != null){
                    while (td.backgroundDoubleWriteFuture.isDone() != true){
                        try {
                            Thread.sleep(1);
                        } catch (Exception e){
                            e.printStackTrace();
                        }
                    }
                    td.backgroundDoubleWriteFuture = null;
                }
                // 触发刷盘任务，异步调用block的刷盘函数
                final PMBlock backgroundBlock = td.block;
                final ByteBuffer finalBuf = td.buf[td.curBuf];
                pool.copyFromByteArrayNT(finalBuf.array(), 0, backgroundBlock.addr , backgroundBlock.capacity);
                td.block = null;
            }
        }
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