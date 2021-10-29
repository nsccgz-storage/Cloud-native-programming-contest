package io.openmessaging;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.*;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.lang.ThreadLocal;
import java.util.concurrent.Callable;

import com.intel.pmem.llpl.MemoryPool;


import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;
import java.lang.reflect.Field;

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

    //TODO: 是否会有cache伪共享问题？，是否要加padding
    public class ThreadData{
        ByteBuffer buf[];
        long bufAddr[];
        int curBuf;
        PMBlock block;
        boolean isFinished;
        Future<Integer> backgroundDoubleWriteFuture;
        ThreadData(){
            buf = new ByteBuffer[2];
            bufAddr = new long[2];
            curBuf = 0;
            // buf[0] = ByteBuffer.allocate(4*1024*1024);  // 4MB
            // buf[1] = ByteBuffer.allocate(4*1024*1024);
            for (int i = 0; i < 2; i++){
                buf[i] = ByteBuffer.allocateDirect(2*1024*1024);
                bufAddr[i] = ((DirectBuffer)buf[i]).address();
            }

            isFinished = false;
        }
    }
    ThreadData[]  threadDatas;
    int maxThreadNum;
    ExecutorService backgroundDoubleWriteThread;

    public long totalCapacity;
    public MemoryPool pool;
    public PMBlockPool pmBlockPool;

    private long poolAddress;
    Method nativeCopyFromByteArrayNT;
    Method nativeCopyMemoryNT;

    PMDoubleWrite(String pmDataFile){
        log.setLevel(Level.INFO);
        // log.setLevel(Level.DEBUG);

        totalCapacity = 60L * 1024 * 1024 * 1024;
//        pool = MemoryPool.createPool(pmDataFile, totalCapacity);
        pmBlockPool = new PMBlockPool(totalCapacity);

        // threadId < 50
        maxThreadNum = 50;
        threadDatas = new ThreadData[maxThreadNum];
        for (int i = 0; i < maxThreadNum; i++){
            threadDatas[i] = new ThreadData();
        }
        backgroundDoubleWriteThread = Executors.newFixedThreadPool(4);

        try {

            Class<?> memoryPoolClass = Class.forName("com.intel.pmem.llpl.MemoryPoolImpl");
            Constructor<?> constructor = memoryPoolClass.getDeclaredConstructor(String.class, long.class);
            constructor.setAccessible(true);
            Object obj = constructor.newInstance(pmDataFile, totalCapacity);
            Field field = memoryPoolClass.getDeclaredField("poolAddress");
            field.setAccessible(true);
            this.poolAddress = (long) field.get(obj);
            this.pool = (MemoryPool) obj;

            nativeCopyFromByteArrayNT = memoryPoolClass.getDeclaredMethod(
                    "nativeCopyFromByteArrayNT",byte[].class, int.class, long.class, int.class);
            nativeCopyFromByteArrayNT.setAccessible(true);

            nativeCopyMemoryNT = memoryPoolClass.getDeclaredMethod(
                    "nativeCopyMemoryNT", long.class, long.class, long.class);
            nativeCopyMemoryNT.setAccessible(true);

        }catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException |
                InvocationTargetException | NoSuchFieldException e){
            log.info(e);
        }

        // iterate pmem space, reducing page fault during write and read
        this.pool.setMemoryNT((byte)0, 0, 60L*1024L*1024L*1024L);
    }

    public long doubleWrite(int threadId, ByteBuffer data){
        // 双写，返回PM地址
        ThreadData td = threadDatas[threadId]; // 一个线程内的数据结构？
        if (td.isFinished){
            return -1;
        }
        if (td.block == null){
            td.block = pmBlockPool.allocate();
            if (td.block == null){ // 满了
                td.isFinished = true;
                log.info("the pm is full!!");
                return -1;
            }
        }
        // 判断是否有空位写，如果没有，触发刷盘任务并切换buf
        if (td.buf[td.curBuf].remaining() < data.remaining()){
            // 确认刷PM任务完成
            if (td.backgroundDoubleWriteFuture != null){
                while (!td.backgroundDoubleWriteFuture.isDone()){
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
            final long flushBufAddr= td.bufAddr[td.curBuf];
            td.backgroundDoubleWriteFuture = backgroundDoubleWriteThread.submit(new Callable<Integer>(){
                @Override
                public Integer call() throws Exception {
                   copyMemoryNT(flushBufAddr, backgroundBlock.addr, backgroundBlock.capacity);
                    // copyFromByteArrayNT(td.buf[flushBuf].array(), 0, backgroundBlock.addr , backgroundBlock.capacity);
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
                    while (!td.backgroundDoubleWriteFuture.isDone()){
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
                // final ByteBuffer finalBuf = td.buf[td.curBuf];
                final long flushBufAddr= td.bufAddr[td.curBuf];
               this.copyMemoryNT(flushBufAddr, backgroundBlock.addr, backgroundBlock.capacity);
                // this.copyFromByteArrayNT(finalBuf.array(), 0, backgroundBlock.addr , backgroundBlock.capacity);
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

            blockSize = 2*1024*1024; // 4MiB
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

    public void unsafeCopyToByteArray(long srcOffset, byte[] dstArray, int dstIndex, int byteCount) {
        long dstAddress = Unsafe.ARRAY_BYTE_BASE_OFFSET + (long) Unsafe.ARRAY_BYTE_INDEX_SCALE * dstIndex;
        UNSAFE.copyMemory(null, poolAddress + srcOffset, dstArray, dstAddress, byteCount);
    }

//    public static void copyFromDirectBufferToPM(DirectBuffer srcBuf, MemoryPool dstPool, long PMAddr, int length){
//        // TODO: 实现DirectBuffer 复制到 PM （llpl原来不支持这个，不过可以自己实现）
////         long addr = srcBuf.address();
//        // Field pmAddrField = dstPool.getClass().getDeclaredField("poolAddress");
//        // pmAddrField.setAccessible(true);
//        // long pmPoolAddr = pmAddrField.get(dstPool);
////         UNSAFE.copyMemory(srcBase, srcOffset, destBase, destOffset, bytes);
//    }
    public void copyPM2MemoryNT(long srcBufAddr, long dstOffset, int byteCount){
        try {
            nativeCopyMemoryNT.invoke(null, poolAddress + srcBufAddr, dstOffset, byteCount);
        }catch (Exception e){
            e.printStackTrace();
        }
    }



    public void copyMemoryNT(long srcBufAddr, long dstOffset, int byteCount){
        try {
            nativeCopyMemoryNT.invoke(null, srcBufAddr, poolAddress+dstOffset, byteCount);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public void copyFromByteArrayNT(byte[] srcArray, int srcIndex, long dstOffset, int byteCount) {
        try {
            nativeCopyFromByteArrayNT.invoke(null, srcArray, srcIndex, poolAddress + dstOffset, byteCount);
        }catch (InvocationTargetException | IllegalAccessException e){
            log.info(e);
        }
    }
}