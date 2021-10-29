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



public class PMwrite {
    public static final Logger log = Logger.getLogger(PMwrite.class);
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
    int maxThreadNum;
    
    ExecutorService backgroundDoubleWriteThread;

    public long totalCapacity;
    public MemoryPool pool;
    public PMBlockPool pmBlockPool;

    private long poolAddress;
    Method nativeCopyFromByteArrayNT;
    Method nativeCopyMemoryNT;

    PMwrite(String pmDataFile){
        log.setLevel(Level.INFO);
        // log.setLevel(Level.DEBUG);

        totalCapacity = 60L * 1024 * 1024 * 1024;
//        pool = MemoryPool.createPool(pmDataFile, totalCapacity);
        pmBlockPool = new PMBlockPool(totalCapacity);

        // threadId < 50
        maxThreadNum = 50;
        // backgroundDoubleWriteThread = Executors.newSingleThreadExecutor();
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

    public void unsafeCopyToByteArray(long srcOffset, byte[] dstArray, int dstIndex, int byteCount) {
        long dstAddress = Unsafe.ARRAY_BYTE_BASE_OFFSET + (long) Unsafe.ARRAY_BYTE_INDEX_SCALE * dstIndex;
        UNSAFE.copyMemory(null, poolAddress + srcOffset, dstArray, dstAddress, byteCount);
    }
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

    public class PMDirectByteBufferPool {
        public final Field byteBufferAddress;
        public final Field byteBufferCapacity;
        ByteBuffer[] dbs;
        int capacity;
        int cur;
        ByteBuffer baseByteBuffer;
    
    
        PMDirectByteBufferPool(){
            try {
                byteBufferAddress = Buffer.class.getDeclaredField("address");
                byteBufferAddress.setAccessible(true);
                byteBufferCapacity = Buffer.class.getDeclaredField("capacity");
                byteBufferCapacity.setAccessible(true);
            } catch (Exception e) {
            throw new RuntimeException(e);
            }


            capacity = 100;
            cur = 0;
            dbs = new ByteBuffer[capacity];
            // baseByteBuffer = ByteBuffer.allocateDirect(1);
            for (int i = 0; i < capacity; i++){
                // dbs[i] = baseByteBuffer.duplicate();
                dbs[i] = ByteBuffer.allocateDirect(0).order(ByteOrder.nativeOrder());
            }
        }
    
        ByteBuffer getNewPMDirectByteBuffer(long pmAddr, int dataLength){
            ByteBuffer buf = dbs[cur];
            try {
                byteBufferAddress.setLong(buf, poolAddress + pmAddr);
                byteBufferCapacity.setInt(buf, dataLength);
                buf.clear();
                buf.limit(dataLength);
                // System.out.println("ok!");
            } catch (Exception e){
                e.printStackTrace();
            }
            cur = (cur + 1) % capacity;
            return buf;
        }
    }

}
