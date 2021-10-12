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

import io.openmessaging.SSDBench;

import java.util.Comparator;

public class PMPrefetchBuffer {
	private static final Logger log = Logger.getLogger(PMPrefetchBuffer.class);

	public class MyPMBlockPool {
		public MemoryPool pool;
		public int blockSize;
		public int bigBlockSize;
		public AtomicLong atomicGlobalFreeOffset;
		public long totalCapacity;
		public ThreadLocal<Long> threadLocalBigBlockStartAddr;
		public ThreadLocal<Integer> threadLocalBigBlockFreeOffset;

		MyPMBlockPool(String path) {
			totalCapacity = 60L * 1024 * 1024 * 1024;
			pool = MemoryPool.createPool(path, totalCapacity);

			blockSize = 9 * 17 * 1024; // 8 个slot
			bigBlockSize = 200 * blockSize;
			atomicGlobalFreeOffset = new AtomicLong();
			atomicGlobalFreeOffset.set(0L);
			threadLocalBigBlockFreeOffset = new ThreadLocal<>();
			threadLocalBigBlockStartAddr = new ThreadLocal<>();
			threadLocalBigBlockFreeOffset.set(0);
		}

		public MyPMBlock allocate() {
			if (threadLocalBigBlockStartAddr.get() == null) {
				allocateBigBlock();
			}
			if (threadLocalBigBlockFreeOffset.get() == bigBlockSize) {
				// 大块满了，分配新的大块
				allocateBigBlock();
			}
			int freeOffset = threadLocalBigBlockFreeOffset.get();
			long addr = threadLocalBigBlockStartAddr.get() + freeOffset;
			threadLocalBigBlockFreeOffset.set(freeOffset + blockSize);

			return new MyPMBlock(pool, addr, blockSize);
		}

		public void allocateBigBlock() {
			long bigBlockStartAddr = atomicGlobalFreeOffset.getAndAdd(bigBlockSize);
			log.debug("big block addr : " + bigBlockStartAddr);
			threadLocalBigBlockStartAddr.set(bigBlockStartAddr);
			threadLocalBigBlockFreeOffset.set(0);
			assert (atomicGlobalFreeOffset.get() < totalCapacity);
		}
	}

	public class MyPMBlock {
		public MemoryPool pool;
		public long addr;
		public int capacity;

		MyPMBlock(MemoryPool p, long a, int c) {
			pool = p;
			addr = a;
			capacity = c;
		}

		public void copyFromArray(byte[] srcArray, int srcIndex, long dstOffset, int length) {
			pool.copyFromByteArrayNT(srcArray, srcIndex, addr + dstOffset, length);

		}

		public void copyToArray(long srcOffset, byte[] dstArray, int dstOffset, int length) {
			pool.copyToByteArray(addr + srcOffset, dstArray, dstOffset, length);
		}
	}

    public static class PMBlockPool {
        // 定长buffer管理器
        public PMBlock allocate(){
            return null;
        }
        public void free(PMBlock block){
            return ;
        }
    }
    public static PMBlockPool pmBlockPool = new PMBlockPool();

    public class RingBuffer {
        MemoryPool pool;
        int headBlock;
        int tailBlock;
        int headBlockAddr;
        int tailBlockAddr;
        int maxBlockNum;
        int curBlockNum;
        PMBlock[] blocks;

        int head;
        int tail;
        int length;
        int maxLength;
        public int[] msgsLength;
        public int[] msgsBlockAddr;
        // public int[] msgsBlock;



        RingBuffer(){
            curBlockNum = 1;
            maxBlockNum = 4;
            maxLength = 100;
            blocks = new PMBlock[maxBlockNum];
            for (int i = 0; i < curBlockNum; i++){
                blocks[i] = pmBlockPool.allocate();
            }
            head = 0;
            tail = 0;
            headBlock = 0;
            tailBlock = 0;
            headBlockAddr = 0;
            tailBlockAddr = 0;
            curBlockNum = 0;
            msgsLength = new int[maxLength];
            msgsBlockAddr = new int[maxLength];
            // msgsBlock = new int[maxLength];
        }

        public boolean offer(ByteBuffer data){
            data = data.duplicate();
            int dataLength = data.remaining();
            long savePMAddr = -1;
            int saveBlock = -1;
            int saveBlockAddr = -1;
            if (isFull()){
                return false;
            }

            allocate:   {
                if ((headBlock != tailBlock) || (headBlockAddr <= tailBlockAddr)){
                    // 最正常的情况，在tail那一块直接放
                    // 无论是head < tail 还是 head > tail 都是这样
                    if ( dataLength < (blocks[tailBlock].capacity - tailBlockAddr)){
                        saveBlock = tailBlock;
                        saveBlockAddr = tailBlockAddr;

                        savePMAddr = blocks[tailBlock].addr + tailBlockAddr;
                        tailBlockAddr += dataLength;
                        // return ret;
                        break allocate;
                    }
                    // tail 不够放，这个时候要放下一个，有两种情况
                    int nextTailBlock = ( tailBlock + 1 ) % curBlockNum;
                    if (nextTailBlock != headBlock){
                        // 1. 说明还有空闲的block，那就用这个空闲的block
                        saveBlock = tailBlock;
                        saveBlockAddr = tailBlockAddr;

                        tailBlock = nextTailBlock;
                        tailBlockAddr = 0;
                        savePMAddr = blocks[tailBlock].addr + tailBlockAddr;
                        tailBlockAddr += dataLength;
                        // return ret;
                        break allocate;
                    } else {
                        // 2. 没有空闲的block了，退化成 headBlock == tailBlock 的情况
                        tailBlock = nextTailBlock;
                        tailBlockAddr = 0;
                    }
                }
                // 后面的代码处理 headBlock == tailBlock 且 tailBlockAddr < headBlockAddr 的情形
                // tail 追上 head 的时候，看看tail和head之间的空隙能否放数据
                if (dataLength < (headBlockAddr-tailBlockAddr)){
                    // 能放
                    saveBlock = tailBlock;
                    saveBlockAddr = tailBlockAddr;


                    savePMAddr = blocks[tailBlock].addr + tailBlockAddr;
                    tailBlockAddr += dataLength;
                    // return ret;
                    break allocate;
                }
            }
            if (savePMAddr == -1){
                // 申请不到
                return false;
            }
            // 申请到了
            if (isEmpty()){
                msgsBlockAddr[tail] = saveBlockAddr;
                msgsLength[tail] = dataLength;
                pool.copyFromByteArray(data.array(), data.arrayOffset() + data.position(), savePMAddr, dataLength);
                length ++;
                return true;
            }
            tail = (tail + 1) % maxLength;
            msgsBlockAddr[tail] = saveBlockAddr;
            msgsLength[tail] = dataLength;
            pool.copyFromByteArray(data.array(), data.arrayOffset() + data.position(), savePMAddr, dataLength);
            length ++;
            return true;
        }
        public ByteBuffer poll(){
            if (isEmpty()){
                return null;
            }
            int dataLength = msgsLength[head];
            int msgBlockAddr = msgsBlockAddr[head];
            long msgPMAddr = blocks[headBlock].addr + headBlockAddr;
            ByteBuffer data = ByteBuffer.allocate(dataLength);
            pool.copyToByteArray(msgPMAddr, data.array(), data.arrayOffset()+data.position(), dataLength);
            // free the head
            if (msgBlockAddr == 0){
                // 说明上次free的时候上一个block已经free完啦，应该把head切换到下一个block来
                headBlock = (headBlock + 1 ) % curBlockNum;
                headBlockAddr = dataLength;
            }

            if (length == 1){
                length --;
                return data;
            }
            head = ( head + 1 ) % maxLength;
            length --;
            return data;
        }

        public boolean isFull(){
            return length == maxLength;
        }
        public boolean isEmpty(){
            return length == 0;
        }

        public void addBlock(){
            PMBlock block = pmBlockPool.allocate();
            // 两种情况
            // 情况1
            if ((headBlock < tailBlock) || (headBlock == tailBlock && headBlockAddr <= tailBlockAddr)) {
                // 可以直接将队列向后延伸
                blocks[curBlockNum] = block;
                curBlockNum += 1;
                return ;
            }
            if ( headBlock > tailBlock ){
                // 可以将新增的块放到 tailBlock 和 headBlock 之间
                // 准确的说，是headBlock之前
                // 然后把 headBlock以及之后的块整体向后移动
                for (int i = curBlockNum; i >= headBlock; i--){
                    blocks[i] = blocks[i-1];
                }
                int newBlockIndex = headBlock;
                blocks[newBlockIndex] = block;
                headBlock += 1;
                return ;
            }
            // headBlock == tailBlock && headBlockAddr > tailBlock
            // 可以将新增的块放到 tailBlock 和 headBlock 之间
            // 准确的说，是headBlock之前
            // 然后把 headBlock以及之后的块整体向后移动
            for (int i = curBlockNum; i >= headBlock; i--){
                blocks[i] = blocks[i-1];
            }
            int newBlockIndex = headBlock;
            blocks[newBlockIndex] = block;
            headBlock += 1;
            // 然后还需要重新整理一下数据
            // 还需要把headBlock中, 把0-tailBlockAddr的部分 复制到新块
            pool.copyFromPool(blocks[headBlock].addr, block.addr, tailBlockAddr);
        }
    }

    public class PMBlock{
        public MemoryPool pool;
        public long addr;
        public int capacity;
        PMBlock(MemoryPool p, long a, int c){
            pool = p;
            addr = a;
            capacity = c;
        }

        public void copyFromArray(byte[] srcArray, int srcIndex, long dstOffset, int length){
            pool.copyFromByteArrayNT(srcArray, srcIndex, addr+dstOffset, length);

        }
        public void copyToArray(long srcOffset, byte[] dstArray, int dstOffset, int length){
            pool.copyToByteArray(addr+srcOffset, dstArray, dstOffset, length);
        }
    }

}