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
    public long totalCapacity;
    public MemoryPool pool;
    public PMBlockPool pmBlockPool;
    PMPrefetchBuffer(String pmDataFile){
        totalCapacity = 60L*1024*1024*1024;
        pool = MemoryPool.createPool(pmDataFile, totalCapacity);
        pmBlockPool = new PMBlockPool(totalCapacity);
    }
    public RingBuffer newRingBuffer(){
        return new RingBuffer();
    } 

    public class PMBlock{
        public long addr;
        public int capacity;
        PMBlock(long a, int c){
            addr = a;
            capacity = c;
        }
    }

	public class PMBlockPool {
        // 分为两个阶段
        // 阶段1：直接通过移动偏移量的方法申请内存
        // 依然是分大池子小池子的情况
        // 释放的时候，可以放到本线程的队列中，以备后续再用，如果本线程的队列满了就放到公共的池子里
        // 阶段2：直接移动偏移量的方法如果申请不了内存了，就从每个线程的队列中拿定长块，如果线程内的队列没有，那就从公共的池子里再拿一些

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
        public ThreadLocal<Boolean> step1;


        // 阶段2
        public ConcurrentLinkedQueue<PMBlock> blockQueue;
        // 大池子
        public ThreadLocal<Queue<PMBlock>> threadLocalBlockQueue;
        // 小池子


		PMBlockPool(long capacity) {
			totalCapacity = capacity;

			blockSize = 128 * 1024; // 8 个slot
            threadLocalBlockNum = 200;
			bigBlockSize = threadLocalBlockNum * blockSize; // 25MiB

            // 初始化阶段1
            // 大池子
			atomicGlobalFreeOffset = new AtomicLong();
			atomicGlobalFreeOffset.set(0L);
            // 小池子
			threadLocalBigBlockFreeOffset = new ThreadLocal<>();
			threadLocalBigBlockStartAddr = new ThreadLocal<>();
			threadLocalBigBlockFreeOffset.set(0);
            step1 = new ThreadLocal<>();

            // 初始化阶段2
            // 大池子
            blockQueue = new ConcurrentLinkedQueue<>();
            // 小池子
            threadLocalBlockQueue = new ThreadLocal<>();
		}

        public PMBlock allocate(){
            // 先尝试直接申请
            PMBlock ret = null;
            ret = this.step1Allocate();
            if (ret != null){
                return ret;
            }
            // 如果不能，那就从本线程的队列中申请
            Queue<PMBlock> q = threadLocalBlockQueue.get();
            if (q == null){
                q = new ArrayDeque<PMBlock>(threadLocalBlockNum);
                threadLocalBlockQueue.set(q);
            }
            if (q.isEmpty()){
                // 如果q是空的，那么从全局队列申请
                if (blockQueue.isEmpty()){
                    // 如果全局队列空了，那就没办法了，要从其他线程偷一些块过来？
                    // TODO: threadLocal的好像没法偷
                    log.error("allocate fail !");
                    return null;
                }
                ret = blockQueue.poll();
                // TODO: 并且往小池子里补充一些，补充一半吧
                for (int i = 0; i < threadLocalBlockNum/2; i++){
                    PMBlock b = blockQueue.poll();
                    if (b != null){
                        q.offer(b);
                    } else {
                        break;
                    }
                }
            } else {
                ret = q.poll();
            }
            return ret;
        }
        public void free(PMBlock block){
            Queue<PMBlock> q = threadLocalBlockQueue.get();
            if (q == null){
                q = new ArrayDeque<PMBlock>(threadLocalBlockNum);
                threadLocalBlockQueue.set(q);
            }

            // free 的时候 往本地的队列里放
            if (q.size() < threadLocalBlockNum){
                q.add(block);
            } else {
                // 如果本地队列满了那就放到全局队列中
                blockQueue.add(block);
            }
        }

		public PMBlock step1Allocate() {
            if (step1.get() == null){
                step1.set(true);
            }

            if (step1.get() == false){
                return null;
            }
			if (threadLocalBigBlockStartAddr.get() == null || threadLocalBigBlockFreeOffset.get() == bigBlockSize) {
                // 本地没有大块，或者大块满了
                if (atomicGlobalFreeOffset.get() > totalCapacity){
                    // 阶段1可以结束了
                    step1.set(false);
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

    public class RingBuffer {
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


        RingBuffer(){
            // 一开始都默认有一个块
            curBlockNum = 1;
            // NOTE: 暂时先最多放4个块
            maxBlockNum = 4;
            maxLength = 100; // 限制一下每个ringBuffer存放的消息数
            // 总体上，由两个来共同限制，一个是ringBuffer存放的消息数，另一个是ringbuffer的空间大小
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
            msgsLength = new int[maxLength];
            msgsBlockAddr = new int[maxLength];
            // msgsBlock = new int[maxLength];
        }

        public boolean offer(ByteBuffer data){
            data = data.duplicate();
            int dataLength = data.remaining();
            long savePMAddr = -1;
            int saveBlockAddr = -1;
            if (isFull()){
                return false;
            }

            allocate:   {
                if ((headBlock != tailBlock) || (headBlockAddr <= tailBlockAddr)){
                    // 最正常的情况，在tail那一块直接放
                    // 无论是head < tail 还是 head > tail 都是这样
                    if ( dataLength < (blocks[tailBlock].capacity - tailBlockAddr)){
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

    public static void main(String[] args) {
        PMPrefetchBuffer p = new PMPrefetchBuffer("/mnt/pmem/mq/testdata");
        RingBuffer b = p.newRingBuffer();
        byte[] byteData = new byte[100];
        for (int i = 0; i < 100; i++){
            byteData[i] = (byte)i;
        }
        for (int i = 0; i < 10; i++){
            b.offer(ByteBuffer.wrap(byteData));
        }
        b.addBlock();
        for (int i = 0; i < 10; i++){
            ByteBuffer ret = b.poll();
            for (int k = 0; k < 100; k++){
                if (ret.array()[k] != byteData[k]){
                    log.error("data error !!");
                    System.exit(-1);
                }
            }

        }
    }

}
