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

import java.util.Comparator;

public class PMPrefetchBuffer {
    private static final Logger log = Logger.getLogger(PMPrefetchBuffer.class);
    public long totalCapacity;
    public MemoryPool pool;
    public PMBlockPool pmBlockPool;

    PMPrefetchBuffer(String pmDataFile) {
        totalCapacity = 60L * 1024 * 1024 * 1024;
        pool = MemoryPool.createPool(pmDataFile, totalCapacity);
        pmBlockPool = new PMBlockPool(totalCapacity);
    }

    public RingBuffer newRingBuffer() {
        return new RingBuffer();
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

            // blockSize = 128 * 1024;
            blockSize = 170;
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

        public PMBlock allocate() {
            // 先尝试直接申请
            PMBlock ret = null;
            ret = this.step1Allocate();
            if (ret != null) {
                return ret;
            }
            // 如果不能，那就从本线程的队列中申请
            Queue<PMBlock> q = threadLocalBlockQueue.get();
            if (q == null) {
                q = new ArrayDeque<PMBlock>(threadLocalBlockNum);
                threadLocalBlockQueue.set(q);
            }
            if (q.isEmpty()) {
                // 如果q是空的，那么从全局队列申请
                if (blockQueue.isEmpty()) {
                    // 如果全局队列空了，那就没办法了，要从其他线程偷一些块过来？
                    // TODO: threadLocal的好像没法偷
                    log.error("allocate fail !");
                    return null;
                }
                ret = blockQueue.poll();
                // TODO: 并且往小池子里补充一些，补充一半吧
                for (int i = 0; i < threadLocalBlockNum / 2; i++) {
                    PMBlock b = blockQueue.poll();
                    if (b != null) {
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

        public void free(PMBlock block) {
            Queue<PMBlock> q = threadLocalBlockQueue.get();
            if (q == null) {
                q = new ArrayDeque<PMBlock>(threadLocalBlockNum);
                threadLocalBlockQueue.set(q);
            }

            // free 的时候 往本地的队列里放
            if (q.size() < threadLocalBlockNum) {
                q.add(block);
            } else {
                // 如果本地队列满了那就放到全局队列中
                blockQueue.add(block);
            }
        }

        public PMBlock step1Allocate() {
            if (step1.get() == null) {
                step1.set(true);
            }

            if (step1.get() == false) {
                return null;
            }
            if (threadLocalBigBlockStartAddr.get() == null || threadLocalBigBlockFreeOffset.get() == bigBlockSize) {
                // 本地没有大块，或者大块满了
                if (atomicGlobalFreeOffset.get() > totalCapacity) {
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
        public int[] msgsBlock;

        RingBuffer() {
            // 一开始都默认有一个块
            curBlockNum = 1;
            // NOTE: 暂时先最多放4个块
            maxBlockNum = 4;
            maxLength = 100; // 限制一下每个ringBuffer存放的消息数
            // 总体上，由两个来共同限制，一个是ringBuffer存放的消息数，另一个是ringbuffer的空间大小
            blocks = new PMBlock[maxBlockNum];
            for (int i = 0; i < curBlockNum; i++) {
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
            msgsBlock = new int[maxLength];
        }

        public boolean offer(ByteBuffer data) {
            log.debug("offer start !");
            this.debugLog();
            data = data.duplicate();
            int dataLength = data.remaining();
            try {

                long savePMAddr = -1;
                int saveBlockAddr = -1;
                int saveBlock = -1;
                if (isFull()) {
                    return false;
                }

                allocate: {
                    if ((headBlock != tailBlock) || (headBlockAddr <= tailBlockAddr)) {
                        // 最正常的情况，在tail那一块直接放
                        // 无论是head < tail 还是 head > tail 都是这样
                        if (dataLength < (blocks[tailBlock].capacity - tailBlockAddr)) {
                            saveBlockAddr = tailBlockAddr;
                            saveBlock = tailBlock;

                            savePMAddr = blocks[tailBlock].addr + tailBlockAddr;
                            tailBlockAddr += dataLength;
                            // return ret;
                            break allocate;
                        }
                        // tail 不够放，这个时候要放下一个，有两种情况
                        int nextTailBlock = (tailBlock + 1) % curBlockNum;
                        if (nextTailBlock != headBlock) {
                            // 1. 说明还有空闲的block，那就用这个空闲的block
                            saveBlockAddr = tailBlockAddr;
                            saveBlock = tailBlock;

                            tailBlock = nextTailBlock;
                            tailBlockAddr = 0;
                            savePMAddr = blocks[tailBlock].addr + tailBlockAddr;
                            tailBlockAddr += dataLength;
                            // return ret;
                            break allocate;
                        } else {
                            // 2. 没有空闲的block了，退化成 headBlock == tailBlock 的情况
                            // tailBlock = nextTailBlock;
                            // tailBlockAddr = 0;
                            // 下一个就是自己，那就看看headBlockAddr前面还有没有位置
                            if (dataLength < (headBlockAddr - 0)) {
                                // 能放
                                tailBlock = nextTailBlock;

                                saveBlockAddr = 0;
                                saveBlock = nextTailBlock;
                                savePMAddr = blocks[tailBlock].addr;

                                tailBlockAddr = dataLength;
                                break allocate;
                            }
                        }
                    }
                    // 后面的代码处理 headBlock == tailBlock 且 tailBlockAddr < headBlockAddr 的情形
                    // tail 追上 head 的时候，看看tail和head之间的空隙能否放数据
                    if (dataLength < (headBlockAddr - tailBlockAddr)) {
                        // 能放
                        saveBlockAddr = tailBlockAddr;
                        saveBlock = tailBlock;

                        savePMAddr = blocks[tailBlock].addr + tailBlockAddr;
                        tailBlockAddr += dataLength;
                        // return ret;
                        break allocate;
                    }
                }
                if (savePMAddr == -1) {
                    // 申请不到
                    // 申请不到的情况下，不能动tailBlock、tailBlockAddr
                    return false;
                }
                // 申请到了
                if (isEmpty()) {
                    msgsBlockAddr[tail] = saveBlockAddr;
                    msgsBlock[tail] = saveBlock;
                    msgsLength[tail] = dataLength;
                    pool.copyFromByteArray(data.array(), data.arrayOffset() + data.position(), savePMAddr, dataLength);
                    length++;
                    return true;
                }
                tail = (tail + 1) % maxLength;
                msgsBlockAddr[tail] = saveBlockAddr;
                msgsBlock[tail] = saveBlock;
                msgsLength[tail] = dataLength;
                pool.copyFromByteArray(data.array(), data.arrayOffset() + data.position(), savePMAddr, dataLength);
                length++;
                return true;
            } finally {
                log.debug("offer end");
                this.debugLog();
            }
        }

        public ByteBuffer poll() {
            log.debug("poll start !");
            this.debugLog();
            try {

                if (isEmpty()) {
                    return null;
                }
                int dataLength = msgsLength[head];
                long msgPMAddr = blocks[headBlock].addr + headBlockAddr;
                ByteBuffer data = ByteBuffer.allocate(dataLength);
                pool.copyToByteArray(msgPMAddr, data.array(), data.arrayOffset() + data.position(), dataLength);
                // free the head
                headBlockAddr += dataLength;

                if (length == 1) {
                    length--;
                    return data;
                }
                head = (head + 1) % maxLength;
                length--;
                return data;
            } finally {
                // 说明上次free的时候上一个block已经free完啦，应该把head切换到下一个block来
                // 如果存在下一条消息，并且加上下一条消息的长度超过了这个block
                // 那么说明此时应该跳到下一个block
                if (!isEmpty()) {
                    if (headBlockAddr + msgsLength[head] >= blocks[headBlock].capacity) {
                        headBlock = (headBlock + 1) % curBlockNum;
                        headBlockAddr = 0;
                    }
                }
                log.debug("poll end");
                this.debugLog();
            }

        }

        public boolean isFull() {
            return length == maxLength;
        }

        public boolean isEmpty() {
            return length == 0;
        }

        public void addBlock() {
            log.debug("addBlock start !");
            this.debugLog();

            try {
                PMBlock block = pmBlockPool.allocate();
                // 两种情况
                // 情况1
                if ((headBlock < tailBlock) || (headBlock == tailBlock && headBlockAddr <= tailBlockAddr)) {
                    // 可以直接将队列向后延伸
                    blocks[curBlockNum] = block;
                    curBlockNum += 1;
                    return;
                }
                if (headBlock > tailBlock) {
                    // 可以将新增的块放到 tailBlock 和 headBlock 之间
                    // 准确的说，是headBlock之前
                    // 然后把 headBlock以及之后的块整体向后移动
                    for (int i = curBlockNum; i > headBlock; i--) {
                        blocks[i] = blocks[i - 1];
                    }
                    int newBlockIndex = headBlock;
                    blocks[newBlockIndex] = block;
                    headBlock += 1;
                    curBlockNum += 1;
                    return;
                }
                // headBlock == tailBlock && headBlockAddr > tailBlock
                // 可以将新增的块放到 tailBlock 和 headBlock 之间
                // 准确的说，是headBlock之前
                // 然后把 headBlock以及之后的块整体向后移动
                for (int i = curBlockNum; i > headBlock; i--) {
                    blocks[i] = blocks[i - 1];
                }
                int newBlockIndex = headBlock;
                blocks[newBlockIndex] = block;
                headBlock += 1;
                // 然后还需要重新整理一下数据
                // 还需要把headBlock中, 把0-tailBlockAddr的部分 复制到新块
                pool.copyFromPool(blocks[headBlock].addr, block.addr, tailBlockAddr);
                curBlockNum += 1;
            } finally {
                log.debug("addBlock end");
                this.debugLog();

            }

        }

        public void debugLog() {
            StringBuilder output = new StringBuilder();
            output.append("headBlock : " + headBlock + " ");
            output.append("headBlockAddr : " + headBlockAddr + " ");
            output.append("tailBlock : " + tailBlock + " ");
            output.append("tailBlockAddr : " + tailBlockAddr + " ");
            output.append("maxBlockNum : " + maxBlockNum + " ");
            output.append("curBlockNum : " + curBlockNum + " ");
            StringBuilder output2 = new StringBuilder();
            output.append("head : " + head + " ");
            output.append("tail : " + tail + " ");
            output.append("length : " + length + " ");
            output.append("maxLength : " + maxLength + " ");
            log.debug(output);
            log.debug(output2);
        }
    }

    public static void main(String[] args) {
        log.setLevel(Level.DEBUG);
        // log.setLevel(Level.INFO);
        PMPrefetchBuffer p = new PMPrefetchBuffer("/mnt/pmem/mq/testdata");
        byte[] byteData = new byte[40];
        for (int i = 0; i < 40; i++) {
            byteData[i] = (byte) i;
        }
        boolean ret;
        ByteBuffer buf;
        // {
        //     RingBuffer b = p.newRingBuffer();
        //     // test case 1, set block size to 170 Bytes
        //     for (int i = 0; i < 4; i++) {
        //         ret = b.offer(ByteBuffer.wrap(byteData));
        //         if (ret == false) {
        //             log.error("data error !");
        //             System.exit(-1);
        //         }
        //     }
        //     ret = b.offer(ByteBuffer.wrap(byteData));
        //     if (ret == true) {
        //         log.error("offer error !");
        //         System.exit(-1);
        //     }
        //     b.addBlock();
        //     for (int i = 0; i < 4; i++) {
        //         ret = b.offer(ByteBuffer.wrap(byteData));
        //         if (ret == false) {
        //             log.error("data error !");
        //             System.exit(-1);
        //         }
        //     }
        //     // 两块都满了

        //     for (int i = 0; i < 2; i++) {
        //         buf = b.poll();
        //         for (int k = 0; k < 40; k++) {
        //             if (buf.array()[k] != byteData[k]) {
        //                 log.error("data error !!");
        //                 System.exit(-1);
        //             }
        //         }

        //     }
        //     // 第一块空出2个位置，其中第二个位置会刚好相等，就不让放了
        //     for (int i = 0; i < 1; i++) {
        //         ret = b.offer(ByteBuffer.wrap(byteData));
        //         if (ret == false) {
        //             log.error("offer error !");
        //             System.exit(-1);
        //         }
        //     }
        //     for (int i = 0; i < 1; i++) {
        //         ret = b.offer(ByteBuffer.wrap(byteData));
        //         if (ret == true) {
        //             log.error("offer error !");
        //             System.exit(-1);
        //         }
        //     }

        //     b.addBlock();

        //     for (int i = 0; i < 1; i++) {
        //         ret = b.offer(ByteBuffer.wrap(byteData));
        //         if (ret == false) {
        //             log.error("offer error !");
        //             System.exit(-1);
        //         }
        //     }

        //     // 这个时候head和tail都在第0块
        //     // 扩容，这个时候应该是放在tail前面
        //     for (int i = 0; i < 8; i++) {
        //         buf = b.poll();
        //         for (int k = 0; k < 40; k++) {
        //             if (buf.array()[k] != byteData[k]) {
        //                 log.error("data error !!");
        //                 System.exit(-1);
        //             }
        //         }
        //     }

        // }
        {
            RingBuffer b = p.newRingBuffer();
            // test case 2, set block size to 170 Bytes
            for (int i = 0; i < 4; i++) {
                ret = b.offer(ByteBuffer.wrap(byteData));
                if (ret == false) {
                    log.error("offer error !");
                    System.exit(-1);
                }
            }
            b.addBlock();
            for (int i = 0; i < 4; i++) {
                ret = b.offer(ByteBuffer.wrap(byteData));
                if (ret == false) {
                    log.error("offer error !");
                    System.exit(-1);
                }
            }
            b.addBlock();
            for (int i = 0; i < 4; i++) {
                ret = b.offer(ByteBuffer.wrap(byteData));
                if (ret == false) {
                    log.error("offer error !");
                    System.exit(-1);
                }
            }
            // 这个时候3个block都填满了，消费两个block
            for (int i = 0; i < 8; i++) {
                buf = b.poll();
                for (int k = 0; k < 40; k++) {
                    if (buf.array()[k] != byteData[k]) {
                        log.error("data error !!");
                        System.exit(-1);
                    }
                }
            }
            // 再填第0个block里，此时0block有东西，1block没东西，2block有东西
            for (int i = 0; i < 4; i++) {
                ret = b.offer(ByteBuffer.wrap(byteData));
                if (ret == false) {
                    log.error("offer error !");
                    System.exit(-1);
                }
            }
            log.info("step 1 ok");
            b.addBlock();
            // 再填第1个block里，此时0block有东西，1block有东西，2block有东西
            for (int i = 0; i < 8; i++) {
                ret = b.offer(ByteBuffer.wrap(byteData));
                if (ret == false) {
                    log.error("offer error !");
                    System.exit(-1);
                }
            }
            log.info("offer agin ok");
            // 现在4个block都有消息，总共16个，全部取出来检查一下
            for (int i = 0; i < 16; i++) {
                buf = b.poll();
                for (int k = 0; k < 40; k++) {
                    if (buf.array()[k] != byteData[k]) {
                        log.error("data error !!");
                        System.exit(-1);
                    }
                }
            }
        }

    }

}
