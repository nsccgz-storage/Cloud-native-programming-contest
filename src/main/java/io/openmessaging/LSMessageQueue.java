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


public class LSMessageQueue extends MessageQueue {
    private static final Logger log = Logger.getLogger(LSMessageQueue.class);
    // private static final MemoryPool pmPool = MemoryPool.createPool("/mnt/pmem/data", 60L*1024L*1024L);

    public class MQConfig {
        Level logLevel = Level.INFO;
        // Level logLevel = Level.DEBUG;
        boolean useStats = true;
        // boolean useStats = false;
        int writeMethod = 12;
        int numOfDataFiles = 4;
        int maxBufNum = 8;
        int maxBufLength = 68*1024;
        boolean fairLock = true;
        public String toString() {
            return String.format("useStats=%b | writeMethod=%d | numOfDataFiles=%d | maxBufLength=%d | maxBufNum=%d | ",useStats,writeMethod,numOfDataFiles,maxBufLength,maxBufNum);
            // return String.format("useStats=%b | writeMethod=%d | numOfDataFiles=%d | maxBufLength=%d | maxBufNum=%d | align to 4K !! ",useStats,writeMethod,numOfDataFiles,maxBufLength,maxBufNum);
        }
    }


    public class MQQueue {
        public Long maxOffset = 0L;
        public ArrayList<Long> offset2position;
        public DataFile df;
        // public byte[] maxOffsetData;
        public ByteBuffer maxOffsetData;
        public int type;
        public long consumeOffset;
        public long prefetchOffset;
        public QueuePrefetchBuffer prefetchBuffer;
        public Future<Integer> prefetchFuture;
        public MyByteBufferPool bbPool;

        MQQueue(DataFile dataFile){
            consumeOffset = 0L;
            type = 0;
            maxOffset = 0L;
            offset2position = new ArrayList<>(512);
            df = dataFile;
            prefetchBuffer = new QueuePrefetchBuffer(this, df);
            prefetchFuture = null;
        }
        MQQueue(){
            consumeOffset = 0L;
            type = 0;
            maxOffset = 0L;
            offset2position = new ArrayList<>(512);
            prefetchBuffer = new QueuePrefetchBuffer(this, df);
            prefetchFuture = null;
        }

    }

    public class MQTopic {
        public short topicId;
        public String topicName;
        public HashMap<Integer, MQQueue> id2queue;
        public DataFile df;

        MQTopic(short myTopicId, String name, DataFile dataFile){
            topicId = myTopicId;
            topicName = name;
            id2queue = new HashMap<Integer, MQQueue>();
            df = dataFile;
        }


    }

    MQConfig mqConfig;
    private FileChannel metadataFileChannel;
    DataFile[] dataFiles;
    int numOfDataFiles;
    ConcurrentHashMap<String, MQTopic> topic2object;
    Heap pmHeap;
    MyPMBlockPool pmBlockPool;
    ThreadLocal<MyByteBufferPool> threadLocalByteBufferPool;
    boolean isCrash;

    LSMessageQueue(String dbDirPath, String pmDirPath, MQConfig config){
        // SSDBench.runStandardBench(dbDirPath);
        // PMBench.runStandardBench(pmDirPath);
        mqConfig = config;
        init(dbDirPath, pmDirPath);

    }


    LSMessageQueue(String dbDirPath, String pmDirPath){
        // SSDBench.runStandardBench(dbDirPath);
        // PMBench.runStandardBench(pmDirPath);
        mqConfig = new MQConfig();
        init(dbDirPath, pmDirPath);

    }

    public void init(String dbDirPath, String pmDirPath) {
        try {
            isCrash = false;
            log.setLevel(mqConfig.logLevel);
            log.info(mqConfig);
            Boolean crash = false;
            String metadataFileName = dbDirPath + "/meta";
            String pmDataFile = pmDirPath + "/data";
 
            // whether the MQ is recover from existed file/db ?
            File metadataFile = new File(metadataFileName);
            if (metadataFile.exists() && !metadataFile.isDirectory()) {
                crash = true;
                isCrash = true;
            }


            topic2object = new ConcurrentHashMap<String, MQTopic>();
            log.info("Initializing on PM : " + pmDataFile);

            pmBlockPool = new MyPMBlockPool(pmDataFile);
            // pmHeap = Heap.createHeap(pmDataFile, 60L*1024L*1024L*1024L);
            // if (!isCrash){
            //     pmHeap = Heap.createHeap(pmDataFile, 60L*1024L*1024L*1024L);
            // } else {
            //     pmHeap = Heap.createHeap(pmDataFile+"1", 60L*1024L*1024L*1024L);
            // }
            // init datafile
            numOfDataFiles = mqConfig.numOfDataFiles;
            log.debug("create data files");
            dataFiles = new DataFile[numOfDataFiles];
            for (int i = 0; i < numOfDataFiles; i++) {
                String dataFileName = dbDirPath + "/db" + i;
                log.info("Initializing datafile: " + dataFileName);
                dataFiles[i] = new DataFile(dataFileName);
            }

            log.info("Initializing metadata file");
            metadataFileChannel = new RandomAccessFile(metadataFile, "rw").getChannel();
            if (crash) {
                log.info("recover !!");
                System.exit(-1);
                // recover();
            }
            localThreadId = new ThreadLocal<>();
            numOfThreads = new AtomicInteger();
            numOfThreads.set(0);
            numOfTopics = new AtomicInteger();
            numOfTopics.set(1);
            threadLocalByteBufferPool = new ThreadLocal<>();

            if (mqConfig.useStats) {
                testStat = new TestStat(dataFiles);
            }

        } catch (IOException ie) {
            ie.printStackTrace();
        }

        System.gc();

        log.info("init ok!");
    }

    public void recover(){
        try {
            // topic2object
            HashMap<Short, String> id2topic = new HashMap<>();
            {
                // TODO: read metadata to get this mapping
                ByteBuffer strBuffer = ByteBuffer.allocate(128);
                strBuffer.clear();
                int ret = 0;
                short topicId = 1;
                while ((ret = metadataFileChannel.read(strBuffer)) != -1){
                    log.debug("ret : " + ret);
                    log.debug(strBuffer);
                    strBuffer.flip();
                    log.debug(strBuffer);
                    int strLength = strBuffer.getInt();
                    log.debug("strLength :"+strLength);
                    byte[] strBytes = new byte[strLength];
                    strBuffer.get(strBytes);
                    String topic = new String(strBytes);
                    id2topic.put(topicId, topic);
                    topicId += 1;
                    log.debug("recover topic : "+topic);
                    strBuffer.clear();
                }
            }
            

            // topicId -> topic

            ByteBuffer bufMetadata = ByteBuffer.allocate(8);
            ByteBuffer msgMetadata = ByteBuffer.allocate(8);
            for (int i = 0; i < numOfDataFiles; i++){
                long curPosition = 0L;
                FileChannel fc = dataFiles[i].dataFileChannel;
                int ret = 0;
                while ((ret = fc.read(bufMetadata, curPosition)) != -1){
                    bufMetadata.flip();
                    int bufLength = bufMetadata.getInt();
                    int bufNum = bufMetadata.getInt();
                    log.debug("bufLength : "+bufLength);
                    log.debug("bufNum : "+bufNum);
                    long bufPosition = curPosition+8;
                    for (int k = 0; k < bufNum; k++){
                        msgMetadata.clear();
                        fc.read(msgMetadata, bufPosition);
                        msgMetadata.flip();
                        short topicId = msgMetadata.getShort();
                        int queueId = msgMetadata.getInt();
                        short length = msgMetadata.getShort();
                        String topic = id2topic.get(topicId);
                        replayAppend(i, topicId, topic, queueId, bufPosition);
                        bufPosition += 8+length;
                    }
                    curPosition += bufLength;
                    bufMetadata.clear();
                }
            }


        } catch (IOException ie) {
            ie.printStackTrace();
        }


    }

    public long replayAppend(int dataFileId,short topicId, String topic, int queueId, long position) {

        log.debug("append : " + topic + "," + queueId + "," + position);
        MQTopic mqTopic;
        MQQueue q;
        mqTopic = topic2object.get(topic);
        if (mqTopic == null) {
            // int dataFileId = Math.floorMod(topic.hashCode(), numOfDataFiles);
            // mqTopic = new MQTopic(topic, dataFileId);
            mqTopic = new MQTopic(topicId, topic, dataFiles[dataFileId]);
            topic2object.put(topic, mqTopic);
        }

        q = mqTopic.id2queue.get(queueId);
        if (q == null){
            // q = new MQQueue(dataFileId);
            // q = new MQQueue(dataFiles[dataFileId]);
            q = new MQQueue();
            mqTopic.id2queue.put(queueId, q);
        }

        q.offset2position.add(position);
        long ret = q.maxOffset;
        q.maxOffset++;
        return ret;
    }


    @Override
    public long append(String topic, int queueId, ByteBuffer data) {

        log.debug("append : "+topic+","+queueId + data);
        if (mqConfig.useStats){
            testStat.appendStart();
            testStat.appendUpdateStat(topic, queueId, data);
        }
    
        // FIXME: 申请内存需要占用额外时间，因为这段内存不能被重复使用，生命周期较短，还可能频繁触发GC


        MQTopic mqTopic;
        MQQueue q;
        mqTopic = topic2object.get(topic);
        if (mqTopic == null) {
            int threadId = updateThreadId();
            int dataFileId = threadId % numOfDataFiles; 
            short topicId = getAndUpdateTopicId(topic);
            // int dataFileId = Math.floorMod(topic.hashCode(), numOfDataFiles);
            // mqTopic = new MQTopic(topic, dataFileId);
            mqTopic = new MQTopic(topicId, topic, dataFiles[dataFileId]);
            topic2object.put(topic, mqTopic);
        }
        // data = data.slice();
        data.mark();

        q = mqTopic.id2queue.get(queueId);
        if (q == null){
            Integer queueIdObject = queueId;
            int dataFileId = Math.floorMod(topic.hashCode()+queueIdObject.hashCode(), numOfDataFiles);
            // q = new MQQueue(dataFileId);
            // q = new MQQueue(dataFiles[dataFileId]);
            q = new MQQueue(mqTopic.df); // 要和topic用一样的df
            q.bbPool = threadLocalByteBufferPool.get();
            mqTopic.id2queue.put(queueId, q);
            if (mqConfig.useStats){
                testStat.incQueueCount();
            }
        }

        // // 确保和这个queue相关的异步任务已完成
        if (q.prefetchFuture != null){
            while (!q.prefetchFuture.isDone()){
                try {
                    Thread.sleep(0, 10000);
                } catch (Throwable ie){
                    ie.printStackTrace();
                }
            }
            q.prefetchFuture = null;
        }


        // if (localThreadId.get() == 1){
        //     log.info("append : "+topic+","+queueId+","+data.remaining()+" maxOffset :"+q.maxOffset);
        // }

        log.debug("append : "+topic+","+queueId+","+data.remaining()+" maxOffset :"+q.maxOffset);

        DataFile df = mqTopic.df;

        // long position = df.syncSeqWritePushConcurrentQueueHeapBatchBufferPrefetch(mqTopic.topicId, queueId, data, q);

        // long position = df.syncSeqWritePushConcurrentQueueHeapBatchBufferHotData(mqTopic.topicId, queueId, data, q);

        long position = df.syncSeqWritePushConcurrentQueueHeapBatchBuffer(mqTopic.topicId, queueId, data);
        // long position = df.syncSeqWritePushConcurrentQueueHeapBatchBuffer4K(mqTopic.topicId, queueId, data);
        q.offset2position.add(position);
        long ret = q.maxOffset;

        // 换成在每个append中写pm，而不是在聚合中写pm，也会有明显的开销
        data.reset();
        if (!q.prefetchBuffer.isFull()){
            q.prefetchBuffer.prefetch();
            if (!q.prefetchBuffer.isFull() && q.prefetchOffset == q.maxOffset){
                log.debug("double write");
                q.prefetchBuffer.directAddData(data);
            }
            // 写满就不管了
            // if (q.prefetchOffset == q.maxOffset){
            //     log.debug("double write");
            //     q.prefetchBuffer.directAddData(data);
            // }
        }


        q.maxOffset++;

        // 未知队列和热队列需要双写，冷队列不用，冷队列还是预取多一些内容吧
        // if ((q.type == 0 || q.type == 1) && (!q.prefetchBuffer.isFull())){
        //     final MQQueue finalQ = q;
        //     q.prefetchFuture = df.prefetchThread.submit(new Callable<Integer>(){
        //        @Override
        //        public Integer call() throws Exception {
        //            MQQueue q = finalQ;
        //            long startTime = System.nanoTime();
        //            if (!q.prefetchBuffer.isFull()){
        //                // 不管如何，先去尝试预取一下内容，如果需要就从SSD读
        //                q.prefetchBuffer.prefetch();
        //                long thisOffset = q.maxOffset-1;
        //                if (!q.prefetchBuffer.isFull() && thisOffset == q.prefetchOffset){
        //                    log.debug("double write !");
        //                    // 如果目前要写入的数据刚好就是下一个要预取的内容
        //                    // 双写
        //                    data.reset();
        //                    q.prefetchBuffer.directAddData(data);
        //                    // TODO: 担心data在异步中途被改
        //                }
        //            }
        //            long endTime = System.nanoTime();
        //            log.debug("prefetch ok");
        //            log.debug("time : " + (endTime - startTime) + " ns");
        //            return 0;
        //        }
        //     });
        // }

        return ret;
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {

        if (mqConfig.useStats){
            testStat.getRangeStart();
            testStat.getRangeUpdateStat(topic, queueId, offset, fetchNum);
        }
        Map<Integer, ByteBuffer> ret = new HashMap<>();
        MQTopic mqTopic;
        MQQueue q;
        mqTopic = topic2object.get(topic);
        if (mqTopic == null) {
            return ret;
        }
        q = mqTopic.id2queue.get(queueId);
        if (q == null){
            return ret;
        }
        // // to see the trace online
        // if (localThreadId.get() == 1){
        //     log.info("getRange : "+topic+","+queueId+","+offset+","+fetchNum+" maxOffset: "+(q.maxOffset-1));
        // }

        
        log.debug("getRange : "+topic+","+queueId+","+offset+","+fetchNum+" maxOffset: "+(q.maxOffset-1));
        // 更新一下offset和fetchNum，略去那些肯定没有的
        if (offset >= q.maxOffset){
            return ret;
        }
        if (offset + fetchNum-1 >= q.maxOffset){
            fetchNum = (int)(q.maxOffset-offset);
        }

        int fetchStartIndex = 0;
        // // 确保和这个queue相关的异步任务已完成
        if (q.prefetchFuture != null){
           while (!q.prefetchFuture.isDone()){
               try {
                   Thread.sleep(0, 10000);
               } catch (Throwable ie){
                   ie.printStackTrace();
               }
           }
           q.prefetchFuture = null;
        }

        // 把ret扔到prefetchBuffer过一圈，看看能读到哪些数据
        // 如果有东西在prefetchBuffer的话
        // 需要的数据:      [offset, offset+fetchNum-1]
        // 已经预取好的数据: [consumeOffset, prefetchOffset-1]
        // 暂时只考虑 offset == consumeOffset 的情况
        // assert (offset == q.consumeOffset);
        q.consumeOffset = offset;

        // 目前消费的offset刚好和我预取好的消息相匹配
        // 所以前面的消息都不用取了
        int prefetchNum = q.prefetchBuffer.consume(ret, offset, fetchNum);
        fetchStartIndex += prefetchNum;
        q.consumeOffset += prefetchNum;
        // 当一个热读过来，offset会和consumeOffset不匹配，这个时候我要如何处理？


        // 分类
        if (q.type == 0){
            if (offset == 0){
                q.type = 2; // cold
                if (mqConfig.useStats){
                    testStat.incColdQueueCount();
                }
                // TODO: 可以触发prefetch buffer 释放
            } else {
                q.type = 1;
                if (mqConfig.useStats){
                    testStat.incHotQueueCount();
                }
                // TODO: 可以触发 prefetch buffer 扩容
            }
            // } else if (offset >= q.maxOffset-5) {
            //     q.type = 1; // hot
            //     if (mqConfig.useStats){
            //         testStat.incHotQueueCount();
            //     }
            // }
        }

        if(mqConfig.useStats){
            testStat.incFetchMsgCount(fetchNum);
            if (q.type == 1){
                // hot
                testStat.incHotFetchMsgCount(fetchNum);
            } else if (q.type == 2){
                // cold
                testStat.incColdFetchMsgCount(fetchNum);

            }
        }




        if (mqConfig.useStats){
            testStat.incReadSSDCount(fetchNum-fetchStartIndex);
            if (q.type == 1){
                // hot
                testStat.incHotReadSSDCount(fetchNum-fetchStartIndex);
            } else if (q.type == 2){
                // cold
                testStat.incColdReadSSDCount(fetchNum-fetchStartIndex);
            }

        }

        DataFile df = mqTopic.df;
        // 前面已经把超出maxOffset 的fetchNum 缩小到和maxOffset一样了，这里其实可以直接更新
        // q.consumeOffset += fetchNum-fetchStartIndex;
        q.consumeOffset = offset + fetchNum ; // 下一个被消费的位置

        // // 既然从预取中消费了一些数据，那当然可以补回来
        // // getRange 结束后应该要用一个异步任务补一些数据到预取队列中
        // if (q.type == 2){
        //     // 冷读才需要预取
        //     q.prefetchFuture = df.prefetchThread.submit(new Callable<Integer>(){
        //        @Override
        //        public Integer call() throws Exception {
        //            long startTime = System.nanoTime();
        //            if (!q.prefetchBuffer.isFull()){
        //                // 不管如何，先去尝试预取一下内容，如果需要就从SSD读
        //                q.prefetchBuffer.prefetch();
        //            }
        //            long endTime = System.nanoTime();
        //            log.debug("prefetch ok");
        //            log.debug("time : " + (endTime - startTime) + " ns");
        //            return 0;
        //        }
        //     });
        // }



        long pos = 0;
        for (int i = fetchStartIndex; i < fetchNum; i++){
            long curOffset = offset + i;
            int intCurOffset = (int)curOffset;
            pos = q.offset2position.get(intCurOffset);
            ByteBuffer buf = df.read(pos);
            if (buf != null){
                //buf.position(0);
                //buf.limit(buf.capacity());
                ret.put(i, buf);
            }
        }
        // if (!q.prefetchBuffer.isFull()){
        //     // 不管如何，先去尝试预取一下内容，如果需要就从SSD读
        //     q.prefetchBuffer.prefetch();
        // }

        return ret;
    }

    public void close(){
        try {
            for (int i = 0; i < numOfDataFiles; i++){
                dataFiles[i].prefetchThread.shutdown();
    			while (!dataFiles[i].prefetchThread.awaitTermination(60, TimeUnit.SECONDS)) {
    				System.out.println("Pool did not terminate, waiting ...");
    			}

            }
        } catch (InterruptedException ie){
            ie.printStackTrace();
        }

    }

    private ThreadLocal<Integer> localThreadId;
    private AtomicInteger numOfThreads;

    public int updateThreadId() {
        if (localThreadId.get() == null) {
            int thisNumOfThread = numOfThreads.getAndAdd(1);
            localThreadId.set(thisNumOfThread);
            log.info("init thread id : " + thisNumOfThread);
        }
        if (threadLocalByteBufferPool.get() == null){
            threadLocalByteBufferPool.set(new MyByteBufferPool());
        }
        return localThreadId.get();
    }


    private AtomicInteger numOfTopics;

    public short getAndUpdateTopicId(String topic) {
        int topicId = numOfTopics.getAndAdd(1);
        try {
            ByteBuffer buf = ByteBuffer.allocate(128);
            buf.putInt(topic.length());
            buf.put(topic.getBytes());
            buf.position(0);
            log.debug(buf);
            metadataFileChannel.write(buf, (topicId-1)*128);
            metadataFileChannel.force(true);
            log.info("get topic id : " + topicId );

        } catch (IOException ie){
            ie.printStackTrace();
        }
        return (short)topicId;
    }
    public class QueuePrefetchBuffer{
        public long[] addrBuffer;
        public int[] msgLengths;

        public long headOffset; // == consumeOffset
        public long tailOffset;
        // 缓存 [headOffset, tailOffset] 的内容
        public int head;
        public int tail;
        public int length;

        public MyPMBlock block;
        // public MemoryBlock block;
        public int maxLength;
        public MQQueue q;
        public DataFile df;
        public int slotSize;

        QueuePrefetchBuffer(MQQueue myQ, DataFile myDf){
            head = 0;
            tail = 0;
            length = 0;
            maxLength = 8;
            msgLengths = new int[maxLength];
            slotSize = 17*1024;
            // FIXME: 性能很差，考虑换掉
            q = myQ;
            df = myDf;
            // block = pmBlockPool.allocate();
            // 大小写死在pool的实现里了，改大小的话要改两个地方
        }

        public int consume(Map<Integer, ByteBuffer> ret, long offset,  int fetchNum){
            // 把分配内存放在异步任务中完成
            if (block == null){
                block = pmBlockPool.allocate();
                // block = pmHeap.allocateMemoryBlock(maxLength*slotSize);
            }

            log.debug("tail : " + tail + " head : "+head + " length : " + length);
            log.debug("tailOffset : " + tailOffset + " headOffset : " + headOffset);
            log.debug("q.consumeOffset : " + q.consumeOffset + " q.prefetchOffset : " + q.prefetchOffset);

            // 始终假定 offset == q.consumeOffset
            // 如果 headOffset != offset
            // 那么 ，两种情况
            // 一种是 buf中还有需要消费的内容，那么就移动一下队列就好
            if (offset != headOffset){
                if (offset < headOffset){
                    /// 倒退了
                    // 不管，不预取了
                    return 0;
                }
                if ( headOffset < offset && offset < tailOffset){
                    // 说明当前要拿的数据还在buf中
                    // 先移动一下head，让队列符合 headOffset = offset 的假定
                    long num = offset - headOffset;
                    for (long i = 0;  i < num; i++){
                        this.poll();
                    }
                } else {
                    // 说明当前要拿的数据不在buf中
                    // 另一种是buf中所有内容都没用，直接重置这条buffer吧
                    // 后面再次调用prefetch的时候会重置的
                    // 重置buffer，清空
                    this.reset(offset + fetchNum-1);

                    return 0;
                }
            }

            // read from consume Offset
            // 假定 刚好匹配，一定是从headOffset开始读取
            // 想要fetchNum那么多个，但不一定有这么多
            int consumeNum = Math.min(fetchNum, length);
            for (int i = 0; i < consumeNum; i++){
                ByteBuffer buf = this.poll();
                log.debug(buf);
                ret.put(i, buf);
            }
            // return number of msg  read from buffer
            log.debug("consume " + consumeNum + " msgs in prefetch buffer");
            log.debug("tail : " + tail + " head : "+head + " length : " + length);
            log.debug("tailOffset : " + tailOffset + " headOffset : " + headOffset);
            log.debug("q.consumeOffset : " + q.consumeOffset + " q.prefetchOffset : " + q.prefetchOffset);


            return consumeNum;

        }

        public void prefetch(){
            // 把分配内存放在异步任务中完成
            if (block == null){
                long startTime = System.nanoTime();
                block = pmBlockPool.allocate();
                // block = pmHeap.allocateMemoryBlock(maxLength*slotSize);
                long endTime = System.nanoTime();
                log.debug("memory block allocate : " + (endTime - startTime) + " ns");
            }
            log.debug("try to prefetch !!");
            log.debug("tail : " + tail + " head : "+head + " length : " + length);
            log.debug("tailOffset : " + tailOffset + " headOffset : " + headOffset);
            log.debug("q.consumeOffset : " + q.consumeOffset + " q.prefetchOffset : " + q.prefetchOffset);
            // 先看看能prefetch多少个？
            // 数一下从consumeOffset开始后面有多少有效消息
            // 再看看队列还能放多少个
            if (isEmpty() && q.consumeOffset > headOffset){
                // 说明之前有buffer不够用的情况，consumeOffset走得更快了，此时要更新一下headOffset和prefetchOffset
                headOffset = q.consumeOffset;
                tailOffset = q.consumeOffset;
                q.prefetchOffset = q.consumeOffset;
                // 相当于重置 prefetch buffer

            }
            log.debug("q.consumeOffset : " + q.consumeOffset + " q.prefetchOffset : " + q.prefetchOffset);
            long prefetchNum = (q.maxOffset-1)-q.prefetchOffset;
            log.debug("prefetch start from offset : " + q.prefetchOffset);
            log.debug("prefetchNum : " + prefetchNum);
            // 得到能够被预取的消息数量
            if (prefetchNum <= 0){
                // 没有需要预取的消息，或者所有消息都被预取了
                log.debug("nothing to prefetch or all msgs has been prefetched");
                return;
            }
            // 检查consumeOffset和队列的headOffset是否对应，如果不对应则释放相关数据
            // 被消费的时候应当释放队列相关资源
            assert (q.consumeOffset == headOffset);
            // 预取的数量最大为当前buffer剩余的空间，再多的也没法预取，确定真正要预取这么多个消息
            prefetchNum = Math.min(prefetchNum, (maxLength-length));
            if (prefetchNum <= 0){
                log.debug("the prefetch buffer is full");
                return ;
            }

            log.debug("prefetchNum : " + prefetchNum);

            // 从prefetchOffset开始prefetch，填满数组

            for (int i = 0; i < prefetchNum; i++){
                // FIXME: long转int，不太好
                long pos = q.offset2position.get((int)q.prefetchOffset); 
                ByteBuffer buf = df.read(pos);
                log.debug(buf);
                this.offer(buf);
                q.prefetchOffset++;
            }
            log.debug("prefetch " + prefetchNum + " msgs");

            return ;
        }

        public void directAddData(ByteBuffer data){
            // 把分配内存放在异步任务中完成
            if (block == null){
                long startTime = System.nanoTime();
                block = pmBlockPool.allocate();
                // block = pmHeap.allocateMemoryBlock(maxLength*slotSize);
                long endTime = System.nanoTime();
                log.debug("memory block allocate : " + (endTime - startTime) + " ns");
            }

            // if (block == null){
            //     long startTime = System.nanoTime();
            //     block = pmHeap.allocateMemoryBlock(maxLength*slotSize);
            //     long endTime = System.nanoTime();
            //     log.debug("memory block allocate : " + (endTime - startTime) + " ns");
            // }

            // 如果刚好需要预取这个数据，而且预取数量还不够，那就把这个数据加进去
            this.offer(data);
            q.prefetchOffset++;

            return ;
        }


        public boolean offer(ByteBuffer data){
            data.reset();
            if (isEmpty()){
                // 把一个消息放到队列末尾
                log.debug(data);
                msgLengths[tail] = data.remaining();
                block.copyFromArray(data.array(), data.position(), tail*slotSize, msgLengths[tail] );
                length ++;
                return true;
            }
            if (isFull()){
                return false;
            }
            // 把一个消息放到队列末尾
            tail += 1;
            tail = tail % maxLength;
            log.debug("put data on tail : " + tail);
            log.debug(data);
            msgLengths[tail] = data.remaining();
            block.copyFromArray(data.array(), data.position(), tail*slotSize, msgLengths[tail] );
            tailOffset ++;
            length ++;
            return true;
        }

        public ByteBuffer poll(){
            if (isEmpty()){
                return null;
            }
            // 把一个消息从队列头部去掉
            int msgLength = msgLengths[head];
            log.debug("msgLength : " + msgLength + " head : " + head);
            ByteBuffer buf;
            if (q.bbPool != null){
                buf = q.bbPool.allocate(msgLength);
            } else {
                buf = ByteBuffer.allocate(msgLength);
            }
            log.debug(buf);
            block.copyToArray(head*slotSize, buf.array(), buf.arrayOffset()+buf.position(), msgLength);
            log.debug(buf.arrayOffset());
            log.debug(buf);
            // block.copyToArray(head*slotSize, buf.array(), 0, msgLength);
            log.debug("get buffer from prefetchqueue : " + buf);

            if (length == 1){
                length --;
                return buf;
            }

            head ++;
            head = head % maxLength;
            headOffset ++;
            length --;
            return buf;
        }

        public  void reset(long consumeOffset){
            headOffset = consumeOffset;
            tailOffset = consumeOffset;
            head = 0;
            tail = 0;
            length = 0;
        }


        public boolean isFull(){
            return length == maxLength;
        }
        public boolean isEmpty(){
            return length == 0;
        }





    }

    public class MyByteBufferPool {
        int capacity;
        byte[] buffer;
        // AtomicInteger head;
        int head;
        int slotSize;
        int maxLength;
        MyByteBufferPool(){
            // head = new AtomicInteger();
            head = 0;
            // head.set(0);
            slotSize = 17*1024;
            maxLength = 400;
            capacity = maxLength * slotSize;
            buffer = new byte[capacity];
        }
        public  ByteBuffer allocate(int dataLength){
            // int thisHead = head.getAndAdd(1);
            ByteBuffer ret = ByteBuffer.wrap(buffer, head*slotSize, dataLength);
            ret.mark();
	    assert (ret.arrayOffset() == head*slotSize );
	    // log.info(ret.arrayOffset());
            head++;
            head = head % maxLength;
            return ret;
        }
    }

    public class MyPMBlock{
        public MemoryPool pool;
        public long addr;
        public int capacity;
        MyPMBlock(MemoryPool p, long a){
            pool = p;
            addr = a;
        }

        public void copyFromArray(byte[] srcArray, int srcIndex, long dstOffset, int length){
            pool.copyFromByteArrayNT(srcArray, srcIndex, addr+dstOffset, length);

        }
        public void copyToArray(long srcOffset, byte[] dstArray, int dstOffset, int length){
            pool.copyToByteArray(addr+srcOffset, dstArray, dstOffset, length);
        }
    }

    public class MyPMBlockPool2 { // TODO: 支持free的定长块分配器
        public MemoryPool pool;
        public int blockSize;
        public int bigBlockSize;
        public AtomicLong atomicGlobalFreeOffset;
        public long totalCapacity;
        public ThreadLocal<Long> threadLocalBigBlockStartAddr;
        public ThreadLocal<Integer> threadLocalBigBlockFreeOffset;
        public ConcurrentLinkedQueue<Long> bigBlockQueue;
        MyPMBlockPool2(String path){
            totalCapacity = 60L*1024*1024*1024;
            pool = MemoryPool.createPool(path, totalCapacity);

            blockSize = 8*17*1024; // 8 个slot
            bigBlockSize = 200*blockSize;

            long bigBlockCount = totalCapacity / bigBlockSize;

            for (long i = 0; i < bigBlockCount; i++){
                bigBlockQueue.offer(i*bigBlockSize);
            }
            atomicGlobalFreeOffset = new AtomicLong();
            atomicGlobalFreeOffset.set(0L);
            threadLocalBigBlockFreeOffset = new ThreadLocal<>();
            threadLocalBigBlockStartAddr = new ThreadLocal<>();
            threadLocalBigBlockFreeOffset.set(0);
        }
        public MyPMBlock allocate(){
            if (threadLocalBigBlockStartAddr.get() == null){
                allocateBigBlock();
            }
            if (threadLocalBigBlockFreeOffset.get() == bigBlockSize){
                // 大块满了，分配新的大块
                allocateBigBlock();
            }
            int freeOffset = threadLocalBigBlockFreeOffset.get();
            long addr = threadLocalBigBlockStartAddr.get() + freeOffset;
            threadLocalBigBlockFreeOffset.set(freeOffset+blockSize);

            return new MyPMBlock(pool, addr);
        }
        public void allocateBigBlock(){
            long bigBlockStartAddr = atomicGlobalFreeOffset.getAndAdd(bigBlockSize);
            log.debug("big block addr : " +bigBlockStartAddr);
            threadLocalBigBlockStartAddr.set(bigBlockStartAddr);
            threadLocalBigBlockFreeOffset.set(0);
            assert (atomicGlobalFreeOffset.get() < totalCapacity);
        }
    }

    public class MyPMBlockPool {
        public MemoryPool pool;
        public int blockSize;
        public int bigBlockSize;
        public AtomicLong atomicGlobalFreeOffset;
        public long totalCapacity;
        public ThreadLocal<Long> threadLocalBigBlockStartAddr;
        public ThreadLocal<Integer> threadLocalBigBlockFreeOffset;
        MyPMBlockPool(String path){
            totalCapacity = 60L*1024*1024*1024;
            pool = MemoryPool.createPool(path, totalCapacity);

            blockSize = 8*17*1024; // 8 个slot
            bigBlockSize = 200*blockSize;
            atomicGlobalFreeOffset = new AtomicLong();
            atomicGlobalFreeOffset.set(0L);
            threadLocalBigBlockFreeOffset = new ThreadLocal<>();
            threadLocalBigBlockStartAddr = new ThreadLocal<>();
            threadLocalBigBlockFreeOffset.set(0);
        }
        public MyPMBlock allocate(){
            if (threadLocalBigBlockStartAddr.get() == null){
                allocateBigBlock();
            }
            if (threadLocalBigBlockFreeOffset.get() == bigBlockSize){
                // 大块满了，分配新的大块
                allocateBigBlock();
            }
            int freeOffset = threadLocalBigBlockFreeOffset.get();
            long addr = threadLocalBigBlockStartAddr.get() + freeOffset;
            threadLocalBigBlockFreeOffset.set(freeOffset+blockSize);

            return new MyPMBlock(pool, addr);
        }
        public void allocateBigBlock(){
            long bigBlockStartAddr = atomicGlobalFreeOffset.getAndAdd(bigBlockSize);
            log.debug("big block addr : " +bigBlockStartAddr);
            threadLocalBigBlockStartAddr.set(bigBlockStartAddr);
            threadLocalBigBlockFreeOffset.set(0);
            assert (atomicGlobalFreeOffset.get() < totalCapacity);
        }
    }

    public class DataFile {
        public FileChannel dataFileChannel;
        public long curPosition;
        public ByteBuffer commonWriteBuffer;
        public int writerQueueBufferCapacity;
        public Queue<Writer> writerConcurrentQueue;

        public int bufMetadataLength; // 8B
        public int globalMetadataLength; // 8B


        private ExecutorService prefetchThread;
        public WriteStat writeStat;

        private class Writer {
            short topicIndex;
            int queueId;
            short length;
            ByteBuffer data;
            int done;
            long position;
            Thread currentThread;
            MQQueue q;
            Writer(short myTopicIndex, int myQueueId, ByteBuffer myData, Thread t){
                topicIndex = myTopicIndex;
                queueId = myQueueId;
                length = (short)myData.remaining();
                data = myData;
                currentThread = t;
                done = 0;
                position = 0L;
            }
            Writer(short myTopicIndex, int myQueueId, ByteBuffer myData, Thread t, MQQueue myQ){
                topicIndex = myTopicIndex;
                queueId = myQueueId;
                length = (short)myData.remaining();
                data = myData;
                currentThread = t;
                done = 0;
                position = 0L;
                q = myQ;
            }
        }
        DataFile(String dataFileName){
            try {
                File dataFile = new File(dataFileName);
                curPosition = 0L;
                // FIXME: resource leak ??
                dataFileChannel = new RandomAccessFile(dataFile, "rw").getChannel();
                // dataFileChannel.truncate(100L*1024L*1024L*1024L); // 100GiB
                dataFileChannel.force(true);
                writerQueueBufferCapacity = 128*1024;
                commonWriteBuffer = ByteBuffer.allocate(writerQueueBufferCapacity);
                // commonWriteBuffer = ByteBuffer.allocateDirect(writerQueueBufferCapacity);
                commonWriteBuffer.clear();

                writerConcurrentQueue = new ConcurrentLinkedQueue<>();

                globalMetadataLength = Short.BYTES + Integer.BYTES + Short.BYTES; // 8 byte
                bufMetadataLength = Integer.BYTES + Integer.BYTES;
                writeStat = new WriteStat();
                log.debug("init data file : " + dataFileName + " ok !");


                threadLocalReadMetaBuf = new ThreadLocal<>();

                // prefetchThread = Executors.newSingleThreadExecutor();
                prefetchThread = Executors.newFixedThreadPool(4);
            } catch (IOException ie) {
                ie.printStackTrace();
            }
        }



        public long syncSeqWritePushConcurrentQueueHeapBatchBufferPrefetch(Short topicIndex, int queueId, ByteBuffer data, MQQueue q){

            ByteBuffer writerBuffer = commonWriteBuffer;

            long position = bufMetadataLength;
            try {
                Writer w = new Writer(topicIndex, queueId, data, Thread.currentThread(),q);
                writerConcurrentQueue.offer(w);
                while (!(w.done == 1 || w.equals(writerConcurrentQueue.peek()) )){
                    LockSupport.park();
                }
                if (w.done == 1){
                    return w.position;
                }
                
                int bufLength = bufMetadataLength;
                int maxBufLength = mqConfig.maxBufLength;
                int bufNum = 0;
                int maxBufNum = mqConfig.maxBufNum;

                boolean continueMerge = true;
                Writer[] batchWriters = new Writer[maxBufNum];
                Iterator<Writer> iter = writerConcurrentQueue.iterator();
                Writer lastWriter = null;
                int dataLength = 0;
                int writeLength = 0;

                position += curPosition;
                while ( continueMerge ){
                    lastWriter = iter.next();
                    dataLength = lastWriter.length;
                    writeLength = globalMetadataLength + dataLength;
                    lastWriter.position = position;
                    batchWriters[bufNum] = lastWriter;
                    position += writeLength;
                    bufLength += writeLength;
                    bufNum += 1;
                    if (bufNum >= maxBufNum){
                        continueMerge = false;
                        if (mqConfig.useStats){
                            writeStat.incExceedBufNumCount();
                        }
                    }
                    if (bufLength >= maxBufLength){
                        continueMerge = false;
                        if (mqConfig.useStats){
                            writeStat.incExceedBufLengthCount();
                        }
                    }
                    if (!iter.hasNext()){
                        continueMerge = false;
                        if (mqConfig.useStats){
                            writeStat.incEmptyQueueCount();
                        }
                    }
                }
                long writePosition = curPosition;
                //  对齐到4K
                // assert (curPosition % 4096 == 0);
                if (mqConfig.useStats){
                    writeStat.addSample(bufLength);
                }
                bufLength = bufLength + (4096 - bufLength % 4096);
                curPosition += bufLength;

                // // // 预取内容，以后可以跑出一个异步任务来处理，写数据完成后再等待异步任务完成
                // for (int i = 0; i < bufNum; i++){
                //     Writer thisW = batchWriters[i];
                //     if (!thisW.q.prefetchBuffer.isFull()){
                //         // 不管如何，先去尝试预取一下内容，如果需要就从SSD读
                //         thisW.q.prefetchBuffer.prefetch();
                //         long thisOffset = thisW.q.maxOffset;
                //         if (!thisW.q.prefetchBuffer.isFull() && thisOffset == thisW.q.prefetchOffset){
                //             log.debug("double write !");
                //             // 如果目前要写入的数据刚好就是下一个要预取的内容
                //             // 双写
                //             thisW.data.reset();
                //             log.debug(thisW.data);
                //             thisW.q.prefetchBuffer.directAddData(thisW.data);
                //         }
                //     }
                // }


                writerBuffer.clear();
                writerBuffer.putInt(bufLength);
                writerBuffer.putInt(bufNum);
                for (int i = 0; i < bufNum; i++){
                    Writer thisW = batchWriters[i];
                    writerBuffer.putShort(thisW.topicIndex);
                    writerBuffer.putInt(thisW.queueId);
                    writerBuffer.putShort(thisW.length);
                    writerBuffer.put(batchWriters[i].data);
                }
                writerBuffer.flip();

                boolean needPrefetch = false;

                final int finalBufNum = bufNum;
                for (int i = 0; i < finalBufNum; i++){
                    Writer thisW = batchWriters[i];
                    if (!thisW.q.prefetchBuffer.isFull()){
                        needPrefetch = true;
                        break;
                    }
                }

                Future prefetchFuture = null;
                if (needPrefetch){
                    prefetchFuture = prefetchThread.submit(new Callable<Integer>(){
                        @Override
                        public Integer call() throws Exception {
                            long startTime = System.nanoTime();
                            for (int i = 0; i < finalBufNum; i++){
                                Writer thisW = batchWriters[i];
                                // 未知队列和热队列
                                if ( (thisW.q.type == 0 || thisW.q.type == 1) && !thisW.q.prefetchBuffer.isFull()){
                                    // 不管如何，先去尝试预取一下内容，如果需要就从SSD读
                                    thisW.q.prefetchBuffer.prefetch();
                                    // FIXME: bug ！ 实际上没有被双写，这个maxOffset后面会变的
                                    long thisOffset = thisW.q.maxOffset;
                                    if (!thisW.q.prefetchBuffer.isFull() && thisOffset == thisW.q.prefetchOffset){
                                        log.debug("double write !");
                                        // 如果目前要写入的数据刚好就是下一个要预取的内容
                                        // 双写
                                        thisW.data.reset();
                                        log.debug(thisW.data);
                                        thisW.q.prefetchBuffer.directAddData(thisW.data);
                                    }
                                }
                            }
                            long endTime = System.nanoTime();
                            log.debug("prefetch ok");
                            log.debug("time : " + (endTime - startTime) + " ns");
                            return 0;
                        }
                    });
                    for (int i = 0; i < finalBufNum; i++){
                        Writer thisW = batchWriters[i];
                        thisW.q.prefetchFuture = prefetchFuture;
                    }
                }


                // 希望这个写入的时间能够掩盖异步预取SSD和写PM 的过程
                dataFileChannel.write(writerBuffer, writePosition);
                dataFileChannel.force(true);

                // if ((int)prefetchFuture.get() !=  0 ){
                //     log.error("error !");
                //     System.exit(-1);
                // }


                while(true){
                    Writer ready = writerConcurrentQueue.poll();
                    if (!ready.equals(w)){
                        ready.done = 1;
                        LockSupport.unpark(ready.currentThread);
                    }
                    if (ready.equals(lastWriter)){
                        break;
                    }
                }

                if (!writerConcurrentQueue.isEmpty()){
                    LockSupport.unpark(writerConcurrentQueue.peek().currentThread);
                }
                position = w.position;
                // if (prefetchFuture != null){
                //     while (!prefetchFuture.isDone()){
                //         Thread.sleep(0, 10000);
                //     }
                // }


            } catch (Throwable ie) {
                ie.printStackTrace();
            }
            return position;

        }

        public long syncSeqWritePushConcurrentQueueHeapBatchBuffer(Short topicIndex, int queueId, ByteBuffer data){

            ByteBuffer writerBuffer = commonWriteBuffer;

            long position = bufMetadataLength;
            try {
                Writer w = new Writer(topicIndex, queueId, data, Thread.currentThread());
                writerConcurrentQueue.offer(w);
                while (!(w.done == 1 || w.equals(writerConcurrentQueue.peek()) )){
                    LockSupport.park();
                }
                if (w.done == 1){
                    return w.position;
                }
                
                int bufLength = bufMetadataLength;
                int maxBufLength = mqConfig.maxBufLength;
                int bufNum = 0;
                int maxBufNum = mqConfig.maxBufNum;

                boolean continueMerge = true;
                Writer[] batchWriters = new Writer[maxBufNum];
                Iterator<Writer> iter = writerConcurrentQueue.iterator();
                Writer lastWriter = null;
                int dataLength = 0;
                int writeLength = 0;

                position += curPosition;
                while ( continueMerge ){
                    lastWriter = iter.next();
                    dataLength = lastWriter.length;
                    writeLength = globalMetadataLength + dataLength;
                    lastWriter.position = position;
                    batchWriters[bufNum] = lastWriter;
                    position += writeLength;
                    bufLength += writeLength;
                    bufNum += 1;
                    if (bufNum >= maxBufNum){
                        continueMerge = false;
                        if (mqConfig.useStats){
                            writeStat.incExceedBufNumCount();
                        }
                    }
                    if (bufLength >= maxBufLength){
                        continueMerge = false;
                        if (mqConfig.useStats){
                            writeStat.incExceedBufLengthCount();
                        }
                    }
                    if (!iter.hasNext()){
                        continueMerge = false;
                        if (mqConfig.useStats){
                            writeStat.incEmptyQueueCount();
                        }
                    }
                }
                long writePosition = curPosition;
                //  对齐到4K
                // assert (curPosition % 4096 == 0);
                if (mqConfig.useStats){
                    writeStat.addSample(bufLength);
                }
                bufLength = bufLength + (4096 - bufLength % 4096);
                curPosition += bufLength;
                {
                    writerBuffer.clear();
                    writerBuffer.putInt(bufLength);
                    writerBuffer.putInt(bufNum);
                    for (int i = 0; i < bufNum; i++){
                        Writer thisW = batchWriters[i];
                        writerBuffer.putShort(thisW.topicIndex);
                        writerBuffer.putInt(thisW.queueId);
                        writerBuffer.putShort(thisW.length);
                        writerBuffer.put(batchWriters[i].data);
                    }
                    writerBuffer.flip();
                    dataFileChannel.write(writerBuffer, writePosition);
                    dataFileChannel.force(true);
                }

                for (int i = 0; i < bufNum; i++){
                    Writer ready = writerConcurrentQueue.poll();
                    if (!ready.equals(w)){
                        ready.done = 1;
                        LockSupport.unpark(ready.currentThread);
                    }
                }

                if (!writerConcurrentQueue.isEmpty()){
                    LockSupport.unpark(writerConcurrentQueue.peek().currentThread);
                }
                position = w.position;

            } catch (IOException ie) {
                ie.printStackTrace();
            }
            return position;

        }

        public long syncSeqWritePushConcurrentQueueHeapBatchBufferHotData(Short topicIndex, int queueId, ByteBuffer data, MQQueue q){

            ByteBuffer writerBuffer = commonWriteBuffer;

            long position = bufMetadataLength;
            try {
                Writer w = new Writer(topicIndex, queueId, data, Thread.currentThread(),q);
                writerConcurrentQueue.offer(w);
                while (!(w.done == 1 || w.equals(writerConcurrentQueue.peek()) )){
                    LockSupport.park();
                }
                if (w.done == 1){
                    return w.position;
                }
                
                int bufLength = bufMetadataLength;
                int maxBufLength = mqConfig.maxBufLength;
                int bufNum = 0;
                int maxBufNum = mqConfig.maxBufNum;

                boolean continueMerge = true;
                Writer[] batchWriters = new Writer[maxBufNum];
                Iterator<Writer> iter = writerConcurrentQueue.iterator();
                Writer lastWriter = null;
                int dataLength = 0;
                int writeLength = 0;

                position += curPosition;
                while ( continueMerge ){
                    lastWriter = iter.next();
                    dataLength = lastWriter.length;
                    writeLength = globalMetadataLength + dataLength;
                    lastWriter.position = position;
                    batchWriters[bufNum] = lastWriter;
                    position += writeLength;
                    bufLength += writeLength;
                    bufNum += 1;
                    if (bufNum >= maxBufNum){
                        continueMerge = false;
                        if (mqConfig.useStats){
                            writeStat.incExceedBufNumCount();
                        }
                    }
                    if (bufLength >= maxBufLength){
                        continueMerge = false;
                        if (mqConfig.useStats){
                            writeStat.incExceedBufLengthCount();
                        }
                    }
                    if (!iter.hasNext()){
                        continueMerge = false;
                        if (mqConfig.useStats){
                            writeStat.incEmptyQueueCount();
                        }
                    }
                }
                long writePosition = curPosition;
                //  对齐到4K
                // assert (curPosition % 4096 == 0);
                if (mqConfig.useStats){
                    writeStat.addSample(bufLength);
                }
                bufLength = bufLength + (4096 - bufLength % 4096);
                curPosition += bufLength;
                {
                    writerBuffer.clear();
                    writerBuffer.putInt(bufLength);
                    writerBuffer.putInt(bufNum);
                    for (int i = 0; i < bufNum; i++){
                        Writer thisW = batchWriters[i];
                        writerBuffer.putShort(thisW.topicIndex);
                        writerBuffer.putInt(thisW.queueId);
                        writerBuffer.putShort(thisW.length);
                        writerBuffer.put(batchWriters[i].data);
                    }
                    writerBuffer.flip();
                    dataFileChannel.write(writerBuffer, writePosition);
                    dataFileChannel.force(true);
                }

                while(true){
                    Writer ready = writerConcurrentQueue.poll();
                    if (!ready.equals(w)){
                        ready.done = 1;
                        LockSupport.unpark(ready.currentThread);
                    }
                    if (ready.equals(lastWriter)){
                        break;
                    }
                }

                if (!writerConcurrentQueue.isEmpty()){
                    LockSupport.unpark(writerConcurrentQueue.peek().currentThread);
                }
                position = w.position;

            } catch (IOException ie) {
                ie.printStackTrace();
            }
            return position;

        }

        public long syncSeqWritePushConcurrentQueueHeapBatchBuffer4K(Short topicIndex, int queueId, ByteBuffer data){

            ByteBuffer writerBuffer = commonWriteBuffer;

            long position = 0L;
            try {
                Writer w = new Writer(topicIndex, queueId, data, Thread.currentThread());
                writerConcurrentQueue.offer(w);
                while (!(w.done == 1 || w.equals(writerConcurrentQueue.peek()) )){
                    LockSupport.park();
                }
                if (w.done == 1){
                    return w.position;
                }
                
                int maxBufLength = mqConfig.maxBufLength;
                int maxBufNum = mqConfig.maxBufNum;

                Writer[] batchWriters = new Writer[maxBufNum];
                Iterator<Writer> iter = writerConcurrentQueue.iterator();
                Writer nextWriter = null;

                int min4KDiff = Integer.MAX_VALUE;
                int cur4KDiff = 0;

                int bufNum = 0;
                int bufLength = bufMetadataLength;

                int curBufNum = 0;
                int curBufLength = bufMetadataLength;


                // 确定聚合多少个，大于48KiB，小于64KiB，尽量靠近4K边界
                while (true){
                    if (!iter.hasNext()){
                        if (mqConfig.useStats){
                            writeStat.incEmptyQueueCount();
                        }
                        break;
                    }
                    nextWriter = iter.next();
                    curBufLength += globalMetadataLength + nextWriter.length;
                    curBufNum += 1;
                    cur4KDiff = 4096 - curBufLength % 4096;
                    if (curBufLength < 50*1024){
                        bufLength = curBufLength;
                        bufNum = curBufNum;
                        if (curBufNum >= maxBufNum){
                            if (mqConfig.useStats){
                                writeStat.incExceedBufNumCount();
                            }
                            break;
                        } else {
                            continue;
                        }
                    } else {
                        // 取离4K边界最近的
                        if (cur4KDiff < min4KDiff){
                            min4KDiff = cur4KDiff;
                            bufLength = curBufLength;
                            bufNum = curBufNum;
                        }
                        // 该停了
                        if (curBufLength >= maxBufLength){
                            if (mqConfig.useStats){
                                writeStat.incExceedBufLengthCount();
                            }
                            break;
                        }
                        if (curBufNum >= maxBufNum){
                            if (mqConfig.useStats){
                                writeStat.incExceedBufNumCount();
                            }
                            break;
                        }
                    }
                }
                // 给出bufNum和bufLength就够了

                position = curPosition;
                position += bufMetadataLength;

                Writer lastWriter = null;
                int writeLength = 0;
                iter = writerConcurrentQueue.iterator();
                for (int i = 0; i <bufNum; i++){
                    lastWriter = iter.next();
                    writeLength = globalMetadataLength + lastWriter.length;
                    lastWriter.position = position;
                    batchWriters[i] = lastWriter;
                    position += writeLength;
                }

                long writePosition = curPosition;
                //  对齐到4K
                // assert (curPosition % 4096 == 0);
                if (mqConfig.useStats){
                    writeStat.addSample(bufLength);
                }
                bufLength = bufLength + (4096 - bufLength % 4096);
                curPosition += bufLength;
                {
                    writerBuffer.clear();
                    writerBuffer.putInt(bufLength);
                    writerBuffer.putInt(bufNum);
                    for (int i = 0; i < bufNum; i++){
                        Writer thisW = batchWriters[i];
                        writerBuffer.putShort(thisW.topicIndex);
                        writerBuffer.putInt(thisW.queueId);
                        writerBuffer.putShort(thisW.length);
                        writerBuffer.put(batchWriters[i].data);
                    }
                    writerBuffer.flip();
                    dataFileChannel.write(writerBuffer, writePosition);
                    dataFileChannel.force(true);
                }

                while(true){
                    Writer ready = writerConcurrentQueue.poll();
                    if (!ready.equals(w)){
                        ready.done = 1;
                        LockSupport.unpark(ready.currentThread);
                    }
                    if (ready.equals(lastWriter)){
                        break;
                    }
                }

                if (!writerConcurrentQueue.isEmpty()){
                    LockSupport.unpark(writerConcurrentQueue.peek().currentThread);
                }
                position = w.position;

            } catch (IOException ie) {
                ie.printStackTrace();
            }
            return position;

        }

        public ThreadLocal<ByteBuffer> threadLocalReadMetaBuf;

        public ByteBuffer read(long position) {
            if (threadLocalReadMetaBuf.get() == null) {
                threadLocalReadMetaBuf.set(ByteBuffer.allocate(globalMetadataLength));
            }
            ByteBuffer readMeta = threadLocalReadMetaBuf.get();
            MyByteBufferPool bufPool = threadLocalByteBufferPool.get();
        
            readMeta.clear();
            try {
                int ret;
                ret = dataFileChannel.read(readMeta, position);
                readMeta.position(6);
                int dataLength = readMeta.getShort();
                ByteBuffer tmp;
                if (bufPool != null){
                    tmp = bufPool.allocate(dataLength);
                } else {
                    tmp = ByteBuffer.allocate(dataLength);
                }
                tmp.mark();
                ret = dataFileChannel.read(tmp, position + globalMetadataLength);
                tmp.reset();
                // log.debug(ret);
                return tmp;
            } catch (IOException ie) {
                ie.printStackTrace();
            }
        
            return null;
        }

        public class WriteStat{
            public int[] bucketBound;
            public int[] bucketCount;
            public int emptyQueueCount;
            public int exceedBufNumCount;
            public int exceedBufLengthCount;
            WriteStat(){
                bucketBound = new int[]{100, 4*1024, 16*1024, 32*1024, 48*1024, 52*1024, 56*1024, 60*1024, 64*1024, 68*1024, 72*1024 ,80*1024};
                // bucketBound = new int[]{100, 512, 1024, 2*1024, 4*1024, 8*1024, 16*1024, 32*1024, 48*1024, 56*1024, 64*1024, 80*1024 , 96*1024, 112*1024, 128*1024};
                // bucketBound = new int[]{100, 512, 1024, 2*1024, 4*1024, 8*1024, 16*1024, 32*1024, 48*1024, 64*1024, 80*1024 , 96*1024, 112*1024, 128*1024, 256*1024, 512*1024};

                bucketCount = new int[bucketBound.length-1];
                for (int i = 0; i < bucketCount.length; i++){
                    bucketCount[i] = 0;
                }
                emptyQueueCount = 0;
                exceedBufNumCount = 0;
                exceedBufLengthCount = 0;
            }
            public void addSample(int len){
                for (int i = 0; i < bucketCount.length; i++){
                    if (len < bucketBound[i+1]){
                        bucketCount[i]++;
                        break;
                    }
                }
            }
            public void incEmptyQueueCount(){
                emptyQueueCount++;
            }
            public void incExceedBufNumCount(){
                exceedBufNumCount++;
            }
            public void incExceedBufLengthCount(){
                exceedBufLengthCount++;
            }

            @Override
            public String toString() {
                String ret = "";
                ret += bucketBound[0] + " < ";
                for (int i = 0; i < bucketCount.length; i++){
                    ret += "[" + bucketCount[i] + "]";
                    ret += " < " + bucketBound[i+1] + " < "; 
                }
                return ret;
            }
            public void report(){
                log.info(this.toString());
            }
            public WriteStat clone(){
                WriteStat ret = new WriteStat();
                ret.emptyQueueCount = emptyQueueCount;
                ret.exceedBufLengthCount = exceedBufLengthCount;
                ret.exceedBufNumCount = exceedBufNumCount;
                ret.bucketBound = bucketBound.clone();
                ret.bucketCount = bucketCount.clone();
                return ret;
            }
        }

    }


    private TestStat testStat;
    private class TestStat {
        // report throughput per second
        ThreadLocal<Integer> threadId;
        AtomicInteger numOfThreads;
        Long startTime;
        Long endTime;
        Long opCount;
        AtomicBoolean reported;
        int[] oldTotalWriteBucketCount;
        MemoryUsage memoryUsage;

        private class ThreadStat {
            Long appendStartTime;
            Long appendEndTime;
            int appendCount;
            Long getRangeStartTime;
            Long getRangeEndTime;
            int getRangeCount;
            int hitHotDataCount;
            int hotDataAllocCount;
            int queueCount;
            int coldQueueCount;
            int hotQueueCount;
            Long writeBytes;
            int fetchCount;
            int readSSDCount;
            int coldFetchCount;
            int coldReadSSDCount;
            int hotFetchCount;
            int hotReadSSDCount;

            public int[] bucketBound;
            public int[] bucketCount;

            ThreadStat() {
                appendStartTime = 0L;
                appendEndTime = 0L;
                appendCount = 0;
                getRangeStartTime = 0L;
                getRangeEndTime = 0L;
                getRangeCount = 0;
                writeBytes = 0L;
                hitHotDataCount = 0;
                hotDataAllocCount = 0;
                queueCount = 0;
                coldQueueCount = 0;
                hotQueueCount = 0;
                fetchCount = 0;
                readSSDCount = 0;

                coldFetchCount = 0;
                coldReadSSDCount = 0;
                hotFetchCount = 0;
                hotReadSSDCount = 0;



                fetchCount = 0;
                readSSDCount = 0;
                reported = new AtomicBoolean();
                reported.set(false);

                bucketBound = new int[19];
                bucketBound[0] = 100;
                bucketBound[1] = 512;
                for (int i = 1; i <= 17; i++){
                    bucketBound[i+1] = i*1024;
                }
                bucketCount = new int[bucketBound.length-1];
                for (int i = 0; i < bucketCount.length; i++){
                    bucketCount[i] = 0;
                }
                MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
                memoryUsage = memory.getHeapMemoryUsage();
            }

            public ThreadStat clone() {
                ThreadStat ret = new ThreadStat();
                ret.appendStartTime = this.appendStartTime;
                ret.appendEndTime = this.appendEndTime;
                ret.appendCount = this.appendCount;
                ret.getRangeStartTime = this.getRangeStartTime;
                ret.getRangeEndTime = this.getRangeEndTime;
                ret.getRangeCount = this.getRangeCount;
                ret.writeBytes = this.writeBytes;
                ret.bucketBound = this.bucketBound.clone();
                ret.bucketCount = this.bucketCount.clone();
                return ret;
            }
            public void addSample(int len){
                for (int i = 0; i < bucketCount.length; i++){
                    if (len < bucketBound[i+1]){
                        bucketCount[i]++;
                        break;
                    }
                }
            }
        }

        ThreadStat[] oldStats;
        Long oldEndTime;
        ThreadStat[] stats;

        DataFile[] myDataFiles;
        DataFile.WriteStat[] oldWriteStats;

        // ThreadLocal< HashMap<Integer, Long> >
        // report operation per second
        TestStat(DataFile[] dataFiles) {
            threadId = new ThreadLocal<>();
            numOfThreads = new AtomicInteger();
            numOfThreads.set(0);
            stats = new ThreadStat[100];
            for (int i = 0; i < 100; i++) {
                stats[i] = new ThreadStat();
            }
            startTime = 0L;
            endTime = 0L;
            oldEndTime = 0L;
            opCount = 0L;
            myDataFiles = dataFiles;
            oldWriteStats = new DataFile.WriteStat[myDataFiles.length];
        }

        void updateThreadId() {
            if (threadId.get() == null) {
                int thisNumOfThread = numOfThreads.getAndAdd(1);
                threadId.set(thisNumOfThread);
                log.info("init thread id : " + thisNumOfThread);
            }
        }

        void incQueueCount(){
            int id = threadId.get();
            stats[id].queueCount++;
        }
        void incHotQueueCount(){
            int id = threadId.get();
            stats[id].hotQueueCount++;
        }
        void incColdQueueCount(){
            int id = threadId.get();
            stats[id].coldQueueCount++;
        }

        void incFetchMsgCount(int fetchNum){
            int id = threadId.get();
            stats[id].fetchCount += fetchNum;
        }

        void incReadSSDCount(int fetchNum){
            int id = threadId.get();
            stats[id].readSSDCount+= fetchNum;
        }

        void incColdFetchMsgCount(int fetchNum){
            int id = threadId.get();
            stats[id].coldFetchCount += fetchNum;
        }

        void incColdReadSSDCount(int fetchNum){
            int id = threadId.get();
            stats[id].coldReadSSDCount+= fetchNum;
        }

        void incHotFetchMsgCount(int fetchNum){
            int id = threadId.get();
            stats[id].hotFetchCount += fetchNum;
        }

        void incHotReadSSDCount(int fetchNum){
            int id = threadId.get();
            stats[id].hotReadSSDCount+= fetchNum;
        }




        void appendStart() {
            updateThreadId();
            int id = threadId.get();
            if (stats[id].appendStartTime == 0L) {
                stats[id].appendStartTime = System.nanoTime();
                // log.info("init append time");
            }
        }

        void getRangeStart() {
            updateThreadId();
            int id = threadId.get();
            if (stats[id].getRangeStartTime == 0L) {
                stats[id].getRangeStartTime = System.nanoTime();
                // log.info("init getRange time");
            }
        }

        void appendUpdateStat(String topic, int queueId, ByteBuffer data) {
            int id = threadId.get();
            stats[id].addSample(data.remaining());
            stats[id].appendEndTime = System.nanoTime();
            stats[id].appendCount += 1;
            stats[id].writeBytes += data.remaining();
            stats[id].writeBytes += Integer.BYTES; // metadata
            update();
        }

        void getRangeUpdateStat(String topic, int queueId, long offset, int fetchNum) {
            int id = threadId.get();
            stats[id].getRangeEndTime = System.nanoTime();
            stats[id].getRangeCount += 1;
            update();
        }

        void hitHotData(String topic, int queueId){
            int id = threadId.get();
            stats[id].hitHotDataCount += 1;
        }


        synchronized void update() {
            if (reported.get() == true){
                return;
            }
            if (startTime == 0L) {
                startTime = System.nanoTime();
                endTime = System.nanoTime();
            }
            opCount += 1;
            if (opCount % 10 == 0){
                return ;
            }
            Long curTime = System.nanoTime();
            if (curTime - endTime > 5L * 1000L * 1000L * 1000L) {
                endTime = curTime;
                reported.set(true);
                report();
                reported.set(false);
            }
        }

        synchronized void report() {
            // throughput, iops for append/getRange
            // writeBandwidth
            log.info("============================================================================");
            log.info("==================================report====================================");
            int getNumOfThreads = numOfThreads.get();
            double[] appendTpPerThread = new double[getNumOfThreads];
            double[] getRangeTpPerThread = new double[getNumOfThreads];
            double[] appendLatencyPerThread = new double[getNumOfThreads];
            double[] getRangeLatencyPerThread = new double[getNumOfThreads];
            double[] bandwidthPerThread = new double[getNumOfThreads];


            double appendThroughput = 0;
            double getRangeThroughput = 0;
            double appendLatency = 0;
            double getRangeLatency = 0;
            double writeBandwidth = 0; // MiB/s

            // total

            double elapsedTimeS = (endTime - startTime) / (double) (1000 * 1000 * 1000);
            for (int i = 0; i < getNumOfThreads; i++) {
                double appendElapsedTimeS = (stats[i].appendEndTime - stats[i].appendStartTime)
                        / ((double) (1000 * 1000 * 1000));
                double appendElapsedTimeMS = (stats[i].appendEndTime - stats[i].appendStartTime)
                        / ((double) (1000 * 1000));
                appendTpPerThread[i] = stats[i].appendCount / appendElapsedTimeS;
                appendLatencyPerThread[i] = appendElapsedTimeMS / stats[i].appendCount;
                double getRangeElapsedTimeS = (stats[i].getRangeEndTime - stats[i].getRangeStartTime)
                        / ((double) (1000 * 1000 * 1000));
                double getRangeElapsedTimeMS = (stats[i].getRangeEndTime - stats[i].getRangeStartTime)
                        / ((double) (1000 * 1000));
                getRangeTpPerThread[i] = stats[i].getRangeCount / getRangeElapsedTimeS;
                getRangeLatencyPerThread[i] = getRangeElapsedTimeMS / stats[i].getRangeCount;
                double dataSize = stats[i].writeBytes / (double) (1024 * 1024);
                bandwidthPerThread[i] = dataSize / elapsedTimeS;
            }

            for (int i = 0; i < getNumOfThreads; i++) {
                appendThroughput += appendTpPerThread[i];
                getRangeThroughput += getRangeTpPerThread[i];
                appendLatency += appendLatencyPerThread[i];
                getRangeLatency += getRangeLatencyPerThread[i];
                writeBandwidth += bandwidthPerThread[i];
            }
            // appendThroughput /= getNumOfThreads;
            // getRangeThroughput /= getNumOfThreads;
            appendLatency /= getNumOfThreads;
            getRangeLatency /= getNumOfThreads;
            // writeBandwidth /= getNumOfThreads; // bandwidth 不用平均，要看总的
            
            // 报告总的写入大小分布
            // int[] totalWriteBucketCount = new int[100];
            // int[] myBucketBound = stats[0].bucketBound;
            // for (int i = 0; i < 100; i++){
            //     totalWriteBucketCount[i] = 0;
            // }
            // int numOfBucket = stats[0].bucketCount.length;
            // for (int i = 0; i < getNumOfThreads; i++){
            //     for (int j = 0; j < numOfBucket; j++){
            //         totalWriteBucketCount[j] += stats[i].bucketCount[j];
            //     }
            // }

            // String totalWriteBucketReport = "";
            // totalWriteBucketReport += myBucketBound[0] + " < ";
            // for (int i = 0; i < numOfBucket; i++){
            //     totalWriteBucketReport += "[" + totalWriteBucketCount[i] + "]";
            //     totalWriteBucketReport += " < " + myBucketBound[i+1] + " < ";
            // }
            // log.info("[Total Append Data Dist]" + totalWriteBucketReport);

            // if (oldTotalWriteBucketCount != null) {
            //     int[] curWriteBucketCount = new int[100];
            //     for (int i = 0; i < numOfBucket; i++) {
            //         curWriteBucketCount[i] = totalWriteBucketCount[i] - oldTotalWriteBucketCount[i];
            //     }
            //     String curWriteBucketReport = "";
            //     curWriteBucketReport += myBucketBound[0] + " < ";
            //     for (int i = 0; i < numOfBucket; i++) {
            //         curWriteBucketReport += "[" + curWriteBucketCount[i] + "]";
            //         curWriteBucketReport += " < " + myBucketBound[i + 1] + " < ";
            //     }
            //     log.info("[Current Append Data Dist]" + curWriteBucketReport);
            // }

            // oldTotalWriteBucketCount = totalWriteBucketCount;

            double curAppendThroughput = 0;
            double curGetRangeThroughput = 0;
            double curAppendLatency = 0;
            double curGetRangeLatency = 0;
            double curWriteBandwidth = 0; // MiB/s
            double thisElapsedTimeS = 0;

            int[] curAppendCount = new int[getNumOfThreads];
            int[] curGetRangeCount = new int[getNumOfThreads];

            // current
            // get the stat for this period
            if (oldStats != null) {
                thisElapsedTimeS = (endTime - oldEndTime) / (double) (1000 * 1000 * 1000);
                for (int i = 0; i < getNumOfThreads; i++) {
                    double appendElapsedTimeMS = (stats[i].appendEndTime - oldStats[i].appendEndTime)
                            / ((double) (1000 * 1000));
                    double appendElapsedTimeS = (stats[i].appendEndTime - oldStats[i].appendEndTime)
                            / ((double) (1000 * 1000 * 1000));
                    double appendCount = stats[i].appendCount - oldStats[i].appendCount;
                    curAppendCount[i] = stats[i].appendCount - oldStats[i].appendCount;
                    appendTpPerThread[i] = (appendCount) / appendElapsedTimeS;
                    appendLatencyPerThread[i] = appendElapsedTimeMS / appendCount;
                    double getRangeElapsedTimeMS = (stats[i].getRangeEndTime - oldStats[i].getRangeEndTime)
                            / ((double) (1000 * 1000));
                    double getRangeElapsedTimeS = (stats[i].getRangeEndTime - oldStats[i].getRangeEndTime)
                            / ((double) (1000 * 1000 * 1000));
                    double getRangeCount = stats[i].getRangeCount - oldStats[i].getRangeCount;
                    curGetRangeCount[i] = stats[i].getRangeCount - oldStats[i].getRangeCount;
                    getRangeTpPerThread[i] = getRangeCount / getRangeElapsedTimeS;
                    getRangeLatencyPerThread[i] = getRangeElapsedTimeMS / getRangeCount;
                    double dataSize = (stats[i].writeBytes - oldStats[i].writeBytes) / (double) (1024 * 1024);
                    bandwidthPerThread[i] = dataSize / thisElapsedTimeS;
                }
                for (int i = 0; i < getNumOfThreads; i++) {
                    curAppendThroughput += appendTpPerThread[i];
                    curGetRangeThroughput += getRangeTpPerThread[i];
                    curAppendLatency += appendLatencyPerThread[i];
                    curGetRangeLatency += getRangeLatencyPerThread[i];
                    curWriteBandwidth += bandwidthPerThread[i];
                }
                // curAppendThroughput /= getNumOfThreads;
                // curGetRangeThroughput /= getNumOfThreads;
                curAppendLatency /= getNumOfThreads;
                curGetRangeLatency /= getNumOfThreads;
            }
            
            StringBuilder totalAppendStat = new StringBuilder();
            StringBuilder totalGetRangeStat = new StringBuilder();
            StringBuilder appendStat = new StringBuilder();
            StringBuilder getRangeStat = new StringBuilder();
            for (int i = 0; i < getNumOfThreads; i++){
                appendStat.append(String.format("%d,", curAppendCount[i]));
                getRangeStat.append(String.format("%d,", curGetRangeCount[i]));
                totalAppendStat.append(String.format("%d,", stats[i].appendCount));
                totalGetRangeStat.append(String.format("%d,", stats[i].getRangeCount));
            }
            String csvStat = String.format("%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,XXXX,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f",
                    writeBandwidth, elapsedTimeS, appendThroughput, appendLatency, getRangeThroughput, getRangeLatency,
                    curWriteBandwidth, thisElapsedTimeS, curAppendThroughput, curAppendLatency, curGetRangeThroughput,
                    curGetRangeLatency);

            log.info("csvStat      :"+csvStat);
            log.info("appendStat   :"+appendStat);
            log.info("getRangeStat :"+getRangeStat);
            log.info("total appendStat   :"+ totalAppendStat);
            log.info("total getRangeStat :"+totalGetRangeStat);

            // report hit hot data ratio
            StringBuilder hotDataHitCountReport = new StringBuilder();
            StringBuilder hotDataReport = new StringBuilder();
            StringBuilder fetchCountReport = new StringBuilder();
            StringBuilder readSSDCountReport = new StringBuilder();
            StringBuilder coldFetchCountReport = new StringBuilder();
            StringBuilder coldReadSSDCountReport = new StringBuilder();
            StringBuilder hotFetchCountReport = new StringBuilder();
            StringBuilder hotReadSSDCountReport = new StringBuilder();

            for (int i = 0; i < getNumOfThreads; i++){
                hotDataHitCountReport.append(String.format("%d,",(stats[i].hitHotDataCount)));
                hotDataReport.append(String.format("%.2f,",(double)(stats[i].hitHotDataCount)/stats[i].getRangeCount));
                fetchCountReport.append(String.format("%d,",(stats[i].fetchCount)));
                readSSDCountReport.append(String.format("%d,",(stats[i].readSSDCount)));
                hotFetchCountReport.append(String.format("%d,",(stats[i].hotFetchCount)));
                hotReadSSDCountReport.append(String.format("%d,",(stats[i].hotReadSSDCount)));
                coldFetchCountReport.append(String.format("%d,",(stats[i].coldFetchCount)));
                coldReadSSDCountReport.append(String.format("%d,",(stats[i].coldReadSSDCount)));
            }
            log.info("[hit hot data counter] : " + hotDataHitCountReport);
            log.info("[hit hot data] : " + hotDataReport);
            log.info("[fetch Msg Count ] : "+fetchCountReport);
            log.info("[read SSD Count] : "+readSSDCountReport);
            log.info("[HOT fetch Msg Count ] : "+hotFetchCountReport);
            log.info("[HOT read SSD Count] : "+hotReadSSDCountReport);
            log.info("[COLD fetch Msg Count ] : "+coldFetchCountReport);
            log.info("[COLD read SSD Count] : "+coldReadSSDCountReport);




            log.info("Memory Used (GiB) : "+memoryUsage.getUsed()/(double)(1024*1024*1024));

            // report write stat
            for (int i = 0; i < dataFiles.length; i++){
                if (oldWriteStats[i] != null){
                    // get total write stat and cur write stat
                    
                    DataFile.WriteStat curWriteStat = dataFiles[i].writeStat;
                    DataFile.WriteStat oldWriteStat = oldWriteStats[i];
                    String writeReport = "";
                    writeReport += "[Total ] File " + i;
                    writeReport += " " + "emptyQueueCount : " + curWriteStat.emptyQueueCount;
                    writeReport += " " + "exceedBufNumCount : " + curWriteStat.exceedBufNumCount;
                    writeReport += " " + "exceedBufLengthCount : " + curWriteStat.exceedBufLengthCount;
                    log.info(writeReport);
                    log.info("Write Size Dist : "+curWriteStat.toString());

                    // current

                    oldWriteStat.emptyQueueCount = curWriteStat.emptyQueueCount - oldWriteStat.emptyQueueCount;
                    oldWriteStat.exceedBufLengthCount = curWriteStat.exceedBufLengthCount - oldWriteStat.exceedBufLengthCount;
                    oldWriteStat.exceedBufNumCount = curWriteStat.exceedBufNumCount - oldWriteStat.exceedBufNumCount;
                    for (int j = 0; j < oldWriteStat.bucketCount.length; j++){
                        oldWriteStat.bucketCount[j] = curWriteStat.bucketCount[j] - oldWriteStat.bucketCount[j];
                    }

                    curWriteStat = oldWriteStat;
                    writeReport = "";
                    writeReport += "[Current ] File " + i;
                    writeReport += " " + "emptyQueueCount : " + curWriteStat.emptyQueueCount;
                    writeReport += " " + "exceedBufNumCount : " + curWriteStat.exceedBufNumCount;
                    writeReport += " " + "exceedBufLengthCount : " + curWriteStat.exceedBufLengthCount;
                    log.info(writeReport);
                    log.info("Write Size Dist : "+curWriteStat.toString());

 
                }
                oldWriteStats[i] = dataFiles[i].writeStat.clone();
            }


            StringBuilder queueCountReport = new StringBuilder();
            StringBuilder hotQueueCountReport = new StringBuilder();
            StringBuilder coldQueueCountReport = new StringBuilder();
            StringBuilder otherQueueCountReport = new StringBuilder();
            queueCountReport.append("[queueCount report]");
            hotQueueCountReport.append("[hot queueCount report]");
            coldQueueCountReport.append("[cold queueCount report]");
            otherQueueCountReport.append("[other queueCount report]");
            for (int i = 0; i < getNumOfThreads; i++){
                queueCountReport.append(String.format("%d,",stats[i].queueCount));
                hotQueueCountReport.append(String.format("%d,",stats[i].hotQueueCount));
                coldQueueCountReport.append(String.format("%d,",stats[i].coldQueueCount));
                otherQueueCountReport.append(String.format("%d,",stats[i].queueCount-stats[i].hotQueueCount-stats[i].coldQueueCount));
            }
            log.info(queueCountReport);
            log.info(hotQueueCountReport);
            log.info(coldQueueCountReport);
            log.info(otherQueueCountReport);




            // log.info(writeBandwidth+","+elapsedTimeS+","+appendThroughput+","+appendLatency+","+getRangeThroughput+","+getRangeLatency+",XXXXXX,"+curWriteBandwidth+","+thisElapsedTimeS+","+curAppendThroughput+","+curAppendLatency+","+curGetRangeThroughput+","+curGetRangeLatency);

            // deep copy
            oldStats = stats.clone();
            for (int i = 0; i < 100; i++) {
                oldStats[i] = stats[i].clone();
            }
            oldEndTime = endTime;
        }

        // report topic stat per second
    }

}
