package io.openmessaging;

import java.io.IOException;

import java.nio.channels.FileChannel;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.io.RandomAccessFile;
import java.util.concurrent.Future;
import java.io.File;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import java.util.function.IntUnaryOperator;

import java.util.HashMap;
import java.util.Iterator;

import java.util.Map;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import java.util.ArrayList;

import org.apache.log4j.spi.LoggerFactory;

import io.openmessaging.PMwrite.PMBlock;

// import io.openmessaging.PMDoubleWrite.PMBlock;
// import io.openmessaging.PMDoubleWrite.PMBlockPool;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.lang.ThreadLocal;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.Math;


// import io.openmessaging.SSDBench;



public class LSMessageQueue extends MessageQueue {
    public static final Logger log = Logger.getLogger(LSMessageQueue.class);
    // private static final MemoryPool pmPool = MemoryPool.createPool("/mnt/pmem/data", 60L*1024L*1024L);

    public class MQConfig {
        Level logLevel = Level.INFO;
        // Level logLevel = Level.DEBUG;
        boolean useStats = true;
        // boolean useStats = false;
        int writeMethod = 12;
        int numOfDataFiles = 4;
        int maxBufNum = 10;
        int maxBufLength = 256*1024;
        boolean fairLock = true;
        public String toString() {
            return String.format("useStats=%b | writeMethod=%d | numOfDataFiles=%d | maxBufLength=%d | maxBufNum=%d | ",useStats,writeMethod,numOfDataFiles,maxBufLength,maxBufNum);
            // return String.format("useStats=%b | writeMethod=%d | numOfDataFiles=%d | maxBufLength=%d | maxBufNum=%d | align to 4K !! ",useStats,writeMethod,numOfDataFiles,maxBufLength,maxBufNum);
        }
    }

    public void pmWriteShutdown(){
        // 把目前的buffer全部写到PM到，并且结束双写
        for (int i = 0; i < numOfDataFiles; i++){
            MyByteBuffer bf =  dataFiles[i].commonWriteBuffer;
            if (bf.block != null){
                bf.isFinished = true;
                log.info("shutdown the double write for thread " + i);
                // 确认刷PM任务完成
                if (bf.backgroundDoubleWriteFuture != null){
                    while (bf.backgroundDoubleWriteFuture.isDone() != true){
                        try {
                            Thread.sleep(1);
                        } catch (Exception e){
                            e.printStackTrace();
                        }
                    }
                    bf.backgroundDoubleWriteFuture = null;
                }
                // 触发刷盘任务，异步调用block的刷盘函数
                final PMBlock backgroundBlock = bf.block;
                if(bf.block != null && bf.curPositions[bf.curBufIndex] != 0){
                    pmWrite.pool.copyFromByteArrayNT(bf.commByteBuffers[bf.curBufIndex].array(), 0, backgroundBlock.addr , backgroundBlock.capacity);
                }
                bf.block = null;
            }
        }
    }
    public class MQQueue {
        public Long maxOffset = 0L;
        public ArrayList<Long> offset2position;
        public ArrayList<Long> offset2PMAddr;
        public ArrayList<Integer> offset2Length;
        public ArrayList<Integer> offset2DramAddr;

        public DataFile df;
        // public byte[] maxOffsetData;
        // public ByteBuffer maxOffsetData;
        public int type;
        // public long consumeOffset;
        //public QueuePrefetchBuffer prefetchBuffer;
        //public Future<Integer> prefetchFuture;
        public MyByteBufferPool bbPool;
        public MyDirectBufferPool dbPool;
        //  public ExecutorService prefetchThread;

        MQQueue(DataFile dataFile){
            // consumeOffset = 0L;
            type = 0;
            maxOffset = 0L;
            offset2position = new ArrayList<>(256);
            offset2PMAddr = new ArrayList<>(256);
            offset2Length = new ArrayList<>(256);
            offset2DramAddr = new ArrayList<>(256);
            df = dataFile;
            // prefetchFuture = null;
        }
        MQQueue(){
            // consumeOffset = 0L;
            type = 0;
            maxOffset = 0L;
            offset2position = new ArrayList<>(256);
            offset2PMAddr = new ArrayList<>(256);
            offset2Length = new ArrayList<>(256);
            offset2DramAddr = new ArrayList<>(256);
            // prefetchFuture = null;
        }
        public void initPrefetchBuffer(){
            // prefetchBuffer = new QueuePrefetchBuffer(this, df, bbPool);
        }

    }

    public class MQTopic {
        public short topicId;
        public String topicName;
        public HashMap<Integer, MQQueue> id2queue;
        public DataFile df;
        public int dataFileId;
        public int threadId;

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
    // ThreadLocal< HashMap<String, MQTopic> > threadLocalTopic2object;
    ThreadLocal<MyByteBufferPool> threadLocalByteBufferPool;
    ThreadLocal<MyDirectBufferPool> threadLocalDirectBufferPool;
    // public ThreadLocal<ExecutorService> threadLocalPrefetchThread;
    public ThreadLocal<Semaphore> threadLocalSemaphore;
    public ThreadLocal<ByteBuffer> threadLocalWriterBuffer;
    boolean isCrash;
    // public PMPrefetchBuffer pmRingBuffer;
    public PMDoubleWrite pmDoubleWrite;
    // public Writer[] appendWriterBuffer;
    // public DRAMbuffer dramBuffer;
    public byte[] dramBuffer;

    public ThreadLocal<MyDRAMbuffer> localDramBuffer;
    public MyDRAMbuffer[] DRAMbufferList;

    LSMessageQueue(String dbDirPath, String pmDirPath, MQConfig config){
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
        SSDBench.runStandardBench(dbDirPath);
        // PMBench.runStandardBench(pmDirPath);

        try {
            // 超时自动退出
            new Timer("timer").schedule(new TimerTask() {
                @Override
                public void run() {
                    log.info(Thread.currentThread().getName() + " Exit !");
                    System.exit(-1);
                }
            }, 900000);
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

            // pmRingBuffer = new PMPrefetchBuffer(pmDataFile);
            // pmDoubleWrite = new PMDoubleWrite(pmDataFile);

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
            localThreadId = new ThreadLocal<>();
            numOfThreads = new AtomicInteger();
            numOfThreads.set(0);
            numOfTopics = new AtomicInteger();
            numOfTopics.set(1);
            threadLocalByteBufferPool = new ThreadLocal<>();
            threadLocalDirectBufferPool = new ThreadLocal<>();
            // threadLocalPrefetchThread = new ThreadLocal<>();
            // threadLocalTopic2object = new ThreadLocal<>();
            threadLocalSemaphore = new ThreadLocal<>();
            threadLocalWriterBuffer = new ThreadLocal<>();
            // appendWriterBuffer = new Writer[400];

            DRAMbufferList = new MyDRAMbuffer[42];

            pmWrite = new PMwrite(pmDataFile);

            for(int i=0; i<42; i++){
                DRAMbufferList[i] = new MyDRAMbuffer();
            }

            if (mqConfig.useStats) {
                testStat = new TestStat(dataFiles);
            }
            if (crash) {
                log.info("recover !!");
                System.exit(-1);
                recover();
            }

            // dramBuffer = new DRAMbuffer();
            // dramBuffer = new byte[ 1 << 30];
            localDramBuffer = new ThreadLocal<>();

            
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
                        replayAppend(i, topicId, topic, queueId, bufPosition, length);
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

    public long replayAppend(int dataFileId,short topicId, String topic, int queueId, long position,int dataLength) {

        log.debug("replay append : " + topic + "," + queueId + "," + position);
        MQTopic mqTopic;
        MQQueue q;
        // if (threadLocalTopic2object.get() == null){
        //     threadLocalTopic2object.set(new HashMap<>());
        // }
        // HashMap<String, MQTopic> topic2object = threadLocalTopic2object.get();

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
        q.offset2Length.add(dataLength);
        long ret = q.maxOffset;
        q.maxOffset++;
        log.debug("replay ok");
        return ret;
    }

    public long append2(String topic, int queueId, ByteBuffer data){
        // 放数据
        data.mark();

        ByteBuffer writeDramData = data.duplicate();
        // ByteBuffer doubleWriteData = data.duplicate();
        log.debug("append : "+topic+","+queueId + data);
        if (mqConfig.useStats){
            testStat.appendStart();
            testStat.appendUpdateStat(topic, queueId, data);
        }
        MQTopic mqTopic;
        // TODO: maybe useless
        // 好家伙，全设成 localThread 变量
        if (threadLocalSemaphore.get() == null){
            // threadLocalTopic2object.set(new HashMap<>());
            threadLocalSemaphore.set(new Semaphore(0));
            // threadLocalWriterBuffer.set(ByteBuffer.allocateDirect(512*1024));  //writeBuffer 只分配 512 KB 
        }
        // HashMap<String, MQTopic> topic2object = threadLocalTopic2object.get();
        mqTopic = topic2object.get(topic);
        if (mqTopic == null) {
            int threadId = updateThreadId();
            int dataFileId = threadId % numOfDataFiles; 
            short topicId = getAndUpdateTopicId(topic);
            // int dataFileId = Math.floorMod(topic.hashCode(), numOfDataFiles);
            // mqTopic = new MQTopic(topic, dataFileId);
            mqTopic = new MQTopic(topicId, topic, dataFiles[dataFileId]);
            mqTopic.threadId = threadId;
            mqTopic.dataFileId = dataFileId;
            topic2object.put(topic, mqTopic);
        }

 
        log.debug("the init sema is " + threadLocalSemaphore.get().availablePermits());
        Writer w = new Writer(mqTopic.topicId, queueId, data, threadLocalSemaphore.get());
        
        DataFile df = mqTopic.df;
        int writerIndex = mqTopic.threadId / numOfDataFiles;

        df.syncSeqWriteAddWriterTryLock(writerIndex, w);
        // 不管拿不拿得到锁，都先做别的事情，然后等待完成
    
        MQQueue q;

        q = mqTopic.id2queue.get(queueId);
        if (q == null){
            q = new MQQueue(mqTopic.df); // 要和topic用一样的df
            q.bbPool = threadLocalByteBufferPool.get();
            q.dbPool = threadLocalDirectBufferPool.get();
            // q.prefetchThread = threadLocalPrefetchThread.get();

            q.initPrefetchBuffer();
            mqTopic.id2queue.put(queueId, q);
            if (mqConfig.useStats){
                testStat.incQueueCount();
            }
        }

       
        int dataLength = writeDramData.remaining();
        q.offset2Length.add(dataLength);


        if(q.type == 1){ // 热队列
            if(localDramBuffer.get() == null){
                // localDramBuffer.set(new MyDRAMbuffer());
                int threadId = mqTopic.threadId;
                localDramBuffer.set(DRAMbufferList[threadId]);
            }

            MyDRAMbuffer draMbuffer = localDramBuffer.get();
            int addr = draMbuffer.put(writeDramData);   
            if(addr == -1){
                testStat.incDramFullCount();
                testStat.dramBufferUsedReport(draMbuffer.toString());
            }
            q.offset2DramAddr.add(addr);
        }else{
            q.offset2DramAddr.add(-1);
        }
        // TODO: 看看有没有完成，如果没有完成就 1)等待完成 2）自己主动尝试获取锁去完成
        try {
            log.debug("wait to acquire the sema");

            // 有bug
            // w.sema.acquire(1);
            // 修好bug了
            if (!w.sema.tryAcquire(1, 500*1000, TimeUnit.MICROSECONDS)){
                // 我插入的writer可能要等待下一个能获取锁的写入线程帮我写入
                // 如果已经没有新的线程需要写入了，这个时候这个线程就会无限等待，此时需要有一个超时自救的机制
                if (w.done != 1){
                    log.debug("time out !");
                    df.syncSeqWriteBatchLock();
                    log.debug("my position result : " + w.position);
                }
                w.sema.acquire();
            }
        } catch (Exception ie){
            ie.printStackTrace();
        }

        long ret = q.maxOffset;
        q.maxOffset++;


        log.debug("add position " + w.position);
        q.offset2position.add(w.position);
        if(w.pmAddr != -1L){
            q.offset2PMAddr.add(w.pmAddr); 
        }
        // w.pmAddr = -1L; 并不需要这样做
        return ret;
    }
    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        return append2(topic, queueId, data);
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
     
        log.debug("getRange : "+topic+","+queueId+","+offset+","+fetchNum+" maxOffset: "+(q.maxOffset-1));
        // 更新一下offset和fetchNum，略去那些肯定没有的
        if (offset >= q.maxOffset){
            return ret;
        }
        if (offset + fetchNum-1 >= q.maxOffset){
            fetchNum = (int)(q.maxOffset-offset);
        }

        int fetchStartIndex = 0;
        // 尝试读双写的内容
        if (offset < q.offset2PMAddr.size()){
            // TODO: 需要处理一种情况：PM没写满就来读，这个怎么办？
            // 我在测试程序里手动执行shutdown，将这些buffer刷下来
            // 也可以在这里监测有没有完成，如果没完成就现场刷下来
            // 可以从双写 的内容里读数据
            // TODO: 需要释放前期双写所使用的内存buffer
            long fetchMaxOffset = offset + fetchNum - 1;
            long doubleWriteMaxOffset = q.offset2PMAddr.size()-1;
            long doubleWriteNum = Math.min(fetchMaxOffset, doubleWriteMaxOffset) - offset + 1;
            int intDoubleWriteNum =(int)doubleWriteNum;
            for (int i = 0; i < intDoubleWriteNum; i++){
                int curOffset = (int)offset+i;
                log.debug("curOffset : " + curOffset);
                int dataLength = q.offset2Length.get(curOffset);
                log.debug("get from double buffer datLength " + dataLength);
                // TODO: 需要修复
                ByteBuffer buf = q.bbPool.allocate(dataLength);
                // ByteBuffer buf = ByteBuffer.allocate(dataLength);
                long readPMAddr = q.offset2PMAddr.get(curOffset) + 2 * Short.BYTES + Integer.BYTES;

                pmWrite.pool.copyToByteArray(readPMAddr, buf.array(), buf.position(), dataLength);
                // log.info(buf);
                ret.put(i, buf);
            }
            fetchStartIndex += intDoubleWriteNum;
        }

        // 分类
        if(! isCrash){
            if(q.type == 0){
                if(offset != 0){ // 热队列
                    q.type = 1;
                }else{
                    q.type = 2; 
                }
            }
        }
        DataFile df = mqTopic.df;
        
        if(q.type == 1){ // 只有热队列才有可能读 DRAM
            int i = 0;
            MyDRAMbuffer dramBuffer = localDramBuffer.get();
            for(i=fetchStartIndex; i < fetchNum; i++){
                int curOffset = (int) offset + i;

                int addr = q.offset2DramAddr.get(curOffset);
                int dataLength = q.offset2Length.get(curOffset);
                if(addr != -1){
                    ByteBuffer buf = dramBuffer.read(addr, dataLength);
                    ret.put(i, buf);

                    testStat.incHitHotReadCount();

                }else{
                    long pos = q.offset2position.get(curOffset);
                    log.debug("read position : " + pos);
                    ByteBuffer buf = df.readData(pos,dataLength);
                    if (buf != null){
                        //buf.position(0);
                        //buf.limit(buf.capacity());
                        ret.put(i, buf);
                        testStat.incMissHotReadCount();
                    } 
                }
            }
            fetchStartIndex = i;
        }


        long pos = 0;
        for (int i = fetchStartIndex; i < fetchNum; i++){
            long curOffset = offset + i;
            int intCurOffset = (int)curOffset;
            pos = q.offset2position.get(intCurOffset);
            int dataLength = q.offset2Length.get(intCurOffset);
            log.debug("read position : " + pos);
            ByteBuffer buf = df.readData(pos,dataLength);
            if (buf != null){
                ret.put(i, buf);
            }
        }
        return ret;
    }

    public ByteBuffer debugReadCheck(String topic, int queueId, long offset){
        
        MQTopic mq = topic2object.get(topic);
        MQQueue q = mq.id2queue.get(queueId);
        if(offset < q.offset2PMAddr.size()){
            int dataLength = q.offset2Length.get((short) offset);
            ByteBuffer tmp1 = ByteBuffer.allocate( Short.BYTES * 2 + Integer.BYTES + dataLength);
            
            long pmAddr = q.offset2PMAddr.get((short) offset);
            // tmp1.putLong(pmAddr);

            log.error("[ADDR]: " + pmAddr);

            pmWrite.pool.copyToByteArray(pmAddr, tmp1.array(), tmp1.position(), dataLength + Short.BYTES * 2 + Integer.BYTES);
            // tmp1.flip();
            return tmp1;
        }
        return null;
    }

    public void close(){
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
        if (threadLocalDirectBufferPool.get() == null){
            threadLocalDirectBufferPool.set(new MyDirectBufferPool());
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


    // public class QueuePrefetchBuffer{ // 

    //     // 说明目前的这条ringBuffer的头从哪里开始
    //     public long headOffset;
    //     public long nextPrefetchOffset; // 下一个需要预取的offset
    //     // 缓存 [headOffset, tailOffset] 的内容

    //     public MQQueue q;
    //     public DataFile df;
    //     public RingBuffer ringBuffer;

    //     QueuePrefetchBuffer(MQQueue myQ, DataFile myDf, MyByteBufferPool myBBPool){
    //         headOffset = 0;
    //         nextPrefetchOffset = 0;
    //         q = myQ;
    //         df = myDf;
    //         // ringBuffer = pmRingBuffer.newRingBuffer(myBBPool);

    //     }

    //     public int consume(Map<Integer, ByteBuffer> ret, long offset, int fetchNum) {
    //         // offset 就是我当前要访问的offset，fetchNum就是我一定会访问这么多个，未来下一次被消费一定是 offset+fetchNum
    //         // 直接尝试开始消费
    //         // 始终假定 offset == q.consumeOffset， q.consumeOffset 是下一个要消费的offset
    //         log.debug("before consume");
    //         this.debuglog();
    //         try {
    //             // 如果队列为空，那么重置一下预取的各种offset，方便下次调用offset
    //             if (ringBuffer.isEmpty()){
    //                 headOffset = offset + fetchNum;
    //                 nextPrefetchOffset = offset + fetchNum;
    //                 return 0;
    //             }

    //             // 如果队列不为空，但是要访问的位置不是从headOffset开始
    //             // 那么 ，三种情况
    //             if (offset != headOffset) {
    //                 // 倒退，不预取了，清空重置吧
    //                 if (offset < headOffset) {
    //                     ringBuffer.reset();
    //                     headOffset = offset + fetchNum;
    //                     nextPrefetchOffset = offset + fetchNum;
    //                     return 0;
    //                 }
    //                 // 如果offset超过了要预取的内容
    //                 if (nextPrefetchOffset <= offset){
    //                     // 另一种是buf中所有内容都没用，直接重置这条buffer吧
    //                     ringBuffer.reset();
    //                     // 下一次预取的时候就是从这个offset开始预取
    //                     // 保证fetchNum不会超过maxOffset，所以如果按顺序访问的话，下一次一定从这里开始访问
    //                     headOffset = offset + fetchNum;
    //                     nextPrefetchOffset = offset + fetchNum;
    //                     return 0;
    //                 }
    //                 if (headOffset < offset && offset < nextPrefetchOffset) {
    //                     // 一种是 buf中还有需要消费的内容，那么就移动一下队列就好
    //                     // 说明当前要拿的数据还在buf中
    //                     // 先移动一下head，让队列符合 headOffset = offset 的假定
    //                     // TODO: 可以用justPoll
    //                     long num = offset - headOffset;
    //                     for (long i = 0; i < num; i++) {
    //                         ringBuffer.poll();
    //                     }
    //                     headOffset = offset;
    //                 }
    //             }
    //             // 假定 刚好匹配，一定是从headOffset开始读取
    //             // 想要fetchNum那么多个，但不一定有这么多
    //             int consumeNum = Math.min(fetchNum, ringBuffer.length);
    //             for (int i = 0; i < consumeNum; i++) {
    //                 ByteBuffer buf = ringBuffer.poll();
    //                 log.debug(buf);
    //                 ret.put(i, buf);
    //             }
    //             headOffset += consumeNum;
    //             return consumeNum;

    //         } finally {
    //             log.debug("after consume");
    //             this.debuglog();
    //         }
    //     }

    //     public void prefetch() {
    //         log.debug("before prefetch");
    //         this.debuglog();
    //         try {

    //             // 先看看能prefetch多少个？
    //             // 数一下从consumeOffset开始后面有多少有效消息
    //             // 再看看队列还能放多少个
    //             if (q.consumeOffset != headOffset) {
    //                 // 经常发生  q.consumeOffset > headOffset 这种情况，原因是，刚刚append的东西，getRange读不到，就导致必须读SSD，然后就导致consumeOffset超过HeadOffset
    //                 // log.info("q.consumeOffset > headOffset");
    //                 // 要求 q.consumeOffset 一定和 headOffset 相等，如果不相等就重置buffer
    //                 // TODO: 可以用just Poll
    //                 ringBuffer.reset();
    //                 log.debug("reset the ringBuffer !");
    //                 headOffset = q.consumeOffset;
    //                 nextPrefetchOffset = q.consumeOffset;
    //                 // 相当于重置 prefetch buffer
    //             }
    //             long prefetchNum = q.maxOffset - nextPrefetchOffset;
    //             // 得到能够被预取的消息数量
    //             if (prefetchNum <= 0) {
    //                 // 没有需要预取的消息，或者所有消息都被预取了
    //                 log.debug("nothing to prefetch or all msgs has been prefetched");
    //                 return;
    //             }
    //             // 预取的数量最大为当前buffer剩余的空间，再多的也没法预取，确定真正要预取这么多个消息
    //             prefetchNum = Math.min(prefetchNum, (ringBuffer.maxLength - ringBuffer.length));
    //             if (prefetchNum <= 0) {
    //                 log.debug("the prefetch buffer is full");
    //                 return;
    //             }

    //             // 从prefetchOffset开始prefetch，填满数组
    //             // TODO: 如果ringBuffer满了就不放了，早点停
    //             int actualPrefetchNum = 0;
    //             // FIXME: 不读就不知道消息有多长，这会造成一些额外的读取

    //             for (int i = 0; i < prefetchNum; i++) {
    //                 // FIXME: long转int，不太好
    //                 long pos = q.offset2position.get((int) nextPrefetchOffset);
    //                 ByteBuffer buf = df.read(pos);
    //                 log.debug(buf);
    //                 if (ringBuffer.offer(buf)) {
    //                     nextPrefetchOffset++;
    //                     actualPrefetchNum++;
    //                 } else {
    //                     break;
    //                 }
    //             }
    //             log.debug("prefetch " + actualPrefetchNum + " msgs");

    //         } finally {
    //             log.debug("after prefetch");
    //             this.debuglog();
    //         }
    //         return;
    //     }

    //     public boolean directAddData(long offset, ByteBuffer data){
    //         log.debug("before direct add data");
    //         this.debuglog();

    //         try {
    //             if (nextPrefetchOffset == offset){
    //                 // 如果刚好需要预取这个数据，而且预取数量还不够，那就把这个数据加进去
    //                 if (ringBuffer.offer(data)){
    //                     log.debug("double write !!");
    //                     nextPrefetchOffset ++;
    //                     return true  ;
    //                 }
    //             }
    //             log.debug("can not offer new data in ringBuffer");
    //             //  可能会加失败
    //             return false;
    //         } finally {
    //             log.debug("after direct add data");
    //             this.debuglog();
    //         }
    //     }
    //     public void debuglog(){
    //         StringBuilder output = new StringBuilder();
    //         output.append("headOffset : " + headOffset + " ");
    //         output.append("nextPrefetchOffset : " + nextPrefetchOffset + " ");
    //         output.append("q.consumeOffset : " + q.consumeOffset + " ");
    //         log.debug(output);
    //     }
    // }

    public class MyByteBufferPool {
        int capacity;
        byte[] buffer;
        AtomicInteger atomicHead;
        // int head;
        int slotSize;
        int maxLength;
        IntUnaryOperator getNext; // 一个操作方法
        MyByteBufferPool(){
            atomicHead = new AtomicInteger();
            atomicHead.set(0);
            // head = 0;
            slotSize = 17*1024;
            maxLength = 500;
            capacity = maxLength * slotSize;  // 分配 17 * 1024 * 500 个？
            buffer = new byte[capacity];
            getNext = (int curHead) -> {
                int nextHead = curHead+1;
                nextHead = nextHead % maxLength;
                return nextHead;
            };
        }
        public  ByteBuffer allocate(int dataLength){
            int thisHead = atomicHead.getAndUpdate(getNext);
            ByteBuffer ret = ByteBuffer.wrap(buffer, thisHead*slotSize, dataLength);
            // ByteBuffer ret = ByteBuffer.wrap(buffer, head*slotSize, dataLength);
            ret.mark(); // TODO: 为什么 mark?
	    // assert (ret.arrayOffset() == head*slotSize );
	    // log.info(ret.arrayOffset());
            // head++;
            // head = head % maxLength;
            return ret;
        }
    }
    public class MyDirectBufferPool {
        int capacity;
        // byte[] buffer;
        ByteBuffer directBuffer;
        AtomicInteger atomicHead;
        // int head;
        int slotSize;
        int maxLength;
        IntUnaryOperator getNext;
        MyDirectBufferPool(){
            atomicHead = new AtomicInteger();
            atomicHead.set(0);
            // head = 0;
            slotSize = 17*1024;
            maxLength = 500;
            capacity = maxLength * slotSize;
            // buffer = new byte[capacity];
            getNext = (int curHead) -> {
                int nextHead = curHead+1;
                nextHead = nextHead % maxLength;
                return nextHead;
            };
            directBuffer = ByteBuffer.allocateDirect(slotSize*maxLength);
        }
        public  ByteBuffer allocate(int dataLength){
            int thisHead = atomicHead.getAndUpdate(getNext);
            ByteBuffer ret = directBuffer.duplicate();
            ret.position(thisHead*slotSize);
            ret.limit(thisHead*slotSize+dataLength);
            ret.mark(); // 为什么 mark ?
            return ret;
        }
    }
        private class Writer {
            short topicIndex;
            int queueId;
            short length;
            ByteBuffer data;
            int needWrite;
            int done;
            long position;
            Thread currentThread;
            MQQueue q;
            Semaphore sema;
            AtomicBoolean isDone;
            long pmAddr;

            Writer(short myTopicIndex, int myQueueId, ByteBuffer myData, Thread t){
                topicIndex = myTopicIndex;
                queueId = myQueueId;
                length = (short)myData.remaining();
                data = myData;
                currentThread = t;
                done = 0;
                needWrite = 0;
                position = 0L;
                pmAddr = -1L;
            }
            Writer(short myTopicIndex, int myQueueId, ByteBuffer myData, Thread t, MQQueue myQ){
                topicIndex = myTopicIndex;
                queueId = myQueueId;
                length = (short)myData.remaining();
                data = myData;
                currentThread = t;
                done = 0;
                needWrite = 0;
                position = 0L;
                q = myQ;
                pmAddr = -1L;
            }
            Writer(short myTopicIndex, int myQueueId, ByteBuffer myData, Semaphore s){
                topicIndex = myTopicIndex;
                queueId = myQueueId;
                length = (short)myData.remaining();
                data = myData;
                sema = s;
                done = 0;
                position = 0L;
                isDone = new AtomicBoolean();
                isDone.set(false);
                pmAddr = -1L;
            }

        }
    

    PMwrite pmWrite;
    
    public class MyByteBuffer{

        int curBufIndex;
        ByteBuffer[] commByteBuffers;
        int minBufLen;
        int capacity;
        int curPositions[];

        PMBlock block;
        boolean isFinished;
        Future<Integer> backgroundDoubleWriteFuture;
        int bufferNum;
        public MyByteBuffer(int writerQueueBufferCapacity){
            curBufIndex = 0;
            bufferNum = 2;

            commByteBuffers = new ByteBuffer[bufferNum];
            capacity = 4 * (1 << 20); // 4*1024*1024
            for(int i=0; i<bufferNum; i++){
                commByteBuffers[i] = ByteBuffer.allocateDirect(capacity);
            }
            minBufLen = 512 * 1024;
            curPositions = new int[bufferNum];

            for(int i=0; i<bufferNum; i++){
                curPositions[i] = 0;
            }
            // curPositions[2] = 0;

            isFinished = false;
        }
        boolean writePmem(){
            if(isFinished) return false;
            if(block == null){
                block = pmWrite.pmBlockPool.allocate();
                if(block == null){
                    isFinished = true;
                    log.info("the pm is full!");
                    return false;
                }
            }
            
            if(backgroundDoubleWriteFuture != null){
                while(backgroundDoubleWriteFuture.isDone() != true){
                    try{
                        Thread.sleep(1);
                    }catch(Exception e){
                        e.printStackTrace();
                    }
                }
                backgroundDoubleWriteFuture = null;
                
            }
            final long backgroundAddr = block.addr;
            final int backgroundCapacity = block.capacity;

            final ByteBuffer flushBuf = commByteBuffers[curBufIndex]; 
            backgroundDoubleWriteFuture = pmWrite.backgroundDoubleWriteThread.submit(new Callable<Integer>(){
                @Override
                public Integer call() throws Exception {
                    pmWrite.pool.copyFromByteArrayNT(flushBuf.array(), 0, backgroundAddr , backgroundCapacity);
                    return 0;
                }
            });
            block = null;
            return true;
        }
        void changeBuf(){
            curBufIndex = (curBufIndex + 1) % bufferNum;
            curPositions[curBufIndex] = 0;
            commByteBuffers[curBufIndex].clear();
        }
        boolean isFull(){
            return capacity - curPositions[curBufIndex] < minBufLen;
        }
        ByteBuffer duplicate(int choice){
           
            ByteBuffer res = commByteBuffers[curBufIndex].duplicate();
            res.limit(capacity);
            res.position(curPositions[curBufIndex]);
            res = res.slice();
            return res; 
        }
        void updataCurPosition(int choice, int bufLength){
            curPositions[curBufIndex] += bufLength;
        }
        int getAddr(){
            return curPositions[curBufIndex];
        }
    }

    public class DataFile {
        public FileChannel dataFileChannel;
        public long curPosition;

        //public ByteBuffer commonWriteBuffer;
        public MyByteBuffer commonWriteBuffer;

        public int writerQueueBufferCapacity;
        public Queue<Writer> writerConcurrentQueue;

        public int bufMetadataLength; // 8B
        public int globalMetadataLength; // 8B


        // private ExecutorService prefetchThread;
        public WriteStat writeStat;
        public Lock dataFileLock;
        public Writer[] appendWriters;
        public int maxAppendWritersNum;

        DataFile(String dataFileName){
            try {
                File dataFile = new File(dataFileName);
                curPosition = 0L;
                // FIXME: resource leak ??
                dataFileChannel = new RandomAccessFile(dataFile, "rw").getChannel();
                // dataFileChannel.truncate(100L*1024L*1024L*1024L); // 100GiB
                dataFileChannel.force(true);
                writerQueueBufferCapacity = 4*1024*1024; // 不能大于一个 block 的大小
                // commonWriteBuffer = ByteBuffer.allocate(writerQueueBufferCapacity);
                commonWriteBuffer = new MyByteBuffer(writerQueueBufferCapacity);
                //commonWriteBuffer.clear();

                writerConcurrentQueue = new ConcurrentLinkedQueue<>();

                globalMetadataLength = Short.BYTES + Integer.BYTES + Short.BYTES; // 8 byte
                bufMetadataLength = Integer.BYTES + Integer.BYTES;
                writeStat = new WriteStat();
                log.debug("init data file : " + dataFileName + " ok !");


                threadLocalReadMetaBuf = new ThreadLocal<>();

                // prefetchThread = Executors.newSingleThreadExecutor();
                // prefetchThread = Executors.newFixedThreadPool(10);
                // prefetchThread = Executors.newCachedThreadPool();
                dataFileLock = new ReentrantLock();
                appendWriters = new Writer[100];
                maxAppendWritersNum = 10;

                // block = PMBlockPool.allocate();    
                

            } catch (IOException ie) {
                ie.printStackTrace();
            }
        }


        public void syncSeqWriteAddWriterTryLock(int writerIndex ,Writer w){
            log.debug("writerIndex : " + writerIndex);
            appendWriters[writerIndex*8] = w; // 这里为什么要 * 8，解决缓存伪共享？
            if (dataFileLock.tryLock() == true){
                log.debug("try to get the lock and success !");
                if (w.done == 1){
                    return ;
                }
                syncSeqWriteBatchInLock();
                dataFileLock.unlock();
            }
        }
        public void syncSeqWriteBatchLock(){
            try {
                dataFileLock.lock();
                log.debug("I get the lock!");
                syncSeqWriteBatchInLock();
            } finally {
                dataFileLock.unlock();
            }
        }

        public void syncSeqWriteBatchInLock(){
            long position = curPosition;
            position += bufMetadataLength;

            // 异步刷另外一个缓冲区到 PMEM 
            // 判断缓存区是否满了，满了，就异步刷 PMEM 
            // 
            if(commonWriteBuffer.isFull()){
                commonWriteBuffer.writePmem();
                commonWriteBuffer.changeBuf();
            }
            
            // long blockAddr = block.addr; // 同一个 dataFile
            
            ByteBuffer writerBuffer = commonWriteBuffer.duplicate(0); // 0 for seqWrite
            int bufferStartAddr = commonWriteBuffer.getAddr();
            PMBlock block = commonWriteBuffer.block;
            
            writerBuffer.clear();

            // log.info(writerBuffer.toString());

            int writeLength = 0;
            int bufNum = 0;
            int bufLength = bufMetadataLength;
            int maxBufLength = mqConfig.maxBufLength;
            int maxBufNum = mqConfig.maxBufNum;

            long writePosition = curPosition;
            writerBuffer.position(bufMetadataLength);
            boolean needWrite = false;
            for (int i = 0; i < maxAppendWritersNum; i++){
                Writer thisWriter = appendWriters[i*8];
                if (thisWriter != null && thisWriter.done == 0 && thisWriter.needWrite == 0){
                    log.debug("writer the index : " + i);
                    needWrite = true;
                    writeLength = globalMetadataLength + thisWriter.length;
                    thisWriter.position = position;
                    thisWriter.needWrite = 1;
                    log.debug("save position : " + position);
                    position += writeLength;
                    log.debug("update position to : " + position);
                    bufLength += writeLength;
                    bufNum += 1;
                    
                    thisWriter.pmAddr =  block == null ? -1L :  block.addr + writerBuffer.position() + bufferStartAddr;

                    writerBuffer.putShort(thisWriter.topicIndex);
                    writerBuffer.putInt(thisWriter.queueId);
                    writerBuffer.putShort(thisWriter.length);

                    
                     // 直接存地址, 这个地址可以先存进去

                    //log.info(thisWriter.pmAddr);

                    writerBuffer.put(thisWriter.data);

                    if (bufNum >= maxBufNum){
                        if (mqConfig.useStats){
                            writeStat.incExceedBufNumCount();
                        }
                        break;
                    }
                    if (bufLength >= maxBufLength){
                        if (mqConfig.useStats){
                            writeStat.incExceedBufLengthCount();
                        }
                        break;
                    }
                }
            }
            if (needWrite == false){
                return ;
            }
            // 对齐 4K
            commonWriteBuffer.updataCurPosition(0, bufLength); //  更新

            bufLength = bufLength + (4096 - bufLength % 4096);

            // log.info(writerBuffer);
            writerBuffer.flip();
            // log.info(writerBuffer);
            writerBuffer.putInt(bufLength);
            writerBuffer.putInt(bufNum);
            // log.info(writerBuffer);
            writerBuffer.position(0);
            // log.info(writerBuffer);
            // writerBuffer.position(0);
            

            try {
                dataFileChannel.write(writerBuffer, writePosition);
                dataFileChannel.force(true);
            } catch (Exception ie){
                ie.printStackTrace();
            }
            // TODO: 得找办法通知那些已经完成了的writer，让他们不要阻塞

            for (int i = 0; i < maxAppendWritersNum; i++){
                Writer thisWriter = appendWriters[i*8];
                if (thisWriter != null && thisWriter.done == 0 && thisWriter.needWrite == 1){
                    log.debug("release the index : " + i);
                    appendWriters[i*8] = null;
                    thisWriter.done = 1;
                    // thisWriter.isDone.set(true);
                    // log.debug("release 1");
                    // log.debug("the sema is " + thisWriter.sema.availablePermits());
                    thisWriter.sema.release(1);
                    // log.debug("the sema is " + thisWriter.sema.availablePermits());
                }
            }

            curPosition += bufLength;
            if (mqConfig.useStats){
                writeStat.addSample(bufLength);
            }
            log.debug("df.curPosition : " + curPosition);
        }

        public long syncSeqWritePushConcurrentQueueHeapBatchBufferPrefetch(Short topicIndex, int queueId, ByteBuffer data, MQQueue q){

            ByteBuffer writerBuffer = commonWriteBuffer.duplicate(0);

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
                //         long thisOffset = thisW.q.maxOffset-1;
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

                // boolean needPrefetch = false;

                // final int finalBufNum = bufNum;
                // for (int i = 0; i < finalBufNum; i++){
                //     Writer thisW = batchWriters[i];
                //     if (!thisW.q.prefetchBuffer.isFull()){
                //         needPrefetch = true;
                //         break;
                //     }
                // }

                // Future prefetchFuture = null;
                // if (needPrefetch){
                //     prefetchFuture = prefetchThread.submit(new Callable<Integer>(){
                //         @Override
                //         public Integer call() throws Exception {
                //             long startTime = System.nanoTime();
                //             for (int i = 0; i < finalBufNum; i++){
                //                 Writer thisW = batchWriters[i];
                //                 // 未知队列和热队列
                //                 if ( (thisW.q.type == 0 || thisW.q.type == 1) && !thisW.q.prefetchBuffer.isFull()){
                //                     // 不管如何，先去尝试预取一下内容，如果需要就从SSD读
                //                     thisW.q.prefetchBuffer.prefetch();
                //                     // FIXME: bug ！ 实际上没有被双写，这个maxOffset后面会变的
                //                     long thisOffset = thisW.q.maxOffset;
                //                     if (!thisW.q.prefetchBuffer.isFull() && thisOffset == thisW.q.prefetchOffset){
                //                         log.debug("double write !");
                //                         // 如果目前要写入的数据刚好就是下一个要预取的内容
                //                         // 双写
                //                         thisW.data.reset();
                //                         log.debug(thisW.data);
                //                         thisW.q.prefetchBuffer.directAddData(thisW.data);
                //                     }
                //                 }
                //             }
                //             long endTime = System.nanoTime();
                //             log.debug("prefetch ok");
                //             log.debug("time : " + (endTime - startTime) + " ns");
                //             return 0;
                //         }
                //     });
                //     for (int i = 0; i < finalBufNum; i++){
                //         Writer thisW = batchWriters[i];
                //         thisW.q.prefetchFuture = prefetchFuture;
                //     }
                // }


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

            ByteBuffer writerBuffer = null;

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

            ByteBuffer writerBuffer = null;

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

            ByteBuffer writerBuffer = null;

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
                threadLocalReadMetaBuf.set(ByteBuffer.allocateDirect(globalMetadataLength));
            }
            ByteBuffer readMeta = threadLocalReadMetaBuf.get();
            MyDirectBufferPool dbPool = threadLocalDirectBufferPool.get();
            MyByteBufferPool bbPool = threadLocalByteBufferPool.get();
        
            readMeta.clear();
            try {
                int ret;
                ret = dataFileChannel.read(readMeta, position);
                readMeta.position(6);
                int dataLength = readMeta.getShort();
                ByteBuffer tmp;
                if (bbPool != null){
                    tmp = bbPool.allocate(dataLength);
                // if (dbPool != null){
                    // tmp = dbPool.allocate(dataLength);
                } else {
                    tmp = ByteBuffer.allocate(dataLength);
                }
                tmp.mark();
                // log.info(tmp);
                ret = dataFileChannel.read(tmp, position + globalMetadataLength);
                // log.info(tmp);
                tmp.reset();
                // log.info(ret);
                return tmp;
            } catch (IOException ie) {
                ie.printStackTrace();
            }
        
            return null;
        }
        public ByteBuffer readData(long position, int dataLength) {
            MyDirectBufferPool dbPool = threadLocalDirectBufferPool.get();
            // MyByteBufferPool bbPool = threadLocalByteBufferPool.get();
        
            try {
                int ret;
                ByteBuffer tmp;
                // if (bbPool != null){
                //     tmp = bbPool.allocate(dataLength);
                if (dbPool != null){
                    tmp = dbPool.allocate(dataLength);
                } else {
                    tmp = ByteBuffer.allocate(dataLength);
                }
                tmp.mark();
                // log.info(tmp);
                ret = dataFileChannel.read(tmp, position + globalMetadataLength);
                // log.info(tmp);
                tmp.reset();
                // log.info(ret);
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

            int hitHotCount;
            int missHotCount;
            int dramBufferFullCount;

            String dramBufferUsedInfo;

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

                hitHotCount = 0;
                missHotCount = 0;
                dramBufferFullCount = 0;
                


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
                ret.dramBufferFullCount = this.dramBufferFullCount;
                ret.hitHotCount = this.hitHotCount;
                ret.missHotCount = this.missHotCount;
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

        void incHitHotReadCount(){
            int id = threadId.get();
            stats[id].hitHotCount++;
        }
        void incMissHotReadCount(){
            int id = threadId.get();
            stats[id].missHotCount++;
        }
        void incDramFullCount(){
            int id = threadId.get();
            stats[id].dramBufferFullCount++;
        }
        void dramBufferUsedReport(String str){
            int id = threadId.get();
            stats[id].dramBufferUsedInfo = str;
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
            //StringBuilder coldFetchCountReport = new StringBuilder();
            //StringBuilder coldReadSSDCountReport = new StringBuilder();
            //StringBuilder hotFetchCountReport = new StringBuilder();
            //StringBuilder hotReadSSDCountReport = new StringBuilder();
            StringBuilder dramReadReport = new StringBuilder();
            StringBuffer dramBufferUesdReport = new StringBuffer();
            for (int i = 0; i < getNumOfThreads; i++){
                hotDataHitCountReport.append(String.format("%d,",(stats[i].hitHotDataCount)));
                hotDataReport.append(String.format("%.2f,",(double)(stats[i].hitHotDataCount)/stats[i].getRangeCount));
                fetchCountReport.append(String.format("%d,",(stats[i].fetchCount)));
                readSSDCountReport.append(String.format("%d,",(stats[i].readSSDCount)));
                //hotFetchCountReport.append(String.format("%d,",(stats[i].hotFetchCount)));
                //hotReadSSDCountReport.append(String.format("%d,",(stats[i].hotReadSSDCount)));
                //coldFetchCountReport.append(String.format("%d,",(stats[i].coldFetchCount)));
                //coldReadSSDCountReport.append(String.format("%d,",(stats[i].coldReadSSDCount)));
                dramReadReport.append(String.format("Hot hit DRAM: %d, Hot miss DRAM: %d, full DRAM count: %d | ", stats[i].hitHotCount, stats[i].missHotCount, stats[i].dramBufferFullCount ));

                dramBufferUesdReport.append(String.format("%s| ", stats[i].dramBufferUsedInfo));
              
            }
            log.info("[hit hot data counter] : " + hotDataHitCountReport);
            log.info("[hit hot data] : " + hotDataReport);
            log.info("[fetch Msg Count ] : "+fetchCountReport);
            log.info("[read SSD Count] : "+readSSDCountReport);
            //log.info("[HOT fetch Msg Count ] : "+hotFetchCountReport);
            //log.info("[HOT read SSD Count] : "+hotReadSSDCountReport);
            //log.info("[COLD fetch Msg Count ] : "+coldFetchCountReport);
            //log.info("[COLD read SSD Count] : "+coldReadSSDCountReport);
            log.info("[READ DRAM buffer info] : " + dramReadReport);
            log.info("[DRAM buffer used info] : " + dramBufferUesdReport);



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
