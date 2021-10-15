package io.openmessaging;



import java.io.IOException;

import java.nio.channels.FileChannel;
import java.sql.Time;
import java.nio.ByteBuffer;
import java.io.RandomAccessFile;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import javax.print.DocFlavor;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.ArrayDeque;
import java.util.ArrayList;

import org.apache.log4j.spi.LoggerFactory;

import io.openmessaging.SSDqueue.HotData;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.net.SyslogAppender;

import java.lang.ThreadLocal;
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
import com.intel.pmem.llpl.Util;

import java.util.Comparator;


public class MyLSMessageQueue extends MessageQueue {
    private static final Logger log = Logger.getLogger(LSMessageQueue.class);
    private static final ByteBuffer globalByteBuffer = ByteBuffer.allocate(17 * 1024 * 40 * 100); //17kB * 40 * 100 线程
    private static final ByteBuffer globalPmemByteBuffer = ByteBuffer.allocate(17 * 1024 * 40 * 12); // 40 个线程，或许不需要
    // private static final ByteBuffer globalDirByteBuffer = ByteBuffer.allocateDirect(17 * 1024 * 40);
    // ThreadLocal<ByteBuffer> threadBuffers;

    public class MQConfig {
        Level logLevel = Level.INFO;
        // Level logLevel = Level.DEBUG;
        boolean useStats = true;
        int writeMethod = 12;
        int numOfDataFiles = 4;
        int maxBufNum = 8;
        int maxBufLength = 68*1024;
        boolean fairLock = true;

        int numOfThreads = 10;


        public String toString() {
            return String.format("useStats=%b | writeMethod=%d | numOfDataFiles=%d | maxBufLength=%d | maxBufNum=%d | ",useStats,writeMethod,numOfDataFiles,maxBufLength,maxBufNum);
            // return String.format("useStats=%b | writeMethod=%d | numOfDataFiles=%d | maxBufLength=%d | maxBufNum=%d | align to 4K !! ",useStats,writeMethod,numOfDataFiles,maxBufLength,maxBufNum);
        }
    }

    public class IndexInfo{
        // long index; 
        long offset; // the offset in SSD
        int dataSize;
        long handle; // the offset in PMEM
        public IndexInfo(long offset, int dataSize, long hanlde){
            this.offset = offset;
            this.dataSize = dataSize;
            this.handle = hanlde;
        }
    }

    public class MQQueue {
        public Long maxOffset = 0L;
        public ArrayList<Long> offset2position;
        public Map<Long, IndexInfo> offset2info;
        public DataFile df;
        // public byte[] maxOffsetData;
        public ByteBuffer maxOffsetData;
        public int type;

        public Long uselessIdx;
        public PmemManager.MyBlock block;
        

        MQQueue(DataFile dataFile){
            type = 0;
            maxOffset = 0L;
            offset2position = new ArrayList<>(512);
            df = dataFile;

            offset2info = new HashMap<>();

            uselessIdx = 0L;

            block = pmemManager.createBlock();
        }
        MQQueue(){
            type = 0;
            maxOffset = 0L;
            offset2position = new ArrayList<>(512);

            offset2info = new HashMap<>();

            uselessIdx = 0L;
            block = pmemManager.createBlock();
        }

    }

    public class MQTopic {
        public short topicId;
        public String topicName;
        public HashMap<Integer, MQQueue> id2queue;
        public DataFile df;

        public PrefetchTask prefetchTask;

        MQTopic(short myTopicId, String name, DataFile dataFile, PrefetchTask prefetchTask){
            topicId = myTopicId;
            topicName = name;
            id2queue = new HashMap<Integer, MQQueue>();
            df = dataFile;
            this.prefetchTask = prefetchTask;
        }


    }

    MQConfig mqConfig;
    private FileChannel metadataFileChannel;
    DataFile[] dataFiles;
    int numOfDataFiles;
    ConcurrentHashMap<String, MQTopic> topic2object;

    int numOfPrefetchThreads;
    PrefetchTask[] prefetchTasks;
    PmemManager pmemManager;
    ExecutorService exec = Executors.newFixedThreadPool(16); // 这个线程数会不会太多？

    public void shutdown(){
        for(int i=0; i<numOfPrefetchThreads; i++){
            prefetchTasks[i].t.interrupt();
        }
    }

    MyLSMessageQueue(String dbDirPath, String pmDirPath, MQConfig config){
        // SSDBench.runStandardBench(dbDirPath);
        mqConfig = config;
        init(dbDirPath, pmDirPath);

    }


    MyLSMessageQueue(String dbDirPath, String pmDirPath){
        // SSDBench.runStandardBench(dbDirPath);
        mqConfig = new MQConfig();
        init(dbDirPath, pmDirPath);

    }

    public void init(String dbDirPath, String pmDirPath) {
        try {
            log.setLevel(mqConfig.logLevel);
            log.info(mqConfig);

            topic2object = new ConcurrentHashMap<String, MQTopic>();
            String metadataFileName = dbDirPath + "/meta";

            Boolean crash = false;
            // whether the MQ is recover from existed file/db ?
            File metadataFile = new File(metadataFileName);
            if (metadataFile.exists() && !metadataFile.isDirectory()) {
                crash = true;
            }
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
                recover();
            }
            localThreadId = new ThreadLocal<>();
            numOfThreads = new AtomicInteger();
            numOfThreads.set(0);
            numOfTopics = new AtomicInteger();
            numOfTopics.set(1);        

            if (mqConfig.useStats) {
                testStat = new TestStat(dataFiles);
            }

            boolean initialized = Heap.exists(pmDirPath + "/cache");
            // Heap pmemHeap = initialized ? Heap.openHeap(pmDirPath + "/cache") : Heap.createHeap(pmDirPath + "/cache", 60 * 1024L * 1024L * 1024L);
           
            pmemManager = new PmemManager(pmDirPath + "/cache");
            // pmemManager = new PmemManager(pmemHeap);
            // init prefetchThreads
            numOfPrefetchThreads = mqConfig.numOfThreads;
            executor = Executors.newFixedThreadPool(numOfPrefetchThreads);
            prefetchTasks = new PrefetchTask[numOfPrefetchThreads];
            for(int i=0; i< numOfPrefetchThreads; i++){
                prefetchTasks[i] = new PrefetchTask();
            }

            executor.shutdown();

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
            mqTopic = new MQTopic(topicId, topic, dataFiles[dataFileId], null);
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
        // q.offset2info.put(q.maxOffset, new IndexInfo(position, dataSize, hanlde));
        // TODO: 需要变成 q.offset2info.add();
        long ret = q.maxOffset;
        q.maxOffset++;
        return ret;
    }


    class AsyWritePmemTask implements Callable<Integer>{
        MQQueue q;
        ByteBuffer data;
        public AsyWritePmemTask(MQQueue q, ByteBuffer data){
            this.q = q;
            this.data = data;
        }
        @Override
        public Integer call() throws Exception{
            return q.block.put(data);
        }
    }
    boolean isWritePmem = true;
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
            int dataFileId = threadId / 10;
            short topicId = getAndUpdateTopicId(topic);
            // int dataFileId = Math.floorMod(topic.hashCode(), numOfDataFiles);
            // mqTopic = new MQTopic(topic, dataFileId);
            // mqTopic = new MQTopic(topicId, topic, dataFiles[dataFileId]);
            int taskId = threadId % numOfPrefetchThreads;
            // 那我也每个 MQTopic 直接共用一个预取队列？
            mqTopic = new MQTopic(topicId, topic, dataFiles[dataFileId], prefetchTasks[taskId]);
            topic2object.put(topic, mqTopic);
            //

        }
        data = data.slice();

        q = mqTopic.id2queue.get(queueId);
        if (q == null){
            Integer queueIdObject = queueId;
            int dataFileId = Math.floorMod(topic.hashCode()+queueIdObject.hashCode(), numOfDataFiles);
            // q = new MQQueue(dataFileId);
            q = new MQQueue(dataFiles[dataFileId]); // 这个 df 是不是没用啊
            mqTopic.id2queue.put(queueId, q);
            if (mqConfig.useStats){
                testStat.incQueueCount();
            }
        }
        DataFile df = mqTopic.df; // 
        int size = data.remaining();
        // 先双写

        long time0 = System.nanoTime();

        ByteBuffer tmpData = data.duplicate();
        // 写 PMEM 的异步任务设计
        AsyWritePmemTask task = new AsyWritePmemTask(q, tmpData);
        FutureTask<Integer> futureTask = new FutureTask<Integer>(task);
        if(isWritePmem && q.type != 2){
            // 不是冷队列，开启双写
            exec.submit(futureTask); // 这里的 executor 要不要先申请好，还是直接申请一个？
        }
         // 发起一个异步任务表示写 pmem
        data.mark();

        // 记录写磁盘的时间
        long time1 = System.nanoTime();
        long position = df.syncSeqWritePushConcurrentQueueHeapBatchBufferHotData(mqTopic.topicId, queueId, data, q);

        q.offset2position.add(position);

        // 
        long writeSSDTime = System.nanoTime() - time1;

        long ret = q.maxOffset;
        data.reset();
        int dataSize = data.remaining();
        // hotDataBuf 也会频繁创建 ByteBuffer
        // TODO: 使用全局的 HotByteBuffer 来避免这个问题:
        // 17KB * 5000 * 100 = ? 好像不够，那 HotBuffer 不是不可行吗？
        ByteBuffer hotDataBuf;
        if (q.maxOffsetData == null || q.maxOffsetData.capacity() < dataSize){
            hotDataBuf = ByteBuffer.allocate(dataSize);
        } else {
            hotDataBuf = q.maxOffsetData;
        }
        data.rewind();
        hotDataBuf.clear();
        hotDataBuf.put(data);
        hotDataBuf.flip();
        q.maxOffsetData = hotDataBuf;

        // 等待异步任务的结束
        // if(q.type != 2){
        if(isWritePmem && q.type != 2){
            try{
                q.offset2info.put(q.maxOffset, new IndexInfo(position, size , futureTask.get()));
            }catch(Exception e){
                e.printStackTrace();
            }
        }else{
            q.offset2info.put(q.maxOffset, new IndexInfo(position, size , -1));
        }
        long asyncWritePmemTime = System.nanoTime() - time0;
        
        if(mqConfig.useStats)
            testStat.addAsycWriteTime(writeSSDTime, asyncWritePmemTime);
        // 需要把这个时间添加进一个记录类里面去
        q.maxOffset++;
       if(mqConfig.useStats)
           testStat.updateChunkUsage(q.block.chunkList.getUsage(), q.block.chunkList.getTotalPageNum());
        return ret;
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        log.debug("getRange : "+topic+","+queueId+","+offset+","+fetchNum);
        if (mqConfig.useStats){
            testStat.getRangeStart();
            testStat.getRangeUpdateStat(topic, queueId, offset, fetchNum);
        }
        Map<Integer, ByteBuffer> ret = new HashMap<>();
        MQTopic mqTopic;
        MQQueue q;
        mqTopic = topic2object.get(topic);  // topic2object 里面映射到 queues
        if (mqTopic == null) {
            return ret;
        }
        q = mqTopic.id2queue.get(queueId); // 再次映射到同一个 queueId 下的 q
        if (q == null){
            return ret;
        }
        
        if (offset >= q.maxOffset){
            return ret;
        }
        if (offset + fetchNum-1 >= q.maxOffset){
            fetchNum = (int)(q.maxOffset-offset);
        }

        if (offset == q.maxOffset-1){
            if (q.type == 0){
                q.type = 1; // hot
                if (mqConfig.useStats){
                    testStat.incHotQueueCount();
                }
            }
            if (q.maxOffsetData != null){
                if (mqConfig.useStats){
                    testStat.hitHotData(topic, queueId);
                }
                ret.put(0, q.maxOffsetData);
                return ret;
            }
        }
        if (offset == 0){
            if (q.type == 0){
                q.type = 2; // cold
                // 标记冷队列，然后做一些事情

                if (mqConfig.useStats){
                    testStat.incColdQueueCount();
                }
            }
        }
        // 发起一个预取任务
        PrefetchTask pTask =  mqTopic.prefetchTask;
        DataFile df = mqTopic.df;

        int loadNum = 12;
        pTask.offer(new WritePmemTask(df, offset + fetchNum, loadNum, q));

        
        long pos = 0;
        for (int i = 0; i < fetchNum; i++){
            long curOffset = offset + i;
            int intCurOffset = (int)curOffset;
            pos = q.offset2position.get(intCurOffset);
            long handle = q.offset2info.get(curOffset).handle;
            int size = q.offset2info.get(curOffset).dataSize;
            //handle = -1L;
            if(handle != -1L){
                // ByteBuffer buf = ByteBuffer.allocate(size);
                ByteBuffer buf = ByteBuffer.wrap(q.block.get(handle, size));
                // buf.flip(); // TODO: check wrap meaning
                log.debug("read from pmem");
                ret.put(i, buf);
                testStat.incColdReadPmemCount(1);
            }
            // 判断是否在 PMEM 中，若是，则从 PMEM 中读
            else{
                ByteBuffer buf = df.read(pos, i);
                if (buf != null){
                    // buf.position(0);
                    // buf.limit(buf.capacity());
                    buf.flip();
                    ret.put(i, buf);
                    log.debug("read from SSD");
                    testStat.incColdReadSSDCount(1);
                }
            }
        }
        return ret;
    }

    private ThreadLocal<Integer> localThreadId;
    private AtomicInteger numOfThreads;

    public int updateThreadId() {
        if (localThreadId.get() == null) {
            int thisNumOfThread = numOfThreads.getAndAdd(1);
            localThreadId.set(thisNumOfThread);
            log.info("init thread id : " + thisNumOfThread);
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


    public class DataFile {
        public FileChannel dataFileChannel;
        public long curPosition;
        public ByteBuffer commonWriteBuffer;
        public int writerQueueBufferCapacity;
        public Queue<Writer> writerConcurrentQueue;

        public int bufMetadataLength; // 8B
        public int globalMetadataLength; // 8B


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
            } catch (IOException ie) {
                ie.printStackTrace();
            }
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

        public ByteBuffer preFetchRead(long position, long index, int threadId) {
            if (threadLocalReadMetaBuf.get() == null) {
                threadLocalReadMetaBuf.set(ByteBuffer.allocate(globalMetadataLength));
            }
            ByteBuffer readMeta = threadLocalReadMetaBuf.get();
        
            readMeta.clear();
            try {
                int ret;
                ret = dataFileChannel.read(readMeta, position);
                readMeta.position(6);
                int dataLength = readMeta.getShort();

                // 获取线程的 id
                // int threadId;
                ByteBuffer tmp = globalPmemByteBuffer.duplicate();
                tmp.position(threadId * (17 * 1024 * 12)); // TODO: 这里只设了 12 个，后续调整预取个数时，需要调整这个写法
                tmp.limit(threadId * (17 * 1024 *12) +  ((int)index%12) * 1024 * 17 + dataLength);
                tmp = tmp.slice();
                // ByteBuffer tmp = ByteBuffer.allocate(dataLength);
                ret = dataFileChannel.read(tmp, position + globalMetadataLength);
                // log.debug(ret);
                return tmp;
            } catch (IOException ie) {
                ie.printStackTrace();
            }
            return null;
        }
        public ByteBuffer read(long position, int index) {
            if (threadLocalReadMetaBuf.get() == null) {
                threadLocalReadMetaBuf.set(ByteBuffer.allocate(globalMetadataLength));
            }
            ByteBuffer readMeta = threadLocalReadMetaBuf.get();

            readMeta.clear();
            try {
                int ret;
                ret = dataFileChannel.read(readMeta, position);
                readMeta.position(6);
                int dataLength = readMeta.getShort();

                // 获取线程的 id
                // int threadId;
                ByteBuffer tmp = globalByteBuffer.duplicate();
                int threadId = localThreadId.get(); // threadId 怎么搞？可以使用 ThreadLocal<Integer> 来记录，每次递增的设置
                tmp.position( threadId * (17 * 1024 *100) + index * 17 * 1024);
                tmp.limit(threadId * (17 * 1024 *100) + index * 17 * 1024 + dataLength);
                tmp = tmp.slice();
                // ByteBuffer tmp = ByteBuffer.allocate(dataLength);
                ret = dataFileChannel.read(tmp, position + globalMetadataLength);
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

    class WritePmemTask{
        public int loadNum;
        public long startIdx;
        public DataFile df;
        MQQueue q;
        public WritePmemTask(DataFile df, long startIdx, int loadNum, MQQueue q){
            this.df = df;
            this.startIdx = startIdx;
            this.loadNum = loadNum;
            this.q = q;
        }
    }
    ExecutorService executor;
    AtomicInteger prefetchThreadNums = new AtomicInteger(0);
    //ExecutorService executor = Executors.newFixedThreadPool(2);
    public class PrefetchTask{
        Queue<WritePmemTask> wPmemTaskConcurrentQueue; //
        Thread t;
        int numOfThrowTask = 0;
        int threadId;

        public PrefetchTask(){
            wPmemTaskConcurrentQueue = new ConcurrentLinkedQueue<>();
            // 放全局
            executor.execute(new Prefetch());
            // prefetch(); // 开启一个异步线程
            // t = 这个线程
        }
        void offer(WritePmemTask wTask){
            wPmemTaskConcurrentQueue.offer(wTask);
            LockSupport.unpark(t);
        }
        class Prefetch implements Runnable{
            @Override
            public void run(){
                t = Thread.currentThread();
                threadId = prefetchThreadNums.getAndIncrement();
                try {
                    // 往 PMEM 写入一些数据。
                    while(!Thread.currentThread().isInterrupted()){
                        int size = wPmemTaskConcurrentQueue.size();
                        //logger.info("---- write pmem --- Quese size: " + size);
                        for(int i=0; i<size; ++i){
                            WritePmemTask curTask = wPmemTaskConcurrentQueue.poll();
                            // 从 SSD 上读入数据并写入到 PMEM 上。               
                            for(long k=curTask.startIdx; k<curTask.loadNum && k + curTask.startIdx <= curTask.q.maxOffset ; ++k){
                              
                                // useless task:
                                long uselessIdx = curTask.q.uselessIdx;
                                
                                // 统计到底发生多少次丢弃任务的操作
                                
                                if(k < uselessIdx || curTask.q.offset2position.get((int) k) != -1L){
                                    testStat.incNumOfThrowTask();
                                    continue; // 丢弃任务不执行
                                } 
    
                                long offset = curTask.q.offset2info.get(k).offset;
                                int dataSize = curTask.q.offset2info.get(k).dataSize;
                                
                                ByteBuffer byteBuffer = curTask.df.preFetchRead(offset, k , threadId);
                                byteBuffer.flip();
                                
                                // 可以多次判断 uselessIdx 和 k 的值
                                // TODO: 写到 PMEM
                                long handle = curTask.q.block.put(byteBuffer);
                                // 更新 DRAM 中的 hashMap
                                if(handle == -1L){
                                    curTask.q.offset2info.get(k).handle = handle;
                                }
                                
                                //logger.info("test multithread!---- write pmem");
                            }
                            //logger.info("test multithread!---- write pmem");
                        } 
                        if(wPmemTaskConcurrentQueue.isEmpty()){
                            //logger.info("---------------writeThread sleep!-------------------");
                            LockSupport.park();
                        }
                    }
                }catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        
    }
    // public class PmemManager{
    //     Heap heap;
    //     AtomicLong size;
        
    //     public PmemManager(Heap heap){
    //         this.heap = heap;
    //         //logger.info(heap.size());
    //         this.size = new AtomicLong(heap.size());
    //     }
    //     long write(ByteBuffer byteBuffer){
    //         if(size.get() - byteBuffer.remaining() <= 1*1024L*1024L*1024L) return -1L; // TODO，预留一点，怕有额外空间开销
    //         MemoryBlock block = heap.allocateMemoryBlock(byteBuffer.remaining());
    //         block.copyFromArray(byteBuffer.array(), 0, 0, byteBuffer.remaining());
    //         size.addAndGet(-block.size() - 1024);
    //         return block.handle();
            
            
    //     }
    //     long write(byte[] array){
    //         if(size.get() - array.length <= 1*1024L*1024L*1024L) return -1L; 
    //         MemoryBlock block = heap.allocateMemoryBlock(array.length);
    //         block.copyFromArray(array, 0, 0, array.length);
    //         size.addAndGet(-block.size() - 1024);
    //         return block.handle();
    //     }
    //     void free(long handle, int dataSize){
    //         MemoryBlock block = heap.memoryBlockFromHandle(handle);
    //         long blockSize = block.size();
    //         block.freeMemory();
    //         size.addAndGet(blockSize);
    //     }
    //     int read(ByteBuffer byteBuffer, long handle, int dataSize){
    //         MemoryBlock block = heap.memoryBlockFromHandle(handle);
    //         byte[] dstArray = new byte[dataSize];
    //         block.copyToArray(0, dstArray, 0, dataSize);
    //         byteBuffer.put(dstArray);

    //         //logger.info("pmemManager read: " + new String(byteBuffer.array()));

    //         return dataSize;
    //     }
    // }


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

        AtomicLong numOfThrowTask;

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

            int coldReadPmemCount;
            int coldReadSSDCount;
            int chunkUsage;
            int totalPageNum;

            long writeSSDTime;
            long asyncWritePmemTime;

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
                reported = new AtomicBoolean();
                reported.set(false);

                coldReadPmemCount = 0;
                coldReadSSDCount = 0;
                chunkUsage = 0;
                totalPageNum = 0;

                writeSSDTime = 0L;
                asyncWritePmemTime = 0L;

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

                ret.coldQueueCount = this.coldQueueCount;
                ret.coldReadPmemCount = this.coldReadPmemCount;
                ret.hitHotDataCount = this.hitHotDataCount;
                ret.coldReadSSDCount = this.coldReadSSDCount;
                ret.writeSSDTime = this.writeSSDTime;
                ret.asyncWritePmemTime = this.asyncWritePmemTime;

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
            numOfThrowTask = new AtomicLong(0L);
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
        void incNumOfThrowTask(){
            numOfThrowTask.incrementAndGet();
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
        void incColdReadPmemCount(int fetchNum){
            int id = threadId.get();
            stats[id].coldReadPmemCount += fetchNum;
        }
        void incColdReadSSDCount(int fetchNum){
            int id = threadId.get();
            stats[id].coldReadSSDCount += fetchNum;
        }
        void updateChunkUsage(int usage, int total){
            int id = threadId.get();
            stats[id].chunkUsage = usage;
            stats[id].totalPageNum = total;
        }
        void addAsycWriteTime(long writeSSDTime, long asyncWritePmemTime){
            int id = threadId.get();
            stats[id].writeSSDTime = writeSSDTime;
            stats[id].asyncWritePmemTime = asyncWritePmemTime;
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
            for (int i = 0; i < getNumOfThreads; i++){
                hotDataHitCountReport.append(String.format("%d,",(stats[i].hitHotDataCount)));
                hotDataReport.append(String.format("%.2f,",(double)(stats[i].hitHotDataCount)/stats[i].getRangeCount));
            }
            log.info("[hit hot data counter] : " + hotDataHitCountReport);
            log.info("[hit hot data] : " + hotDataReport);


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
            StringBuilder asyWriteTimeReport = new StringBuilder();

            queueCountReport.append("[queueCount report]");
            hotQueueCountReport.append("[hot queueCount report]");
            coldQueueCountReport.append("[cold queueCount report]");
            asyWriteTimeReport.append("[wirte SSD and asy write report]");
            otherQueueCountReport.append("[other queueCount report]");
            for (int i = 0; i < getNumOfThreads; i++){
                queueCountReport.append(String.format("%d,",stats[i].queueCount));
                hotQueueCountReport.append(String.format("%d,",stats[i].hotQueueCount));
                coldQueueCountReport.append(String.format("%d,",stats[i].coldQueueCount));
                asyWriteTimeReport.append(String.format("{%d, + %d}, ", stats[i].writeSSDTime, stats[i].asyncWritePmemTime-stats[i].writeSSDTime));
                otherQueueCountReport.append(String.format("%d,",stats[i].queueCount-stats[i].hotQueueCount-stats[i].coldQueueCount));
            }
            log.info(queueCountReport);
            log.info(hotQueueCountReport);
            log.info(coldQueueCountReport);
            log.info(asyWriteTimeReport);
            log.info(otherQueueCountReport);
            log.info("[throw task nums]  " + numOfThrowTask.get());

            StringBuilder coldReadPmemCountReport = new StringBuilder("[cold read pmem count]");
            StringBuilder coldReadSSDCountReport = new StringBuilder("[cold read SSD count]");
            StringBuilder chunkUsageReport = new StringBuilder("[pmem usage]");
            for(int i = 0;i < getNumOfThreads;i++){
                coldReadPmemCountReport.append(String.format("%d,",stats[i].coldReadPmemCount));
                coldReadSSDCountReport.append(String.format("%d,", stats[i].coldReadSSDCount));
                chunkUsageReport.append(String.format("%d/%d,", stats[i].chunkUsage, stats[i].totalPageNum));
            }
            log.info(coldReadPmemCountReport);
            log.info(coldReadSSDCountReport);
            log.info(chunkUsageReport);



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
