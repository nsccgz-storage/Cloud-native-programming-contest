package io.openmessaging;



import java.io.IOException;

import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.io.RandomAccessFile;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.ArrayDeque;
import java.util.ArrayList;

import org.apache.log4j.spi.LoggerFactory;

import io.openmessaging.SSDqueue.HotData;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
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



import java.util.Comparator;


public class SSDqueue2 extends MessageQueue {
    private static final Logger log = Logger.getLogger(LSMessageQueue.class);
    public class MQConfig {
        Level logLevel = Level.INFO;
        // Level logLevel = Level.DEBUG;
        boolean useStats = true;
        int writeMethod = 12;
        int numOfDataFiles = 4;
        int maxBufNum = 8;
        int maxBufLength = 68*1024;
        boolean fairLock = true;
        int maxReaderBufNum = 12;
        public String toString() {
            return String.format("useStats=%b | writeMethod=%d | numOfDataFiles=%d | maxBufLength=%d | maxBufNum=%d | ",useStats,writeMethod,numOfDataFiles,maxBufLength,maxBufNum);
            // return String.format("useStats=%b | writeMethod=%d | numOfDataFiles=%d | maxBufLength=%d | maxBufNum=%d | align to 4K !! ",useStats,writeMethod,numOfDataFiles,maxBufLength,maxBufNum);
        }
    }


    public class MQQueue {
        public Long maxOffset = 0L;
        public ArrayList<Long> offset2position;
        public ArrayList<Short>offset2Size;
        public DataFile df;
        // public byte[] maxOffsetData;
        public ByteBuffer maxOffsetData;
        public int type;
        public static final int UNKNOWN = 0, HOT = 1, COLD = 2;
        public long consumeOffset; // 下一个被读的offset
        public long prefetchOffset; //
        public long headOffset = -1L;
        public long tailOffset = -1L;
        public CompletableFuture<Integer> prefetchFuture;

        public PmemManager.MyBlock block;

        int maxReaderMsgNum = 12;

        MQQueue(DataFile dataFile){
            consumeOffset = 0L;
            prefetchOffset = 0L;
            type = UNKNOWN;
            maxOffset = 0L;
            offset2position = new ArrayList<>(512);
            offset2Size = new ArrayList<>(512);
            df = dataFile;
            block = pmem.createBlock(localThreadId.get());
            prefetchFuture = null;
        }
        MQQueue(){
            consumeOffset = 0L;
            prefetchOffset = 0L;
            type = UNKNOWN;
            maxOffset = 0L;
            offset2position = new ArrayList<>(512);
            offset2Size = new ArrayList<>(512);
            block = pmem.createBlock(localThreadId.get());
            prefetchFuture = null;
        }
        public int prefetch1(){
            if(consumeOffset > prefetchOffset){
                // 当getRange需要读SSD时,consumeOffset会比prefetchOffset大
                // 此时需要重置 prefetch
                prefetchOffset = consumeOffset;
            }
            if(maxOffset < prefetchOffset){ // 说明最新写入的数据都在缓冲区了 // Check maxOffset
                log.debug("nothing to prefetch or all msgs has been prefetched");
                return 0;
            }
            long length = prefetchOffset - consumeOffset;
            // maxOffset-1表示最新写入的数据，保存在DRAM所以不考虑fetch?
            int prefetchNum = (int) Math.min(maxReaderMsgNum-length, maxOffset-1-prefetchOffset);
            if (prefetchNum <= 0){
                log.debug("the prefetch buffer is full");
                return 0;
            }

            log.info("maxOffset = "+maxOffset+" prefetchNum = "+prefetchNum+" prefetchOffset="+prefetchOffset);
            for (int i = 0; i < prefetchNum; i++) {
                long pos = offset2position.get((int) prefetchOffset);
                ByteBuffer buf = df.read(pos);
                buf.flip();
                if (buf.remaining() > block.getRemainSize()) {
                    boolean flag = block.doubleCapacity();
                    if (!flag) {
                        maxReaderMsgNum--; // TODO：试试看能否避免频繁扩容，最后会不会缩小得太小？
                        log.debug("not reach maxReaderNum but chunk is full, maxReaderNum=" + maxReaderMsgNum);
//                        break;
                        return i; // 返回fetch的个数
                    }
                }
                block.put(buf.array()); // TODO:应该可以这么写，这里考虑到buf马上就无效了
                prefetchOffset++;
                log.info("prefetchOffset="+prefetchOffset);
            }

            return prefetchNum;
        }
        public void prefetch(){
            prefetchFuture = CompletableFuture.supplyAsync(this::doAsyncPrefetch,df.prefetchThread);
            // 如果执行成功:
//            prefetchFuture.thenAccept((result) -> {
//                System.out.println("price: " + result);
//            });
            // 如果执行异常:
            prefetchFuture.exceptionally((e) -> {
                e.printStackTrace();
                return null;
            });
        }
        public int doAsyncPrefetch(){
            if(consumeOffset > prefetchOffset){
                // 当getRange需要读SSD时,consumeOffset会比prefetchOffset大
                // 此时需要重置 prefetch
                prefetchOffset = consumeOffset;
            }
            if(maxOffset < prefetchOffset){ // 说明最新写入的数据都在缓冲区了 // Check maxOffset
                log.debug("nothing to prefetch or all msgs has been prefetched");
                return 0;
            }
            long length = prefetchOffset - consumeOffset;
            // maxOffset-1表示最新写入的数据，保存在DRAM所以不考虑fetch?
            int prefetchNum = (int) Math.min(maxReaderMsgNum-length, maxOffset-1-prefetchOffset);
            if (prefetchNum <= 0){
                log.debug("the prefetch buffer is full");
                return 0;
            }
            for(int i = 0;i < prefetchNum;i++){
                    long pos = offset2position.get((int) prefetchOffset);
                    ByteBuffer buf = df.read(pos);
                    buf.flip();
                    if(buf.remaining() > block.getRemainSize()){
                        boolean flag = block.doubleCapacity();
                        if(!flag){
                            maxReaderMsgNum--; // TODO：试试看能否避免频繁扩容，最后会不会缩小得太小？
                            log.debug("not reach maxReaderNum but chunk is full, maxReaderNum="+maxReaderMsgNum);
//                        break;
                            return i; // 返回fetch的个数
                        }
                    }
                    block.put(buf.array()); // TODO:应该可以这么写，这里考虑到buf马上就无效了
                    prefetchOffset++;
            }
            return prefetchNum;
        }

        @Override
        public String toString() {
            return "MQQueue{" +
                    "maxOffset=" + maxOffset +
                    ", \noffset2position=" + offset2position +
                    ", \noffset2Size=" + offset2Size +
                    ", \ndf=" + df +
                    ", \nmaxOffsetData=" + maxOffsetData +
                    ", type=" + type +
                    ", consumeOffset=" + consumeOffset +
                    ", prefetchOffset=" + prefetchOffset +
                    ", prefetchFuture=" + prefetchFuture +
                    ", block=" + block +
                    ", maxReaderMsgNum=" + maxReaderMsgNum +
                    '}';
        }

        public void directAddData(ByteBuffer data){
            if(block.getRemainSize()>=data.remaining()){
                block.put(data);
                prefetchOffset++;
            }
        }

        public int consume(Map<Integer, ByteBuffer> ret, long offset,  int fetchNum){
            // 始终假定 offset >= q.consumeOffset
            if (offset > consumeOffset){
                if ( offset < prefetchOffset){
                    // 说明当前要拿的数据还在buf中
                    // 先移动一下head，让队列符合 consumeOffset = offset 的假定
                    int size = 0;
                    for (int i = (int) consumeOffset; i < offset; i++){
                        size += offset2Size.get(i);
                    }
                    block.headForward(size);
                    consumeOffset = offset;
                    log.info("head forward");
                } else {
                    // 说明当前要拿的数据不在buf中
                    block.reset();
                    // TODO: consumeOffset & prefetchOffset 记得调整
                    consumeOffset = offset;
                    return 0;
                }
            }
            // read from consume offset
            int consumeNum = Math.min(fetchNum, (int)(prefetchOffset-consumeOffset));
//            int dataSize = 0;
            if(consumeNum < 0){
                log.error("consume num < 0!!!");
            }
            byte[] bytes;
            for (int i = 0; i < consumeNum; i++){
                int tmp = offset2Size.get((int) (offset+i));
//                dataSize += tmp;
                ByteBuffer buffer = ByteBuffer.allocate(tmp);
                buffer.put(block.get(tmp));
                buffer.flip();
                ret.put(i, buffer);
            }
            return consumeNum;
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
    PmemManager pmem;

    SSDqueue2(String dbDirPath, String pmDirPath, MQConfig config){
        // SSDBench.runStandardBench(dbDirPath);
        mqConfig = config;
        init(dbDirPath, pmDirPath);
    }


    SSDqueue2(String dbDirPath, String pmDirPath){
//        SSDBench.runStandardBench(dbDirPath);
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
                log.info(metadataFile.getAbsolutePath());
                System.exit(-1);
                // recover();
            }
            localThreadId = new ThreadLocal<>();
            numOfThreads = new AtomicInteger();
            numOfThreads.set(0);
            numOfTopics = new AtomicInteger();
            numOfTopics.set(1);

            if (mqConfig.useStats) {
                testStat = new TestStat(dataFiles);
            }

            pmem = new PmemManager(pmDirPath);

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
                        bufPosition += 8+length; // TODO:check length是数据长度
                    }
                    curPosition += bufLength;
                    bufMetadata.clear();
                }
            }


        } catch (IOException ie) {
            ie.printStackTrace();
        }


    }

    public long replayAppend(int dataFileId,short topicId, String topic, int queueId, long position, short length) {
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
        q.offset2Size.add(length);
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
//        data = data.slice();
        data.mark();

        q = mqTopic.id2queue.get(queueId);
        if (q == null){
            Integer queueIdObject = queueId;
            int dataFileId = Math.floorMod(topic.hashCode()+queueIdObject.hashCode(), numOfDataFiles);
            // q = new MQQueue(dataFileId);
            q = new MQQueue(dataFiles[dataFileId]);
//            if(q.block == null){
//                log.error("current queue num ="+mqTopic.id2queue.size());
//            }
            mqTopic.id2queue.put(queueId, q);
            if (mqConfig.useStats){
                testStat.incQueueCount();
            }
        }

//        if(q.prefetchFuture == null){ // 没有在执行的异步任务
//            data.reset();
//            ByteBuffer dataDup = data.duplicate();
//            if((q.type == MQQueue.UNKNOWN)||(q.type == MQQueue.HOT)){
//                q.prefetch1();
//                if(q.prefetchOffset == q.maxOffset-1)
//                    q.directAddData(dataDup);
//            }
//        }


        // ......
//        q.block.put(data); // TODO: async write to pmem
        data.reset();

        q.offset2Size.add((short) data.remaining());
        DataFile df = mqTopic.df;

        long position = df.syncSeqWritePushConcurrentQueueHeapBatchBufferHotData(mqTopic.topicId, queueId, data, q);
        q.offset2position.add(position);
        long ret = q.maxOffset;
        q.maxOffset++;


        // 确保和这个queue相关的异步任务已完成
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

        data.reset();
        int dataSize = data.remaining();
//        int dataSize = data.capacity();
        ByteBuffer hotDataBuf;
        if (q.maxOffsetData == null || q.maxOffsetData.capacity() < dataSize){
            hotDataBuf = ByteBuffer.allocate(dataSize);
        } else {
            hotDataBuf = q.maxOffsetData;
        }
        // data.rewind();
        hotDataBuf.clear();
        hotDataBuf.put(data);
        hotDataBuf.flip();
        q.maxOffsetData = hotDataBuf;

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
        mqTopic = topic2object.get(topic);
        if (mqTopic == null) {
            return ret;
        }
        q = mqTopic.id2queue.get(queueId);
        if (q == null){
            return ret;
        }

        if (offset >= q.maxOffset){
            return ret;
        }
        if (offset + fetchNum-1 >= q.maxOffset){
            fetchNum = (int)(q.maxOffset-offset);
        }

        int fetchStartIndex = 0;
         // 确保和这个queue相关的异步任务已完成
//        if (q.prefetchFuture != null){
////            log.error("before prefetch "+q);
//            while (!q.prefetchFuture.isDone()){
//                try {
//                    Thread.sleep(0, 10000);
//                } catch (Throwable ie){
//                    ie.printStackTrace();
//                }
//            }
////            log.error("after prefetch "+q);
//            q.prefetchFuture = null;
//        }
//        log.error(String.format("fetchStartIndex = %d, consumeOffset = %d, prefetchOffset = %d",fetchStartIndex,q.consumeOffset, q.prefetchOffset));
        int consumeNum = q.consume(ret,offset,fetchNum); // consume完能保证consumeOffset == offset
        fetchStartIndex += consumeNum;
        q.consumeOffset += consumeNum;
//        log.error(String.format("(%s,%d): prefetchNum = %d, fetchStartIndex = %d, consumeOffset = %d, prefetchOffset = %d", topic, queueId,prefetchNum, fetchStartIndex,q.consumeOffset, q.prefetchOffset));

         //分类
        if (q.type == MQQueue.UNKNOWN){
            if (offset == 0){
                q.type = MQQueue.COLD; // cold
                if (mqConfig.useStats){
                    testStat.incColdQueueCount();
                }
                q.maxReaderMsgNum += 4;
            } else {
                q.type = MQQueue.HOT;
                if (mqConfig.useStats){
                    testStat.incHotQueueCount();
                }
//                q.block.freeSpace();
            }
        }

        if(mqConfig.useStats){
            testStat.incFetchMsgCount(fetchNum);
            if (q.type == MQQueue.HOT){
                // hot
                testStat.incHotFetchMsgCount(fetchNum);
            } else if (q.type == MQQueue.COLD){
                // cold
                testStat.incColdFetchMsgCount(fetchNum);

            }
        }

        if (mqConfig.useStats){
            testStat.incReadSSDCount(fetchNum-fetchStartIndex);
            if (q.type == MQQueue.HOT){
                // hot
                testStat.incHotReadSSDCount(fetchNum-fetchStartIndex);
            } else if (q.type == MQQueue.COLD){
                // cold
                testStat.incColdReadSSDCount(fetchNum-fetchStartIndex);
            }

        }

        q.consumeOffset = offset + fetchNum ; // 下一个被消费的位置
//        q.prefetchOffset = offset + fetchNum;

        // getRange 结束后应该要用一个异步任务补一些数据到预取队列中
        // 期望读SSD的时间可以覆盖掉异步任务的时间
        // TODO: new add v1
        if (q.type == MQQueue.COLD){
//            log.error(String.format("consumeNum=%d, consumeOffset=%d, prefetchOffset=%d",
//                    consumeNum,q.consumeOffset,q.prefetchOffset));
//            q.prefetch1();
        }

        DataFile df = mqTopic.df;

        long pos = 0;
        for (int i = fetchStartIndex; i < fetchNum; i++){
            long curOffset = offset + i;
            int intCurOffset = (int)curOffset;
            pos = q.offset2position.get(intCurOffset);
            ByteBuffer buf = df.read(pos);
            buf.flip(); // ?
            if (buf != null){
                ret.put(i, buf);
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
                prefetchThread = Executors.newFixedThreadPool(4);
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

        public ByteBuffer read(long position) {
            if (threadLocalReadMetaBuf.get() == null) {
                threadLocalReadMetaBuf.set(ByteBuffer.allocate(globalMetadataLength));
            }
            ByteBuffer readMeta = threadLocalReadMetaBuf.get();

            readMeta.clear();
            try {
                int ret = dataFileChannel.read(readMeta, position);
//                readMeta.position(6);
                readMeta.flip();
                short t = readMeta.getShort();int tt = readMeta.getInt();
                short dataLength = readMeta.getShort();
                if(dataLength < 0)log.error("data length < 0");
                ByteBuffer tmp = ByteBuffer.allocate(dataLength);
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
                hotQueueCount = 0;fetchCount = 0;
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
