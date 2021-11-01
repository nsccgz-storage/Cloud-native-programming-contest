package io.openmessaging;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import io.openmessaging.PMwrite.PMBlock;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntUnaryOperator;

import java.util.concurrent.Future;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Callable;

public class LSMessageQueue extends MessageQueue {
    public static final Logger log = Logger.getLogger(LSMessageQueue.class);

    public class MQConfig {
        Level logLevel = Level.INFO;
        boolean useStats = false;
        int writeMethod = 12;
        int numOfDataFiles = 4;
        int maxBufNum = 11;
        int maxBufLength = 128*1024;
        public String toString() {
            return String.format("useStats=%b | writeMethod=%d | numOfDataFiles=%d | maxBufLength=%d | maxBufNum=%d | ",useStats,writeMethod,numOfDataFiles,maxBufLength,maxBufNum);
        }
    }

    public void pmWriteShutdown(){
        // 把目前的buffer全部写到PM到，并且结束双写
        for (int i = 0; i < numOfDataFiles; i++){
            MyByteBuffer bf =  dataFiles[i].commonWriteBuffer;
            for(int k=0; k<bf.bufferNum; ++k){
                if (bf.blocks[k] != null) {
                    bf.isFinished = true;
                    log.info("shutdown the double write for thread " + i);
                    // 确认刷PM任务完成
                    if (bf.backgroundDoubleWriteFuture != null) {
                        while (bf.backgroundDoubleWriteFuture.isDone() != true) {
                            try {
                                Thread.sleep(1);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                        bf.backgroundDoubleWriteFuture = null;
                    }
                    // 触发刷盘任务，异步调用block的刷盘函数
                    final PMBlock backgroundBlock = bf.blocks[k];
                    if (backgroundBlock != null && bf.curPositions[bf.curBufIndex] != 0) {
                        pmWrite.copyMemoryNT(bf.bufferAddr[bf.curBufIndex], backgroundBlock.addr, backgroundBlock.capacity);
//                        pmWrite.pool.copyFromByteArrayNT(bf.commByteBuffers[bf.curBufIndex].array(), 0, backgroundBlock.addr, backgroundBlock.capacity);
                    }
                    bf.blocks[k] = null;
                }

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
        public int type;
        public MyByteBufferPool bbPool;
        public MyDirectBufferPool dbPool;

        MQQueue(DataFile dataFile){
            type = 0;
            maxOffset = 0L;
            offset2position = new ArrayList<>(256);
            offset2PMAddr = new ArrayList<>(256);
            offset2Length = new ArrayList<>(256);
            offset2DramAddr = new ArrayList<>(256);
            df = dataFile;
        }
        MQQueue(){
            type = 0;
            maxOffset = 0L;
            offset2position = new ArrayList<>(256);
            offset2PMAddr = new ArrayList<>(256);
            offset2Length = new ArrayList<>(256);
            offset2DramAddr = new ArrayList<>(256);
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
    ThreadLocal<MyByteBufferPool> threadLocalByteBufferPool;
    ThreadLocal<MyDirectBufferPool> threadLocalDirectBufferPool;
    public ThreadLocal<Semaphore> threadLocalSemaphore;
    public ThreadLocal<ByteBuffer> threadLocalWriterBuffer;
    boolean isCrash;
    public PMDoubleWrite pmDoubleWrite;

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
        log.info("init ok");
        if(!isCrash){
            // 超时自动退出
            new Timer("timer").schedule(new TimerTask() {
                @Override
                public void run() {
                    log.info(Thread.currentThread().getName() + " Exit !");
                    System.exit(-1);
                }
            }, 585000);
        }
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

            pmWrite = new PMwrite(pmDataFile);
            topic2object = new ConcurrentHashMap<String, MQTopic>();
//            log.info("Initializing on PM : " + pmDataFile);

            // pmRingBuffer = new PMPrefetchBuffer(pmDataFile);
            // pmDoubleWrite = new PMDoubleWrite(pmDataFile);

            numOfDataFiles = mqConfig.numOfDataFiles;
            dataFiles = new DataFile[numOfDataFiles];
            for (int i = 0; i < numOfDataFiles; i++) {
                String dataFileName = dbDirPath + "/db" + i;
//                log.info("Initializing datafile: " + dataFileName);
                dataFiles[i] = new DataFile(dataFileName);
            }

//            log.info("Initializing metadata file");
            metadataFileChannel = new RandomAccessFile(metadataFile, "rw").getChannel();
            localThreadId = new ThreadLocal<>();
            numOfThreads = new AtomicInteger();
            numOfThreads.set(0);
            numOfTopics = new AtomicInteger();
            numOfTopics.set(1);
            threadLocalByteBufferPool = new ThreadLocal<>();
            threadLocalDirectBufferPool = new ThreadLocal<>();
            threadLocalSemaphore = new ThreadLocal<>();
            threadLocalWriterBuffer = new ThreadLocal<>();

            if (!isCrash){
                DRAMbufferList = new MyDRAMbuffer[42];
                for(int i=0; i<42; i++){
                    DRAMbufferList[i] = new MyDRAMbuffer();
                }
            }
//            DRAMbufferList = new MyDRAMbuffer[42];
//            for(int i=0; i<42; i++){
//                DRAMbufferList[i] = new MyDRAMbuffer();
//            }

            if (mqConfig.useStats) {
                testStat = new TestStat(dataFiles);
            }
            if (crash) {
                log.info("recover !!");
//                System.exit(-1);
                recover();
            }
            localDramBuffer = new ThreadLocal<>();
        } catch (IOException ie) {
            ie.printStackTrace();
        }

        System.gc();
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
//                    log.debug("ret : " + ret);
//                    log.debug(strBuffer);
                    strBuffer.flip();
//                    log.debug(strBuffer);
                    int strLength = strBuffer.getInt();
//                    log.debug("strLength :"+strLength);
                    byte[] strBytes = new byte[strLength];
                    strBuffer.get(strBytes);
                    String topic = new String(strBytes);
                    id2topic.put(topicId, topic);
                    topicId += 1;
//                    log.debug("recover topic : "+topic);
                    strBuffer.clear();
                }
            }

            // topicId -> topic
            ByteBuffer bufMetadata = ByteBuffer.allocateDirect(8);
            ByteBuffer msgMetadata = ByteBuffer.allocateDirect(8);
            for (int i = 0; i < numOfDataFiles; i++){
                long curPosition = 0L;
                FileChannel fc = dataFiles[i].dataFileChannel;
                while ((fc.read(bufMetadata, curPosition)) != -1){
                    bufMetadata.flip();
                    int bufLength = bufMetadata.getInt();
                    if (bufLength == 0){
                        break;
                    }
                    int bufNum = bufMetadata.getInt();
//                    log.debug("bufLength : "+bufLength);
//                    log.debug("bufNum : "+bufNum);
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
//        log.debug("replay append : " + topic + "," + queueId + "," + position);
        MQTopic mqTopic;
        MQQueue q;

        mqTopic = topic2object.get(topic);
        if (mqTopic == null) {
            mqTopic = new MQTopic(topicId, topic, dataFiles[dataFileId]);
            topic2object.put(topic, mqTopic);
        }

        q = mqTopic.id2queue.get(queueId);
        if (q == null){
            q = new MQQueue();
            mqTopic.id2queue.put(queueId, q);
        }

        q.offset2position.add(position);
        q.offset2Length.add(dataLength);
        long ret = q.maxOffset;
        q.maxOffset++;
//        log.debug("replay ok");
        return ret;
    }

    public long append2(String topic, int queueId, ByteBuffer data){
        // 放数据
        data.mark();

        ByteBuffer writeDramData = data.duplicate();
        ByteBuffer doubleWriteData = data.duplicate();
//        log.debug("append : "+topic+","+queueId + data);
        if (mqConfig.useStats){
            testStat.appendStart();
            testStat.appendUpdateStat(topic, queueId, data);
        }
        MQTopic mqTopic;
        // TODO: maybe useless
        if (threadLocalSemaphore.get() == null){
            threadLocalSemaphore.set(new Semaphore(0));
        }

        mqTopic = topic2object.get(topic);
        if (mqTopic == null) {
            int threadId = updateThreadId();
            int dataFileId = threadId % numOfDataFiles; 
            short topicId = getAndUpdateTopicId(topic);
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
        MQQueue q = mqTopic.id2queue.get(queueId);
        if (q == null){
            q = new MQQueue(mqTopic.df); // 要和topic用一样的df
            q.bbPool = threadLocalByteBufferPool.get();
            q.dbPool = threadLocalDirectBufferPool.get();

            mqTopic.id2queue.put(queueId, q);
            if (mqConfig.useStats){
                testStat.incQueueCount();
            }
        }
//        log.debug("append : "+topic+","+queueId+","+data.remaining()+" maxOffset :"+q.maxOffset);


        int dataLength = writeDramData.remaining();
        q.offset2Length.add(dataLength);

        if(q.type == 1){ // 热队列
            if(localDramBuffer.get() == null){
                int threadId = mqTopic.threadId;
                localDramBuffer.set(DRAMbufferList[threadId]);
            }

            MyDRAMbuffer draMbuffer = localDramBuffer.get();
            int addr = draMbuffer.put(writeDramData);   
            if(addr == -1){
                if(mqConfig.useStats) {
                    testStat.incDramFullCount();
                    testStat.dramBufferUsedReport(draMbuffer.toString());
                }
            }
            q.offset2DramAddr.add(addr);
        }else{
            q.offset2DramAddr.add(-1);
        }
        // TODO: 看看有没有完成，如果没有完成就 1)等待完成 2）自己主动尝试获取锁去完成
        try {
            log.debug("wait to acquire the sema");

            if (!w.sema.tryAcquire(1, 500*1000, TimeUnit.MICROSECONDS)){
                // 我插入的writer可能要等待下一个能获取锁的写入线程帮我写入
                // 如果已经没有新的线程需要写入了，这个时候这个线程就会无限等待，此时需要有一个超时自救的机制
                if (w.done != 1){
//                    log.debug("time out !");
                    df.syncSeqWriteBatchLock();
//                    log.debug("my position result : " + w.position);
                }
                w.sema.acquire();
            }
        } catch (Exception ie){
            ie.printStackTrace();
        }

        long ret = q.maxOffset;
        q.maxOffset++;

//        log.debug("add position " + w.position);
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
//        log.debug("getRange : "+topic+","+queueId+","+offset+","+fetchNum+" maxOffset: "+(q.maxOffset-1));
        // 更新一下offset和fetchNum，略去那些肯定没有的
        if (offset >= q.maxOffset){
            return ret;
        }
        if (offset + fetchNum-1 >= q.maxOffset){
            fetchNum = (int)(q.maxOffset-offset);
        }

        int fetchStartIndex = 0;
        // 尝试读双写的内容
//        log.info(String.format("type: %d, max offset:%d, pmem cache size: %d", q.type, q.maxOffset, q.offset2PMAddr.size()));
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
            if(mqConfig.useStats && intDoubleWriteNum > 0) {
                testStat.incHitPmemCount(intDoubleWriteNum);
            }
            for (int i = 0; i < intDoubleWriteNum; i++){
                int curOffset = (int)offset+i;
//                log.debug("curOffset : " + curOffset);
                int dataLength = q.offset2Length.get(curOffset);
//                log.debug("get from double buffer datLength " + dataLength);
                // TODO: 需要修复
                ByteBuffer buf = q.bbPool.allocate(dataLength);
                // ByteBuffer buf = ByteBuffer.allocate(dataLength);
                long readPMAddr = q.offset2PMAddr.get(curOffset) + 2 * Short.BYTES + Integer.BYTES;
                pmWrite.unsafeCopyToByteArray(readPMAddr, buf.array(), buf.position(), dataLength);
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
                    if(mqConfig.useStats) testStat.incHitHotReadCount();
                }else{
                    long pos = q.offset2position.get(curOffset);
//                    log.debug("read position : " + pos);
                    ByteBuffer buf = df.readData(pos,dataLength);
                    if (buf != null){
                        ret.put(i, buf);
                        if(mqConfig.useStats) testStat.incMissHotReadCount();
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
//            log.debug("read position : " + pos);
            ByteBuffer buf = df.readData(pos,dataLength);
            if (buf != null){
                ret.put(i, buf);
            }
        }
        if (q.maxOffset - (offset + fetchNum) <= 4 && q.type == 2){
            q.type = 1; // 修改冷队列为热队列，阈值设置暂定为4
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
//            log.info("init thread id : " + thisNumOfThread);
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
            ByteBuffer buf = ByteBuffer.allocateDirect(128);
            buf.putInt(topic.length());
            buf.put(topic.getBytes());
            buf.position(0);
//            log.debug(buf);
            metadataFileChannel.write(buf, (topicId-1)*128);
            metadataFileChannel.force(true);
//            log.info("get topic id : " + topicId );
        } catch (IOException ie){
            ie.printStackTrace();
        }
        return (short)topicId;
    }

    public class MyByteBufferPool {
        int capacity;
        byte[] buffer;
        AtomicInteger atomicHead;
        int slotSize;
        int maxLength;
        IntUnaryOperator getNext; // 一个操作方法
        MyByteBufferPool(){
            atomicHead = new AtomicInteger();
            atomicHead.set(0);
            slotSize = 17*1024;
            maxLength = 500;
            capacity = maxLength * slotSize;  // 分配 17 * 1024 * 500 个？
            buffer = new byte[capacity];
            getNext = (int curHead) -> {
                return (curHead + 1) %maxLength;
//                int nextHead = curHead+1;
//                nextHead = nextHead % maxLength;
//                return nextHead;
            };
        }
        public  ByteBuffer allocate(int dataLength){
            int thisHead = atomicHead.getAndUpdate(getNext);
            ByteBuffer ret = ByteBuffer.wrap(buffer, thisHead*slotSize, dataLength);
            ret.mark(); // TODO: 为什么 mark?
            return ret;
        }
    }
    public class MyDirectBufferPool {
        int capacity;
        ByteBuffer directBuffer;
        AtomicInteger atomicHead;
        int slotSize;
        int maxLength;
        IntUnaryOperator getNext;
        MyDirectBufferPool(){
            atomicHead = new AtomicInteger();
            atomicHead.set(0);
            slotSize = 17*1024;
            maxLength = 500;
            capacity = maxLength * slotSize;
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
        long[] bufferAddr;
        int minBufLen;
        int capacity;
        int curPositions[];

        PMBlock[] blocks;
        boolean isFinished;
        Future<Integer> backgroundDoubleWriteFuture;
        int bufferNum;
        public MyByteBuffer(int writerQueueBufferCapacity){
            curBufIndex = 0;
            bufferNum = 2;

            commByteBuffers = new ByteBuffer[bufferNum];
            bufferAddr = new long[2];
            capacity = 4 * (1 << 20); // 4*1024*1024
            for(int i=0; i<bufferNum; i++){
                commByteBuffers[i] = ByteBuffer.allocateDirect(capacity);
                bufferAddr[i] = ((DirectBuffer)commByteBuffers[i]).address();
            }
            minBufLen = mqConfig.maxBufLength;
            curPositions = new int[bufferNum];

            for(int i=0; i<bufferNum; i++){
                curPositions[i] = 0;
            }
            // curPositions[2] = 0;
            blocks = new PMBlock[bufferNum];
            for(int i=0; i<bufferNum; i++){
                blocks[i] = pmWrite.pmBlockPool.allocate();
            }
            isFinished = false;
        }
        boolean writePmem(int curIdx){
            if(isFinished) return false;
            if(blocks[curIdx] == null){
//                log.info("the pm is full");
                return false;
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
            final long backgroundAddr = blocks[curIdx].addr;
            final int backgroundCapacity = blocks[curIdx].capacity;

            final long flushAddr = bufferAddr[curBufIndex];
            backgroundDoubleWriteFuture = pmWrite.backgroundDoubleWriteThread.submit(new Callable<Integer>(){
                @Override
                public Integer call() throws Exception {
                    pmWrite.copyMemoryNT(flushAddr, backgroundAddr, backgroundCapacity);
//                    pmWrite.pool.copyFromByteArrayNT(flushBuf.array(), 0, backgroundAddr , backgroundCapacity);
                    // log.info("this is no error!!!!!!");
                    return 0;
                }
            });
            blocks[curIdx] = pmWrite.pmBlockPool.allocate();
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

                if(!isCrash){
                    long totalSize = 90L*1024L*1024L*1024L;
//                    log.info("place 0 in file" + dataFileName);
                    ByteBuffer tempBuf = ByteBuffer.allocateDirect(4*1024);
                    dataFileChannel.write(tempBuf, totalSize);
                    dataFileChannel.force(true);
                }

                writerQueueBufferCapacity = 4*1024*1024; // 不能大于一个 block 的大小
                // commonWriteBuffer = ByteBuffer.allocate(writerQueueBufferCapacity);
                commonWriteBuffer = new MyByteBuffer(writerQueueBufferCapacity);
                //commonWriteBuffer.clear();

                writerConcurrentQueue = new ConcurrentLinkedQueue<>();

                globalMetadataLength = Short.BYTES + Integer.BYTES + Short.BYTES; // 8 byte
                bufMetadataLength = Integer.BYTES + Integer.BYTES;
                writeStat = new WriteStat();
//                log.debug("init data file : " + dataFileName + " ok !");

                threadLocalReadMetaBuf = new ThreadLocal<>();

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
            if (dataFileLock.tryLock()){
//                log.debug("try to get the lock and success !");
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
//                log.debug("I get the lock!");
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
                commonWriteBuffer.writePmem(commonWriteBuffer.curBufIndex);
                commonWriteBuffer.changeBuf();
            }

            // long blockAddr = block.addr; // 同一个 dataFile

            ByteBuffer writerBuffer = commonWriteBuffer.duplicate(0); // 0 for seqWrite
            int bufferStartAddr = commonWriteBuffer.getAddr();
            PMBlock block = commonWriteBuffer.blocks[commonWriteBuffer.curBufIndex];

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
//                    log.debug("writer the index : " + i);
                    needWrite = true;
                    writeLength = globalMetadataLength + thisWriter.length;
                    if (bufLength + writeLength >= maxBufLength){
                        if (mqConfig.useStats){
                            writeStat.incExceedBufLengthCount();
                        }
                        break;
                    }
                    thisWriter.position = position;
                    thisWriter.needWrite = 1;
//                    log.debug("save position : " + position);
                    position += writeLength;
//                    log.debug("update position to : " + position);
                    bufLength += writeLength;
                    bufNum += 1;

                    thisWriter.pmAddr =  block == null ? -1L :  block.addr + writerBuffer.position() + bufferStartAddr;
//                    if(block != null){
//                        log.info("block is not null");
//                        if(block.addr + writerBuffer.position() + bufferStartAddr != -1){
//                            log.info("pm addr != -1");
//                        }
//                    }

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
                }
            }
            if (!needWrite){
                return ;
            }
            // 对齐 4K
            commonWriteBuffer.updataCurPosition(0, bufLength); //  更新

            bufLength = bufLength + (4096 - bufLength % 4096);
            writerBuffer.flip();
            writerBuffer.putInt(bufLength);
            writerBuffer.putInt(bufNum);
            writerBuffer.position(0);
            try {
                // TODO: 给每个DataFile分配一小块direct buffer
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
                    thisWriter.sema.release(1);
                }
            }

            curPosition += bufLength;
            if (mqConfig.useStats){
                writeStat.addSample(bufLength);
            }
            log.debug("df.curPosition : " + curPosition);
        }

        public ThreadLocal<ByteBuffer> threadLocalReadMetaBuf;

        public ByteBuffer readData(long position, int dataLength) {
            MyDirectBufferPool dbPool = threadLocalDirectBufferPool.get();
            try {
                ByteBuffer tmp;
                if (dbPool != null){
                    tmp = dbPool.allocate(dataLength);
                } else {
                    tmp = ByteBuffer.allocate(dataLength);
                }
                tmp.mark();
                dataFileChannel.read(tmp, position + globalMetadataLength);
                tmp.reset();
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

                bucketCount = new int[bucketBound.length-1];
                Arrays.fill(bucketCount, 0);
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
                StringBuilder ret = new StringBuilder();
                ret.append(bucketBound[0]).append(" < ");
                for (int i = 0; i < bucketCount.length; i++){
                    ret.append("[").append(bucketCount[i]).append("]");
                    ret.append(" < ").append(bucketBound[i + 1]).append(" < ");
                }
                return ret.toString();
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

            int hitPmemCount;

            String dramBufferUsedInfo = "";

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
                
                hitPmemCount = 0;

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

                ret.hitPmemCount = this.hitPmemCount;
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

        void incHitPmemCount(int v){
            int id = threadId.get();
            stats[id].hitPmemCount += v;
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
            /*
            StringBuilder totalAppendStat = new StringBuilder();
            StringBuilder totalGetRangeStat = new StringBuilder();
            StringBuilder appendStat = new StringBuilder();
            StringBuilder getRangeStat = new StringBuilder();
            for (int i = 0; i < getNumOfThreads; i++){
                appendStat.append(String.format("%d,", curAppendCount[i]));
                getRangeStat.append(String.format("%d,", curGetRangeCount[i]));
                totalAppendStat.append(String.format("%d,", stats[i].appendCount));
                totalGetRangeStat.append(String.format("%d,", stats[i].getRangeCount));
            }*/
            String csvStat = String.format("%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,XXXX,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f",
                    writeBandwidth, elapsedTimeS, appendThroughput, appendLatency, getRangeThroughput, getRangeLatency,
                    curWriteBandwidth, thisElapsedTimeS, curAppendThroughput, curAppendLatency, curGetRangeThroughput,
                    curGetRangeLatency);

            log.info("csvStat      :"+csvStat);
            /*log.info("appendStat   :"+appendStat);
            log.info("getRangeStat :"+getRangeStat);
            log.info("total appendStat   :"+ totalAppendStat);
            log.info("total getRangeStat :"+totalGetRangeStat);*/

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
            StringBuilder totalDramReadReport = new StringBuilder();
            StringBuffer dramBufferUesdReport = new StringBuffer();
            int hitHotCount = 0, missHotCount = 0, dramBufferFullCount = 0;
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

                hitHotCount += stats[i].hitHotCount;
                missHotCount += stats[i].missHotCount;
                dramBufferFullCount += stats[i].dramBufferFullCount;
            }
            totalDramReadReport.append(String.format("Hot hit DRAM: %d, Hot miss DRAM: %d, full DRAM count: %d | ", hitHotCount, missHotCount, dramBufferFullCount ));

            log.info("[hit hot data counter] : " + hotDataHitCountReport);
            log.info("[hit hot data] : " + hotDataReport);
            log.info("[fetch Msg Count ] : "+fetchCountReport);
            log.info("[read SSD Count] : "+readSSDCountReport);
            //log.info("[HOT fetch Msg Count ] : "+hotFetchCountReport);
            //log.info("[HOT read SSD Count] : "+hotReadSSDCountReport);
            //log.info("[COLD fetch Msg Count ] : "+coldFetchCountReport);
            //log.info("[COLD read SSD Count] : "+coldReadSSDCountReport);
            log.info("[READ DRAM buffer info] : " + dramReadReport);
            log.info("[total dram buffer info] :" + totalDramReadReport);
            log.info("[DRAM buffer used info] : " + dramBufferUesdReport);

            int totalHitPmemCount = 0;
            StringBuilder hitPmemCountReport = new StringBuilder();
            for(int i = 0;i < getNumOfThreads;i++){
                hitPmemCountReport.append(String.format("%d,",(stats[i].hitPmemCount)));
                totalHitPmemCount += stats[i].hitPmemCount;
            }
            log.info("[Pmem hit count] : total= "+totalHitPmemCount+"; "+hitPmemCountReport);

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

            /*
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
            */
//            log.info("[pmem free list for this thread] "+pmDoubleWrite.getFreeListString());

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
