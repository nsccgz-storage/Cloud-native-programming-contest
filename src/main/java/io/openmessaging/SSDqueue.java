package io.openmessaging;

import java.io.IOException;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

public class SSDqueue{
    private static final Logger logger = Logger.getLogger(SSDqueue.class);

    AtomicLong FREE_OFFSET = new AtomicLong();
    AtomicLong META_FREE_OFFSET = new AtomicLong();
    AtomicInteger currentNum = new AtomicInteger();

    int QUEUE_NUM = 10000;
    int TOPIC_NUM = 100;
    
    int TOPIC_NAME_SZIE = 128;
    Long topicArrayOffset; // 常量

    ConcurrentHashMap<String, Long> topicNameQueueMetaMap;
    ConcurrentHashMap<String, Map<Integer, Long>> queueTopicMap;

    FileChannel fileChannel;
    FileChannel metaFileChannel;

    ConcurrentHashMap<String, long[]> lastAccessOffset;

    TestStat testStat;

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
            Long writeBytes;
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

        //DataFile[] myDataFiles;
        //DataFile.WriteStat[] oldWriteStats;

        // ThreadLocal< HashMap<Integer, Long> >
        // report operation per second
        //TestStat(DataFile[] dataFiles) {
        TestStat(){
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
            //myDataFiles = dataFiles;
            //oldWriteStats = new DataFile.WriteStat[myDataFiles.length];
        }

        void updateThreadId() {
            if (threadId.get() == null) {
                int thisNumOfThread = numOfThreads.getAndAdd(1);
                threadId.set(thisNumOfThread);
                logger.info("init thread id : " + thisNumOfThread);
            }
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
//            stats[id].addSample(data.remaining());
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
            int[] totalWriteBucketCount = new int[100];
            int[] myBucketBound = stats[0].bucketBound;
            for (int i = 0; i < 100; i++){
                totalWriteBucketCount[i] = 0;
            }
            int numOfBucket = stats[0].bucketCount.length;
            for (int i = 0; i < getNumOfThreads; i++){
                for (int j = 0; j < numOfBucket; j++){
                    totalWriteBucketCount[j] += stats[i].bucketCount[j];
                }
            }

            StringBuilder totalWriteBucketReport = new StringBuilder();
            totalWriteBucketReport.append(myBucketBound[0]).append(" < ");
            for (int i = 0; i < numOfBucket; i++){
                totalWriteBucketReport.append("[").append(totalWriteBucketCount[i]).append("]");
                totalWriteBucketReport.append(" < ").append(myBucketBound[i + 1]).append(" < ");
            }
            logger.info("[Total Append Data Dist]" + totalWriteBucketReport);

            if (oldTotalWriteBucketCount != null) {
                int[] curWriteBucketCount = new int[100];
                for (int i = 0; i < numOfBucket; i++) {
                    curWriteBucketCount[i] = totalWriteBucketCount[i] - oldTotalWriteBucketCount[i];
                }
                StringBuilder curWriteBucketReport = new StringBuilder();
                curWriteBucketReport.append(myBucketBound[0]).append(" < ");
                for (int i = 0; i < numOfBucket; i++) {
                    curWriteBucketReport.append("[").append(curWriteBucketCount[i]).append("]");
                    curWriteBucketReport.append(" < ").append(myBucketBound[i + 1]).append(" < ");
                }

                logger.info("[Current Append Data Dist]" + curWriteBucketReport);

            }

            oldTotalWriteBucketCount = totalWriteBucketCount;

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

            StringBuilder appendStat = new StringBuilder();
            StringBuilder getRangeStat = new StringBuilder();
            for (int i = 0; i < getNumOfThreads; i++){
                appendStat.append(String.format("%d,", curAppendCount[i]));
                getRangeStat.append(String.format("%d,", curGetRangeCount[i]));
            }
            String csvStat = String.format("%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,XXXX,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f",
                    writeBandwidth, elapsedTimeS, appendThroughput, appendLatency, getRangeThroughput, getRangeLatency,
                    curWriteBandwidth, thisElapsedTimeS, curAppendThroughput, curAppendLatency, curGetRangeThroughput,
                    curGetRangeLatency);

            logger.info("appendStat   :"+appendStat);
            logger.info("getRangeStat :"+getRangeStat);
            logger.info("csvStat      :"+csvStat);
            logger.info("Memory Used (GiB) : "+memoryUsage.getUsed()/(double)(1024*1024*1024));

            // report write stat
            // for (int i = 0; i < dataFiles.length; i++){
            //     if (oldWriteStats[i] != null){
            //         // get total write stat and cur write stat

            //         DataFile.WriteStat curWriteStat = dataFiles[i].writeStat;
            //         DataFile.WriteStat oldWriteStat = oldWriteStats[i];
            //         String writeReport = "";
            //         writeReport += "[Total ] File " + i;
            //         writeReport += " " + "emptyQueueCount : " + curWriteStat.emptyQueueCount;
            //         writeReport += " " + "exceedBufNumCount : " + curWriteStat.exceedBufNumCount;
            //         writeReport += " " + "exceedBufLengthCount : " + curWriteStat.exceedBufLengthCount;
            //         log.info(writeReport);
            //         log.info("Write Size Dist : "+curWriteStat.toString());

            //         // current

            //         oldWriteStat.emptyQueueCount = curWriteStat.emptyQueueCount - oldWriteStat.emptyQueueCount;
            //         oldWriteStat.exceedBufLengthCount = curWriteStat.exceedBufLengthCount - oldWriteStat.exceedBufLengthCount;
            //         oldWriteStat.exceedBufNumCount = curWriteStat.exceedBufNumCount - oldWriteStat.exceedBufNumCount;
            //         for (int j = 0; j < oldWriteStat.bucketCount.length; j++){
            //             oldWriteStat.bucketCount[j] = curWriteStat.bucketCount[j] - oldWriteStat.bucketCount[j];
            //         }

            //         curWriteStat = oldWriteStat;
            //         writeReport = "";
            //         writeReport += "[Current ] File " + i;
            //         writeReport += " " + "emptyQueueCount : " + curWriteStat.emptyQueueCount;
            //         writeReport += " " + "exceedBufNumCount : " + curWriteStat.exceedBufNumCount;
            //         writeReport += " " + "exceedBufLengthCount : " + curWriteStat.exceedBufLengthCount;
            //         log.info(writeReport);
            //         log.info("Write Size Dist : "+curWriteStat.toString());


            //     }
            //    oldWriteStats[i] = dataFiles[i].writeStat.clone();
            // }

            logger.info(writeBandwidth+","+elapsedTimeS+","+appendThroughput+","+appendLatency+","+getRangeThroughput+","+getRangeLatency+",XXXXXX,"+curWriteBandwidth+","+thisElapsedTimeS+","+curAppendThroughput+","+curAppendLatency+","+curGetRangeThroughput+","+curGetRangeLatency);

            // deep copy
            oldStats = stats.clone();
            for (int i = 0; i < 100; i++) {
                oldStats[i] = stats[i].clone();
            }
            oldEndTime = endTime;
        }

        // report topic stat per second
    }

    public ThreadLocal<ByteBuffer> writerQueueLocalBuffer;
    final int writerQueueBufferCapacity = 1024 * 256;
    public Lock lock; // 写队列的锁
    public Condition queueCondition;
    public Deque<Writer> writerQueue;

    private class Writer{
        ByteBuffer data;
        boolean done;
        long offset;
        Condition cv;
        Data block;
        Writer(ByteBuffer d, Condition c, Data b){
            data = d;
            done = false;
            cv = c;
            offset = 0L;
            block = b;
        }
    }

    public SSDqueue(FileChannel fileChannel, FileChannel metaFileChannel){
        testStat = new TestStat();

        this.fileChannel = fileChannel;
        this.metaFileChannel = metaFileChannel;

        // 划分起始的 Long.BYTES * 来存元数据
        currentNum.set(0);
        META_FREE_OFFSET.set(TOPIC_NUM * (TOPIC_NAME_SZIE + Long.BYTES) + Integer.BYTES);
        this.topicArrayOffset = 0L + Integer.BYTES;

        this.topicNameQueueMetaMap = new ConcurrentHashMap<>();
        this.queueTopicMap = new ConcurrentHashMap<>();
        FREE_OFFSET.set(0L);


        writerQueueLocalBuffer = new ThreadLocal<>();
        lock = new ReentrantLock(false); // false 非公平锁
        queueCondition = lock.newCondition();
        writerQueue = new ArrayDeque<>();

        lastAccessOffset = new ConcurrentHashMap<>();
    }

    public SSDqueue(FileChannel fileChannel, FileChannel metaFileChannel, Boolean t)throws IOException{
        testStat = new TestStat();

        // 读盘，建表 
        // TODO, recover
        this.fileChannel = fileChannel;
        this.metaFileChannel = metaFileChannel;
        this.topicNameQueueMetaMap = new ConcurrentHashMap<>();
        this.queueTopicMap = new ConcurrentHashMap<>();
        this.topicArrayOffset = 0L + Integer.BYTES;

        ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES+TOPIC_NAME_SZIE);
        metaFileChannel.read(tmp);
        tmp.flip();

        this.currentNum.set(tmp.getInt());

        Long startOffset = 0L + Integer.BYTES;
        
        for(int i=0; i<currentNum.get(); i++){

            Long offset = startOffset + i*(Long.BYTES + TOPIC_NAME_SZIE);
            tmp.clear();
//            tmp = ByteBuffer.allocate(Long.BYTES);
            int len = metaFileChannel.read(tmp, offset);
            tmp.flip();

            Long queueMetaOffset = tmp.getLong();

            tmp.clear();
//            tmp = ByteBuffer.allocate(TOPIC_NAME_SZIE);
            len = metaFileChannel.read(tmp,offset + Long.BYTES);
            tmp.flip();

            String topicName = new String(tmp.array()).trim();

            topicNameQueueMetaMap.put(topicName, queueMetaOffset);
            
            // 遍历每个 topic 下的 queue
            queueTopicMap.put(topicName, readQueue(queueMetaOffset));

        }

        logger.info("recover a SSDqueue, num: "+currentNum.get());

        writerQueueLocalBuffer = new ThreadLocal<>();
        lock = new ReentrantLock(false); // false 非公平锁
        queueCondition = lock.newCondition();
        writerQueue = new ArrayDeque<>();

        lastAccessOffset = new ConcurrentHashMap<>();
    }

    public Map<Integer, Long> readQueue(Long queueMetaOffset) throws IOException{
        QueueId resData = new QueueId(metaFileChannel, queueMetaOffset);
        return resData.readAll();

    }

    public Long setTopic(String topicName, int queueId, ByteBuffer data){
        testStat.appendStart();

        Long result;
        try{

            Map<Integer, Long> topicData = queueTopicMap.get(topicName);
            if(topicData == null){
                // 自下而上
                Data writeData = new Data(fileChannel);
//                Long res = writeData.put(data);
                result = writeData.put(data);

                QueueId queueArray =  new QueueId(metaFileChannel, QUEUE_NUM);
                Long queueOffset = queueArray.put(queueId, writeData.getMetaOffset());

                // 
                ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES + TOPIC_NAME_SZIE); // offset : name
                tmp.putLong(queueArray.getMetaOffset());
                tmp.put(topicName.getBytes(), 0, topicName.length());
                tmp.flip();
                // 
                int cur = currentNum.getAndIncrement();
                metaFileChannel.write(tmp, this.topicArrayOffset + (cur) * (TOPIC_NAME_SZIE + Long.BYTES));
//                metaFileChannel.force(true);
                
                tmp.clear();
                // tmp = ByteBuffer.allocate(Integer.BYTES);
                tmp.putInt(currentNum.get());
                tmp.flip();
                int len = metaFileChannel.write(tmp, 0L);
//                metaFileChannel.force(true);

                //System.out.println("110: " + len);
                //logger.info("num: "+ cur + " metaQueue: "+ queueArray.getMetaOffset());
                // 更新 DRAM map
                topicData = new ConcurrentHashMap<>(); // TODO: check if HashMap is ok
                topicData.put(queueId, writeData.getMetaOffset());
                topicNameQueueMetaMap.put(topicName, queueArray.getMetaOffset());
                queueTopicMap.put(topicName, topicData);
            }else{
                Long metaDataOffset = topicData.get(queueId);
                if(metaDataOffset == null){
                    // 增加 queueIdArray
                    // 自下而上
                    Data writeData = new Data(fileChannel);
//                    Long res = writeData.put(data);
                    result = writeData.put(data);
                    Long queueMetaOffset = topicNameQueueMetaMap.get(topicName);
                    QueueId queueArray =  new QueueId(metaFileChannel, queueMetaOffset); // 写入 SSD
                    Long queueOffset = queueArray.put(queueId, writeData.getMetaOffset());

                    // 插入 DRAM 哈希表
                    topicData.put(queueId, writeData.getMetaOffset());
                    queueTopicMap.put(topicName, topicData);
                }else{
                    Data writeData = new Data(fileChannel, metaDataOffset);
                    result = writeData.put(data);
                }
            }
            metaFileChannel.force(true);
        } catch (Exception e) {
            //TODO: handle exception
            e.printStackTrace();
            return null;
        }
        testStat.appendUpdateStat(topicName, queueId, data);
        return result;
    }
    public Map<Integer, ByteBuffer> getRange(String topicName, int queueId, Long offset, int fetchNum){
        testStat.getRangeStart();
        Map<Integer, ByteBuffer> result = new HashMap<>();
        try{
            Map<Integer, Long> topicData = queueTopicMap.get(topicName);
            if(topicData == null) return result;
            Long metaDataOffset = topicData.get(queueId);
            if(metaDataOffset == null) return result;

            Data resData = new Data(fileChannel, metaDataOffset);

            result = resData.getRange(topicName+"& "+queueId, offset, fetchNum);
        }catch(IOException e){
            logger.error(e);
        }
        testStat.getRangeUpdateStat(topicName, queueId, offset, fetchNum);
        return result;
    }

    private class QueueId{
        FileChannel metaFileChannel;
        
        Long metaDataOffset;
        int currentNum;
        Long queueIdArray;
        
        public QueueId(FileChannel metaFileChannel, int totalNum)throws IOException{
            this.metaFileChannel = metaFileChannel;

            this.metaDataOffset = META_FREE_OFFSET.getAndAdd(totalNum * (Long.BYTES + Integer.BYTES) + Integer.BYTES);

            this.currentNum = 0;
            ByteBuffer tmp = ByteBuffer.allocate(Integer.BYTES);
            tmp.putInt(this.currentNum);
            tmp.flip();
            metaFileChannel.write(tmp, metaDataOffset);
//            metaFileChannel.force(true);// 在setTopic函数中有 metaFile的force，故省略此步骤

            this.queueIdArray = this.metaDataOffset + Integer.BYTES;
        }
        public QueueId(FileChannel metaFileChannel, Long metaDataOffset)throws IOException{
            this.metaFileChannel = metaFileChannel;
            this.metaDataOffset = metaDataOffset;
            this.queueIdArray = metaDataOffset + Integer.BYTES;

            // TODO: 恢复 currentNum, should atomic
            ByteBuffer tmp =  ByteBuffer.allocate(Integer.BYTES);
            metaFileChannel.read(tmp, this.metaDataOffset);
            tmp.flip();
            this.currentNum = tmp.getInt();
        }
        public Long put(int queueId, Long dataMetaOffset)throws IOException{
            Long offset = queueIdArray + currentNum * (Integer.BYTES + Long.BYTES);

            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + Long.BYTES);
            buffer.putInt(queueId);
            buffer.putLong(dataMetaOffset);
            buffer.flip();
            metaFileChannel.write(buffer, offset);
//            metaFileChannel.force(true);// 在setTopic函数中有 metaFile的force，故省略此步骤
            // TODO: 写回 SSD
            // 这个需要原子修改
            currentNum++;
            buffer.clear();
//            ByteBuffer tmp = ByteBuffer.allocate(Integer.BYTES);
            buffer.putInt(this.currentNum);
            buffer.flip();
            metaFileChannel.write(buffer, metaDataOffset);
//            metaFileChannel.force(true);// 在setTopic函数中有 metaFile的force，故省略此步骤
            this.queueIdArray = this.metaDataOffset + Integer.BYTES;
            return offset;
        }
        public Long getMetaOffset(){
            return this.metaDataOffset;
        }
        public Map<Integer, Long> readAll() throws IOException{
            Map<Integer, Long> res = new HashMap<>();
            ByteBuffer tmp = ByteBuffer.allocate(Integer.BYTES + Long.BYTES);
            //logger.info(toString());
            for(int i=0; i<this.currentNum; ++i){
                int len2 = metaFileChannel.read(tmp, this.queueIdArray + i*(Integer.BYTES + Long.BYTES));
                tmp.flip();
                res.put(tmp.getInt(), tmp.getLong());
                tmp.clear(); // 清空buffer为下一次写buffer作准备
            }
            return res;
        }
        public String toString(){
            return "num: " + this.currentNum + " meta:  " + this.metaDataOffset + "  array： " + this.queueIdArray;
        }
    }
    /*
    * 目前先考虑写入 SSD，而不考虑使用 DRAM 优化，所以先存所有的数据。
    *  <Length, nextOffset, Data>
    */
    private class Data{
        Long totalNum; // 存
        FileChannel fileChannel;
        Long tail; // 存
        Long head; // 存
        Long metaOffset;

        public Data(FileChannel fileChannel) throws IOException{
            this.metaOffset = FREE_OFFSET.getAndAdd(Long.BYTES * 3);
            this.fileChannel = fileChannel;

            this.totalNum = 0L;
            this.tail = -1L;
            this.head = this.tail;

            ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES * 3);
            tmp.putLong(totalNum);
            tmp.putLong(head);
            tmp.putLong(tail);
            tmp.flip();
            fileChannel.write(tmp, this.metaOffset);
//            fileChannel.force(true); // 能保证后面data.put时一定会有 fileChannel的force
        }

        public String toString(){
            return "nums: " + totalNum + " head: " + head + " tail: " + tail + " meta: " + metaOffset;
        }

        public Data(FileChannel fileChannel, Long metaOffset) throws IOException{
            this.fileChannel = fileChannel;
            this.metaOffset = metaOffset;

            // 恢复 totalNum, tail, head
            ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES + Long.BYTES + Long.BYTES);
            this.fileChannel.read(tmp, metaOffset);
            tmp.flip();

            this.totalNum = tmp.getLong();
            this.head = tmp.getLong();
            this.tail = tmp.getLong();
        }
        public long writeAgg(ByteBuffer data, Data dataBlock){
            if (writerQueueLocalBuffer.get() == null) {
                writerQueueLocalBuffer.set(ByteBuffer.allocateDirect(writerQueueBufferCapacity)); // 分配堆外内存
            }
            ByteBuffer writerBuffer = writerQueueLocalBuffer.get();

            long offset = -1;
            lock.lock();
            try {
                Writer w = new Writer(data, queueCondition, dataBlock);
                writerQueue.addLast(w);
                while (!w.done && !w.equals(writerQueue.getFirst())) {
                    w.cv.await();
                }
                if (w.done) {
                    return w.offset;
                }

                // 设置参数
                int maxBufNum = 12;
                int maxBufLength = 238 * 1024; // 90KiB + 17KiB < writerQueueBufferCapacity = 128KiB

                // 执行批量写操作
                int bufLength = 0;
                int bufNum = 0;
                boolean continueMerge = true;
                Iterator<Writer> iter = writerQueue.iterator();
                Writer lastWriter = w;
                writerBuffer.clear();

                long position = 0;

                while (iter.hasNext() && continueMerge) {
                    lastWriter = iter.next();

                    int writeLength = lastWriter.data.remaining() + 2 * Long.BYTES;
//                    lastWriter.offset = FREE_OFFSET.getAndAdd(writeLength);
                    lastWriter.offset = position;
                    position += writeLength;
                    writerBuffer.putLong(lastWriter.data.remaining());
                    writerBuffer.putLong(-1L); // next block
                    writerBuffer.put(lastWriter.data);

                    bufLength += writeLength;
                    bufNum += 1;
                    if (bufNum >= maxBufNum) {
                        continueMerge = false;
                    }
                    if (bufLength >= maxBufLength) {
                        continueMerge = false;
                    }
                }
                writerBuffer.flip();

                long writeStartOffset = FREE_OFFSET.getAndAdd(bufLength);
                ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 3);
                iter = writerQueue.iterator();
                Writer writer;
                do{
                    writer = iter.next();
                    writer.offset += writeStartOffset;
                    // 迁移更新上一个block指针的操作到写聚合
                    if(writer.block.tail == -1L){ // update an empty data queue
                        writer.block.tail = writer.offset;
                        writer.block.head = writer.block.tail;
                    }else{
                        buffer.clear();
                        buffer.putLong(writer.offset);
                        buffer.flip();
                        fileChannel.write(buffer, writer.block.tail + Long.BYTES); // 更新 nextOffset
                        writer.block.tail = writer.offset;
                    }
                    buffer.clear();
                    buffer.putLong(writer.block.totalNum+1);
                    buffer.putLong(writer.block.head);
                    buffer.putLong(writer.block.tail);
                    buffer.flip();
                    fileChannel.write(buffer, writer.block.metaOffset);
                } while (iter.hasNext() && !writer.equals(lastWriter));


                // 写期间 unlock 使得其他 writer 可以被加入 writerQueue
                {
                    lock.unlock();
                    testStat.stats[testStat.threadId.get()].addSample(writerBuffer.remaining());
                    this.fileChannel.write(writerBuffer, writeStartOffset);
                    this.fileChannel.force(true);
                    lock.lock();
                }

                while (true) {
                    Writer ready = writerQueue.pop();
                    if (!ready.equals(w)) {
                        ready.done = true;
                        ready.cv.signal();
//                        ready.offset += writeStartOffset;
                    }
                    if (ready.equals(lastWriter)){
                        break;
                    }
                }
                // Notify new head of write queue
                if (!writerQueue.isEmpty()) {
                    writerQueue.getFirst().cv.signal();
                }

//                w.offset += writeStartOffset;
                offset = w.offset;
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            } finally {
                lock.unlock();
            }
            return offset;
        }

        public Long put(ByteBuffer data) throws IOException{
            long startOffset = writeAgg(data, this);
//            totalNum++;
//            return totalNum-1;
            return totalNum++;
        }
        public long iterDataLink(long beginHandle, long beginOffset, long maxOffset) throws IOException {
            ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES);
            long targetHandle = beginHandle;
            for(long i=beginOffset; i<maxOffset && targetHandle != -1; ++i){
                Long nextOffset = targetHandle + Long.BYTES;
                tmp.clear();
//                ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES);
                int len = fileChannel.read(tmp, nextOffset);
                tmp.flip();
                targetHandle = tmp.getLong();
            }
            return targetHandle;
        }
        public Map<Integer, ByteBuffer> getRange(String key, Long offset, int fetchNum) throws IOException{
            long[] recordOffset = lastAccessOffset.get(key);
            long startOffset;
            if(recordOffset == null){
                recordOffset = new long[]{0, head};
                lastAccessOffset.put(key, recordOffset);
                startOffset = iterDataLink(head, 0, offset);
            }else if(recordOffset[0] > offset){
                startOffset = iterDataLink(head, 0, offset);
            }else{
                startOffset = iterDataLink(recordOffset[1], recordOffset[0], offset);
            }

            Map<Integer, ByteBuffer> res = new HashMap<>();
            ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES + Long.BYTES);
//            for(int i=0; i<offset && startOffset != -1; ++i){
//                Long nextOffset = startOffset + Long.BYTES;
//                tmp.clear();
//                int len = fileChannel.read(tmp, nextOffset);
//                tmp.flip();
//                startOffset = tmp.getLong();
//            }

            for(int i=0; i<fetchNum && startOffset != -1L; ++i){
//                ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES + Long.BYTES);
                tmp.clear();
                int len1 = fileChannel.read(tmp, startOffset);
                tmp.flip();

                Long dataSize = tmp.getLong();
                Long nextOffset = tmp.getLong();

                ByteBuffer tmp1 = ByteBuffer.allocate(dataSize.intValue());
                int len2 = fileChannel.read(tmp1, startOffset + Long.BYTES + Long.BYTES);
                tmp1.flip();
                res.put(i, tmp1);
                startOffset = nextOffset;
            }
            if(offset+fetchNum > recordOffset[0] && startOffset != -1){
                recordOffset[0] = offset+fetchNum;
                recordOffset[1] = startOffset;
            }
            return res;
        }
        public Long getMetaOffset(){
            return this.metaOffset;
        }
    }
}
