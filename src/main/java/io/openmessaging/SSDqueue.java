package io.openmessaging;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

//import org.slf4j.LoggerFactory;
//import org.slf4j.Logger;

public class SSDqueue{
    private static final Logger logger = Logger.getLogger(SSDqueue.class);

    FileChannel spaceMetaFc;

    AtomicLong META_FREE_OFFSET = new AtomicLong();
    AtomicInteger currentNum = new AtomicInteger();

    int QUEUE_NUM = 10000;
    int TOPIC_NUM = 100;
    
    int TOPIC_NAME_SZIE = 128;
    Long topicArrayOffset; // 常量

    ConcurrentHashMap<String, Long> topicNameQueueMetaMap;
    //ConcurrentHashMap<String, Map<Integer, Long>> queueTopicMap;
    ConcurrentHashMap<String, Map<Integer, DataMeta>> qTopicDataMap;

    FileChannel metaFileChannel;
    // opt
    int numOfDataFileChannels;
    DataSpace[] dataSpaces;
    ConcurrentHashMap<String, Map<Long,Long>> allDataOffsetMap;
    boolean RCOVER = false;

    TestStat testStat;
    public class TestStat{
        // report throughput per second
        ThreadLocal<Integer> threadId;
        AtomicInteger numOfThreads;
        Long startTime;
        Long endTime;
        Long opCount;

        AtomicBoolean reported;
        int[] oldTotalWriteBucketCount;
        MemoryUsage memoryUsage;

        public class ThreadStat {
            Long appendStartTime;
            Long appendEndTime;
            int appendCount;
            Long getRangeStartTime;
            Long getRangeEndTime;
            int getRangeCount;
            Long writeBytes;
            public int[] bucketBound;
            public int[] bucketCount;
            int dataSize;

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
                dataSize = 0;
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
                ret.dataSize = this.dataSize;
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

        DataSpace[] myDataSpaces;
        DataSpace.WriteStat[] oldWriteStats;

        // ThreadLocal< HashMap<Integer, Long> >
        // report operation per second
        TestStat(DataSpace[] dataSpaces){
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
            myDataSpaces = dataSpaces;
            oldWriteStats = new DataSpace.WriteStat[myDataSpaces.length];
        }

        void updateThreadId() {
            if (threadId.get() == null) {
                int thisNumOfThread = numOfThreads.getAndAdd(1);
                threadId.set(thisNumOfThread);
                logger.info("init thread id : " + thisNumOfThread);
            }
        }

        void appendStart(int size) {
            updateThreadId();
            int id = threadId.get();
            if (stats[id].appendStartTime == 0L) {
                stats[id].appendStartTime = System.nanoTime();
            }
            stats[id].dataSize = size;
        }

        void getRangeStart() {
            updateThreadId();
            int id = threadId.get();
            if (stats[id].getRangeStartTime == 0L) {
                stats[id].getRangeStartTime = System.nanoTime();
            }
        }

        void appendUpdateStat(String topic, int queueId, ByteBuffer data) {
            int id = threadId.get();
//            stats[id].addSample(data.remaining());
            stats[id].appendEndTime = System.nanoTime();
            stats[id].appendCount += 1;
            stats[id].writeBytes += stats[id].dataSize;
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
            if (reported.get()){
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

            String totalWriteBucketReport = "";
            totalWriteBucketReport += myBucketBound[0] + " < ";
            for (int i = 0; i < numOfBucket; i++){
                totalWriteBucketReport += "[" + totalWriteBucketCount[i] + "]";
                totalWriteBucketReport += " < " + myBucketBound[i+1] + " < ";
            }
            logger.info("[Total Append Data Dist]" + totalWriteBucketReport);

            if (oldTotalWriteBucketCount != null) {
                int[] curWriteBucketCount = new int[100];
                for (int i = 0; i < numOfBucket; i++) {
                    curWriteBucketCount[i] = totalWriteBucketCount[i] - oldTotalWriteBucketCount[i];
                }
                String curWriteBucketReport = "";
                curWriteBucketReport += myBucketBound[0] + " < ";
                for (int i = 0; i < numOfBucket; i++) {
                    curWriteBucketReport += "[" + curWriteBucketCount[i] + "]";
                    curWriteBucketReport += " < " + myBucketBound[i + 1] + " < ";
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
             for (int i = 0; i < dataSpaces.length; i++){
                 if (oldWriteStats[i] != null){
                     // get total write stat and cur write stat
                     DataSpace.WriteStat curWriteStat = dataSpaces[i].writeStat;
                     DataSpace.WriteStat oldWriteStat = oldWriteStats[i];
                     StringBuilder writeReport = new StringBuilder("");
//                     writeReport.append("[Total ] File ").append(i);
//                     writeReport.append(" " + "emptyQueueCount : ").append(curWriteStat.emptyQueueCount);
//                     writeReport.append(" " + "exceedBufNumCount : ").append(curWriteStat.exceedBufNumCount);
//                     writeReport.append(" " + "exceedBufLengthCount : ").append(curWriteStat.exceedBufLengthCount);
//                     logger.info(writeReport);
//                     logger.info("Write Size Dist : "+curWriteStat);

                     // current

                     oldWriteStat.emptyQueueCount = curWriteStat.emptyQueueCount - oldWriteStat.emptyQueueCount;
                     oldWriteStat.exceedBufLengthCount = curWriteStat.exceedBufLengthCount - oldWriteStat.exceedBufLengthCount;
                     oldWriteStat.exceedBufNumCount = curWriteStat.exceedBufNumCount - oldWriteStat.exceedBufNumCount;
                     for (int j = 0; j < oldWriteStat.bucketCount.length; j++){
                         oldWriteStat.bucketCount[j] = curWriteStat.bucketCount[j] - oldWriteStat.bucketCount[j];
                     }

                     curWriteStat = oldWriteStat;
                     writeReport = new StringBuilder("");
                     writeReport.append("[Current ] File ").append(i);
                     writeReport.append(" " + "emptyQueueCount : ").append(curWriteStat.emptyQueueCount);
                     writeReport.append(" " + "exceedBufNumCount : ").append(curWriteStat.exceedBufNumCount);
                     writeReport.append(" " + "exceedBufLengthCount : ").append(curWriteStat.exceedBufLengthCount);
                     logger.info(writeReport);
                     logger.info("Write Size Dist : "+curWriteStat.toString());
                 }
                oldWriteStats[i] = dataSpaces[i].writeStat.clone();
             }

//            logger.info(writeBandwidth+","+elapsedTimeS+","+appendThroughput+","+appendLatency+","+getRangeThroughput+","+getRangeLatency+",XXXXXX,"+curWriteBandwidth+","+thisElapsedTimeS+","+curAppendThroughput+","+curAppendLatency+","+curGetRangeThroughput+","+curGetRangeLatency);

            // deep copy
            oldStats = stats.clone();
            for (int i = 0; i < 100; i++) {
                oldStats[i] = stats[i].clone();
            }
            oldEndTime = endTime;
        }

        // report topic stat per second
    }



    public SSDqueue(String dirPath){

        this.numOfDataFileChannels = 4;
        try {
            //init(dirPath);

            dataSpaces = new DataSpace[numOfDataFileChannels];
            testStat = new TestStat(dataSpaces);
            boolean flag = new File(dirPath + "/meta").exists();
            if(flag){
                // recover
                // 读盘，建表
                logger.info("recover");
                RCOVER = true;
                META_FREE_OFFSET.set(0L);
                ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES * 2);
                this.metaFileChannel = new RandomAccessFile(new File(dirPath + "/meta"), "rw").getChannel();;
                for(int i=0; i < numOfDataFileChannels; i++){
                    String dbPath = dirPath + "/db" + i;
                    dataSpaces[i] = new DataSpace(new RandomAccessFile(new File(dbPath), "rw").getChannel());
                }

                this.topicNameQueueMetaMap = new ConcurrentHashMap<>();
                //this.queueTopicMap = new ConcurrentHashMap<>();
                this.qTopicDataMap = new ConcurrentHashMap<>();

                this.topicArrayOffset = 0L + Integer.BYTES;

                tmp = ByteBuffer.allocate(Integer.BYTES);
                metaFileChannel.read(tmp);
                tmp.flip();
                this.currentNum.set(tmp.getInt());
                //logger.info(this.toString() + " " + this.currentNum.get()) ;
                Long offset = 0L + Integer.BYTES;
                tmp = ByteBuffer.allocate(Long.BYTES + TOPIC_NAME_SZIE);
                for(int i=0; i<currentNum.get(); i++, offset += Long.BYTES + TOPIC_NAME_SZIE){
                    int len = metaFileChannel.read(tmp, offset);
                    //logger.info("len: " + len + " " + this.toString());
                    tmp.flip();
                    Long queueMetaOffset = tmp.getLong();

                    byte [] bytes = new byte[TOPIC_NAME_SZIE];
                    tmp.get(bytes);
                    String topicName = new String(bytes).trim();

                    topicNameQueueMetaMap.put(topicName, queueMetaOffset);

                    // 遍历每个 topic 下的 queue
                    qTopicDataMap.put(topicName, readQueue(queueMetaOffset));
                }
            }else{
                // create new mq
                this.allDataOffsetMap = new ConcurrentHashMap<>();
                
                this.metaFileChannel = new RandomAccessFile(new File(dirPath + "/meta"), "rw").getChannel();
                for(int i=0; i < numOfDataFileChannels; i++){
                    String dbPath = dirPath + "/db" + i;
                    dataSpaces[i] = new DataSpace(new RandomAccessFile(new File(dbPath), "rw").getChannel(), Long.BYTES);
                }

                currentNum.set(0);
                META_FREE_OFFSET.set(TOPIC_NUM * (TOPIC_NAME_SZIE + Long.BYTES) + Integer.BYTES);
                this.topicArrayOffset = 0L + Integer.BYTES;

                this.topicNameQueueMetaMap = new ConcurrentHashMap<>();
                //this.queueTopicMap = new ConcurrentHashMap<>();
                this.qTopicDataMap = new ConcurrentHashMap<>();

                logger.info("initialize new SSDqueue, num: "+currentNum.get());
            }
        // 划分起始的 Long.BYTES * 来存元数据
        } catch (Exception e) {
            //TODO: handle exception
            logger.error("error 201777");
            e.printStackTrace();
        }

    }
    // void init(String dirPath){
    //     try {
    //         this.spaceMetaFc = new RandomAccessFile(new File(dirPath + "space"), "rw").getChannel();
    //     } catch (Exception e) {
    //         //TODO: handle exception
    //         e.printStackTrace();
    //     }
    // }
    // void updataSpace(){
    //     try {
    //         ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES * 2);
    //         //tmp.putLong(FREE_OFFSET.get());
    //         tmp.putLong(META_FREE_OFFSET.get());
    //         tmp.flip();
    //         spaceMetaFc.write(tmp, 0L);
    //         spaceMetaFc.force(true);
    //     } catch (Exception e) {
    //         //TODO: handle exception
    //         e.printStackTrace();
    //     }
    // }
    String spitMark = "*$@%";

    public Long append(String topicName, int queueId, ByteBuffer data){
        testStat.appendStart(data.remaining());
        Long result;
        try{
            Map<Integer, DataMeta> topicData = qTopicDataMap.get(topicName);
            if(topicData == null){
                // 自下而上
                int fcId = Math.floorMod(topicName.hashCode(), numOfDataFileChannels);
                Data writeData = new Data(dataSpaces[fcId]);

                result = writeData.put(data);

                QueueId q =  new QueueId(metaFileChannel, QUEUE_NUM);
                Long qOffset = q.put(queueId, writeData.getMetaOffset());

                this.put(topicName, q.metaDataOffset);
                
                // 更新 DRAM map
                topicData = new HashMap<>();
                topicData.put(queueId, writeData.getMeta());
                topicNameQueueMetaMap.put(topicName, q.getMetaOffset());
                qTopicDataMap.put(topicName, topicData);
                metaFileChannel.force(true);

                String key = topicName + spitMark + queueId;
                Map<Long, Long> tmp2 = new HashMap<>();
                tmp2.put(result, writeData.tail);

                allDataOffsetMap.put(key, tmp2);

            }else{
                DataMeta meta = topicData.get(queueId);
                if(meta == null){
                    // 增加 queueIdArray
                    int fcId = Math.floorMod(topicName.hashCode(), numOfDataFileChannels);
                    Data writeData = new Data(dataSpaces[fcId]);

                    result = writeData.put(data);

                    Long queueMetaOffset = topicNameQueueMetaMap.get(topicName);
                    QueueId queueArray =  new QueueId(metaFileChannel, queueMetaOffset); // 写入 SSD
                    Long queueOffset = queueArray.put(queueId, writeData.getMetaOffset());

                    // 插入 DRAM 哈希表
                    topicData.put(queueId, writeData.getMeta());
                    qTopicDataMap.put(topicName, topicData);
                    metaFileChannel.force(true);

                    String key = topicName + spitMark + queueId;
                    Map<Long, Long> tmp2 = allDataOffsetMap.get(key);
                    tmp2.put(result, writeData.tail);
                    allDataOffsetMap.put(key, tmp2);

                }else{
                    int fcId = Math.floorMod(topicName.hashCode(), numOfDataFileChannels);
                    //Data writeData = new Data(dataSpaces[fcId], metaDataOffset);

                    Data writeData = new Data(dataSpaces[fcId], meta);

                    result = writeData.put(data);

                    topicData.put(queueId, writeData.getMeta());
                    qTopicDataMap.put(topicName, topicData);


                    String key = topicName + spitMark + queueId;
                    Map<Long, Long> tmp2 = allDataOffsetMap.get(key);
                    tmp2.put(result, writeData.tail);
                    allDataOffsetMap.put(key, tmp2);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        testStat.appendUpdateStat(topicName, queueId, data);
        return result;
    }
    public Map<Integer, ByteBuffer> getRange(String topicName, int queueId, Long offset, int fetchNum){
        Map<Integer, ByteBuffer> result = new HashMap<>();
        try{
            testStat.getRangeStart();
            Map<Integer, DataMeta> topicData = qTopicDataMap.get(topicName);
            if(topicData == null) return result;

            DataMeta meta = topicData.get(queueId);
            if(meta == null) return result;

            int fcId = Math.floorMod(topicName.hashCode(), numOfDataFileChannels);
            Data resData = new Data(dataSpaces[fcId], meta.metaOffset);
            //Data resData = new Data(fileChannel, metaDataOffset);
            if(RCOVER) result = resData.getRange(offset, fetchNum);
            else{
                String key = topicName + spitMark + queueId;
                result = resData.getRange(key, offset, fetchNum);
            }
            testStat.getRangeUpdateStat(topicName,queueId, offset, fetchNum);
        }catch(IOException e){
            logger.error(e);
        }
        return result;
    }

    public Map<Integer, DataMeta> readQueue(Long queueMetaOffset) throws IOException{
        QueueId resData = new QueueId(metaFileChannel, queueMetaOffset);
        return resData.readAll();
    }
    public void put(String topic, long qHandle){

        StringBuilder padStr = new StringBuilder(topic);
        char[] help = new char[TOPIC_NAME_SZIE-topic.length() + 1];
        for(int i=0;i<help.length; ++i){
            help[i] = ' ';
        }
        padStr.append(help);

        ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES + TOPIC_NAME_SZIE); // offset : name
        tmp.putLong(qHandle);
        tmp.put(padStr.toString().getBytes(), 0, TOPIC_NAME_SZIE);
        tmp.flip();

        try {
            metaFileChannel.write(tmp, this.topicArrayOffset + (currentNum.getAndIncrement()) * (TOPIC_NAME_SZIE + Long.BYTES));
            ByteBuffer metaTmp = ByteBuffer.allocate(Integer.BYTES);
            metaTmp.putInt(currentNum.get());
            metaTmp.flip();
            int len = metaFileChannel.write(metaTmp, 0L);
        } catch (Exception e) {
            //TODO: handle exception
            e.printStackTrace();
        }
    }

    private class QueueId{
        FileChannel metaFileChannel;
        
        Long metaDataOffset;
        int currentNum;
        Long queueIdArray;
        
        public QueueId(FileChannel metaFileChannel, int totalNum)throws IOException{
            this.metaFileChannel = metaFileChannel;

            this.metaDataOffset = META_FREE_OFFSET.getAndAdd(totalNum * (Long.BYTES + Integer.BYTES) + Integer.BYTES);
            //updataSpace();

            this.currentNum = 0;
            ByteBuffer tmp = ByteBuffer.allocate(Integer.BYTES);
            tmp.putInt(this.currentNum);
            tmp.flip();
            metaFileChannel.write(tmp, metaDataOffset);
//            metaFileChannel.force(true); // TODO:check

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
            long offset = queueIdArray + (long) currentNum * (Integer.BYTES + Long.BYTES);

            ByteBuffer tmpData = ByteBuffer.allocate(Integer.BYTES + Long.BYTES);
            tmpData.putInt(queueId);
            tmpData.putLong(dataMetaOffset);
            tmpData.flip();
            metaFileChannel.write(tmpData, offset);
            //metaFileChannel.force(true);

            currentNum++;
            ByteBuffer tmp = ByteBuffer.allocate(Integer.BYTES);
            tmp.putInt(this.currentNum);
            tmp.flip();
            metaFileChannel.write(tmp, metaDataOffset);
            this.queueIdArray = this.metaDataOffset + Integer.BYTES;
            return offset;
        }
        public Long getMetaOffset(){
            return this.metaDataOffset;
        }
        public Map<Integer, DataMeta> readAll() throws IOException{
            Map<Integer, DataMeta> res = new HashMap<>();
            ByteBuffer tmp = ByteBuffer.allocate(Integer.BYTES + Long.BYTES);
            for(int i=0; i<this.currentNum; ++i){
                int len2 = metaFileChannel.read(tmp, this.queueIdArray + i*(Integer.BYTES + Long.BYTES));
                tmp.flip();
                res.put(tmp.getInt(), new DataMeta(tmp.getLong(), 0, -1, 0) );
                tmp.clear(); // 清空buffer为下一次写buffer作准备
            }
            return res;
        }
        public String toString(){
            return "num: " + this.currentNum + " meta:  " + this.metaDataOffset + "  array： " + this.queueIdArray;
        }
    }
    /*
    * 只存 head
    *  <Length, nextOffset, Data>
    */

    

    public class Data{
        Long totalNum; // 不存
        Long tail; // 不存
        Long head; // 存
        Long metaOffset; //不存

        DataSpace ds;
        public DataMeta getMeta(){
            return new DataMeta(metaOffset, head, tail, totalNum);
        }
        public Data(DataSpace ds) throws IOException{
            this.ds  = ds;
            this.metaOffset = -1L;//ds.createLink();

            this.totalNum = 0L;
            this.tail = -1L;
            this.head = -1L;
        }
        public Data(DataSpace ds, Long metaOffset) throws IOException{
            this.ds = ds;
            this.metaOffset = metaOffset;
//            ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES);
//            ds.read(tmp, metaOffset);
//            tmp.flip();
//
//            this.head = tmp.getLong();
            this.head = metaOffset;
            this.tail = -1L;
            this.totalNum = 0L;

            // 恢复 totalNum, tail, head
            // ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES + Long.BYTES + Long.BYTES);
            // ds.read(tmp, metaOffset);
            // tmp.flip();

            // this.totalNum = tmp.getLong();
            // this.head = tmp.getLong();
            // this.tail = tmp.getLong();
        }
        public Data(DataSpace ds, DataMeta dm){
            this.ds = ds;
            this.head = dm.head;
            this.tail = dm.tail;
            this.totalNum = dm.totalNum;
            this.metaOffset = dm.metaOffset;
        }

        public Long put(ByteBuffer data) throws IOException{
            long res = totalNum;
            DataMeta dataMeta = ds.writeAgg(data, this.getMeta());
            this.head = dataMeta.head;
            this.tail = dataMeta.tail;
            this.totalNum = dataMeta.totalNum + 1;
            this.metaOffset = dataMeta.metaOffset;
//            if(tail == -1L){
//                head = offset;
//                tail = offset;
//                ds.updateMeta(metaOffset, totalNum, head, tail);
//            }else{
//                ds.updateLink(tail, offset);
//                tail = offset;
//            }
//            }

//            totalNum++;
//            ds.updateMeta(metaOffset, totalNum, head, tail);
            return res;
        }


        public Map<Integer, ByteBuffer> getRange(String key, Long offset, int fetchNum) throws IOException{

            Map<Long, Long> map = allDataOffsetMap.get(key);

            Long startOffset = map.get(offset);
            if(startOffset == null){
                startOffset = tail;
            }

            //Long cnt = offset;

            Map<Integer, ByteBuffer> res = new HashMap<>();
            
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2);
            for(int i=0; i<fetchNum && startOffset != -1L; ++i){
                buffer.clear();
                int len1 = ds.read(buffer, startOffset);
                buffer.flip();

                long dataSize = buffer.getLong();
                long nextOffset = buffer.getLong();

                ByteBuffer tmp1 = ByteBuffer.allocate((int) dataSize);
                int len2 = ds.read(tmp1, startOffset + Long.BYTES + Long.BYTES);
                tmp1.flip();
                res.put(i, tmp1);
                // if(startOffset == tail) break;
                startOffset = nextOffset;

            }
            return res;
        }

        public Map<Integer, ByteBuffer> getRange(Long offset, int fetchNum) throws IOException{
            Long startOffset = head;
            Map<Integer, ByteBuffer> res = new HashMap<>();
            ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES);

            for(int i=0; i<offset && startOffset != -1L; ++i){
                // if(startOffset == tail){
                //     startOffset = -1L;
                //     break;
                // }

                Long nextOffset = startOffset + Long.BYTES;
                tmp.clear();
                int len = ds.read(tmp, nextOffset);
                tmp.flip();
                startOffset = tmp.getLong();
            }
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2);
            for(int i=0; i<fetchNum && startOffset != -1L; ++i){
                buffer.clear();
                int len1 = ds.read(buffer, startOffset);
                buffer.flip();

                long dataSize = buffer.getLong();
                long nextOffset = buffer.getLong();

                ByteBuffer tmp1 = ByteBuffer.allocate((int) dataSize);
                int len2 = ds.read(tmp1, startOffset + Long.BYTES + Long.BYTES);
                tmp1.flip();
                res.put(i, tmp1);
                // if(startOffset == tail) break;
                startOffset = nextOffset;

            }
            return res;
        }
        public Long getMetaOffset(){
            return this.metaOffset;
        }
        public String toString(){
            return "nums: " + totalNum + " head: " + head + " tail: " + tail + " meta: " + metaOffset;
        }
    }
}
