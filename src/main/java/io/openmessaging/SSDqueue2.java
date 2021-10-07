package io.openmessaging;


import org.apache.log4j.Logger;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
//import org.slf4j.LoggerFactory;
//import org.slf4j.Logger;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
public class SSDqueue2{
    private static final Logger logger = Logger.getLogger(SSDqueue2.class);
    
    FileChannel spaceMetaFc;

    //AtomicLong FREE_OFFSET = new AtomicLong();
    AtomicLong META_FREE_OFFSET = new AtomicLong();
    AtomicInteger currentNum = new AtomicInteger();

    
    int QUEUE_NUM = 10000;
    int TOPIC_NUM = 100;
    
    int TOPIC_NAME_SZIE = 128;
    Long topicArrayOffset; // 常量

    ConcurrentHashMap<String, Long> topicNameQueueMetaMap;
    
    //FileChannel fileChannel;
    FileChannel metaFileChannel;
    // opt
    int numOfDataFileChannels;
    DataSpace[] dataSpaces;

    ConcurrentHashMap<String,DataMeta> qTopicQueueDataMap;
    MetaTopicQueue mqMeta;
    private String markSpilt = "$@#";

    TestStat testStat;

    public SSDqueue2(String dirPath){
        testStat = new TestStat();
        this.numOfDataFileChannels = 128;
        try {
            //init(dirPath);
            
            dataSpaces = new DataSpace[numOfDataFileChannels];
            boolean flag = new File(dirPath + "/meta").exists();
            if(flag){
                // recover
                // 读盘，建表 
                logger.info("reover");
                this.metaFileChannel = new RandomAccessFile(new File(dirPath + "/meta"), "rw").getChannel();
                this.mqMeta = new MetaTopicQueue(this.metaFileChannel);

                for(int i=0; i < numOfDataFileChannels; i++){
                    String dbPath = dirPath + "/db" + i;
                    dataSpaces[i] = new DataSpace(new RandomAccessFile(new File(dbPath), "rw").getChannel());              
                }
                this.qTopicQueueDataMap = mqMeta.getMap();
            }else{
                // create new mq              
                this.metaFileChannel = new RandomAccessFile(new File(dirPath + "/meta"), "rw").getChannel();
                mqMeta = new MetaTopicQueue(this.metaFileChannel, Long.BYTES);

                for(int i=0; i < numOfDataFileChannels; i++){
                    String dbPath = dirPath + "/db" + i;
                    dataSpaces[i] = new DataSpace(new RandomAccessFile(new File(dbPath), "rw").getChannel(), Long.BYTES);
                }
                this.qTopicQueueDataMap = new ConcurrentHashMap<>();
                testStat = new TestStat();
                //logger.info("initialize new SSDqueue, num: "+currentNum.get());
            }
        // 划分起始的 Long.BYTES * 来存元数据
        } catch (Exception e) {
            //TODO: handle exception
            logger.info("error 201777");
            e.printStackTrace();
        }
        
    }    

    public Long append(String topicName, int queueId, ByteBuffer data){
        testStat.appendStart(data.remaining());

        String key = topicName + markSpilt + queueId;
        Long result;
        try{
            DataMeta tmpD = qTopicQueueDataMap.get(key);

            // if(topicData == null){
            if(tmpD == null){
                // 自下而上
                int fcId = Math.floorMod(topicName.hashCode(), numOfDataFileChannels);
                Data writeData = new Data(dataSpaces[fcId]);
                result = writeData.put(data);

                mqMeta.put(key, writeData.getMetaOffset());            
                // 更新 DRAM map
                qTopicQueueDataMap.put(key, writeData.getMeta());

            }else{
                int fcId = Math.floorMod(topicName.hashCode(), numOfDataFileChannels);
                Data writeData = new Data(dataSpaces[fcId], tmpD);
                result = writeData.put(data);
                qTopicQueueDataMap.put(key, writeData.getMeta());
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
        String key = topicName + markSpilt + queueId;
        try{
            testStat.getRangeStart();

            DataMeta dataMeta = qTopicQueueDataMap.get(key);
            if(dataMeta == null) return result;

            int fcId = Math.floorMod(topicName.hashCode(), numOfDataFileChannels);
            Data resData = new Data(dataSpaces[fcId], dataMeta);
            result = resData.getRange(offset, fetchNum);
            
            testStat.getRangeUpdateStat(topicName,queueId, offset, fetchNum);
        }catch(IOException e){
            logger.error(e);
        }
        return result;
    } 

    class MetaTopicQueue{
        private FileChannel metaFc;
        private AtomicLong META_FREE_OFFSET;
        long keySize = 256;
        long totalNum;
        long arrayStartOffset = Long.BYTES;
        
        MetaTopicQueue(FileChannel fc){
            // recover
            this.metaFc = fc;
            ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES);
            try {
                fc.read(tmp, 0L);
                tmp.flip();
                totalNum = tmp.getLong();
            } catch (Exception e) {
                //TODO: handle exception
                e.printStackTrace();
            }
        }

        MetaTopicQueue(FileChannel fc, long startOffset){
            this.metaFc = fc;
            META_FREE_OFFSET = new AtomicLong(startOffset);
            this.totalNum = 0L;
            //arrayStartOffset = startOffset;
        }
        public long put(String key, long offset) throws IOException{
            long res = META_FREE_OFFSET.getAndAdd( keySize + Long.BYTES);

            // TODO: 填充 key
            StringBuilder padStr = new StringBuilder(key);
            char[] help = new char[(int) (keySize - key.length() + 1)];
            for(int i=0;i<help.length; ++i){
                help[i] = ' ';
            }
            padStr.append(help);

            ByteBuffer buffer = ByteBuffer.allocate((int)(keySize + Long.BYTES));
            buffer.putLong(offset);
            buffer.put(padStr.toString().getBytes(), 0, (int)keySize);
            buffer.flip();
            metaFc.write(buffer, res);

            totalNum++;

            // 持久化 totalNum
            buffer.clear();
            buffer.putLong(totalNum);
            buffer.flip();
            metaFc.write(buffer, 0L);

            metaFc.force(true);

            return totalNum;
        }
        public ConcurrentHashMap<String, DataMeta> getMap(){
            ConcurrentHashMap<String, DataMeta> res = new ConcurrentHashMap<>();
            long startOffset = this.arrayStartOffset;
            ByteBuffer buffer = ByteBuffer.allocate((int)(keySize + Long.BYTES));
            for(int i=0; i<totalNum; i++){
                buffer.clear();
                try {
                    metaFc.read(buffer, startOffset);
                } catch (Exception e) {
                    //TODO: handle exception
                    e.printStackTrace();
                }
                
                buffer.flip();
                long value = buffer.getLong();
                byte[] bytes = new byte[(int) keySize];
                buffer.get(bytes);
                String key = new String(bytes).trim();
                res.put(key, new DataMeta(value, value, -1, -1));
            }
            return res;
        }
    }
    /*
    * 只存 head
    *  <Length, nextOffset, Data>
    */
    private class Data{
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
            this.metaOffset = -1L;
           
            this.totalNum = -1L;
            this.tail = -1L;
            this.head = this.metaOffset;
        }
        public Data(DataSpace ds, Long metaOffset) throws IOException{
            this.ds = ds;
            this.metaOffset = metaOffset;
            this.head = metaOffset;
            // ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES);
            // ds.read(tmp, metaOffset);
            // tmp.flip();

            // this.head = tmp.getLong();
            this.tail = -1L;
            this.totalNum = -1L;

        }
        public Data(DataSpace ds, DataMeta dm){
            this.ds = ds;
            this.head = dm.head;
            this.tail = dm.tail;
            this.totalNum = dm.totalNum;
            this.metaOffset = dm.metaOffset;
        }
        public Long put(ByteBuffer data) throws IOException{
            //logger.info(toString());
            long offset = ds.write(data); // TODO: force
            if(tail == -1L){
                head = offset;
                tail = offset;
                metaOffset = offset;
                //ds.updateMeta(metaOffset, totalNum, head, tail);
            }else{
                ds.updateLink(tail, offset); //
                tail = offset;
            }
            long res = totalNum;
            totalNum++;
            //ds.updateMeta(metaOffset, totalNum, head, tail);
            return res;
        }
        public Map<Integer, ByteBuffer> getRange(Long offset, int fetchNum) throws IOException{
        
            Long startOffset = head;
            Map<Integer, ByteBuffer> res = new HashMap<>();
            ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES);

            boolean flag = true;
            if(this.totalNum != -1 && offset == this.totalNum - 1){
                startOffset = tail;
                flag = false;
            }
            //logger.info(this.toString());

            for(int i=0; i<offset && startOffset != -1L && flag; ++i){
                // if(startOffset == tail){
                //     startOffset = -1L;
                //     break;
                // }
                //logger.info(this.toString());

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
                //logger.info(len2);

                tmp1.flip();
                res.put(i, tmp1);
                // if(startOffset == tail) break;
                startOffset = nextOffset;
                //logger.info(this.toString() + "offset: " + startOffset);
                
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
                dataSize = 0;

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

        void appendStart(int size) {
            updateThreadId();
            int id = threadId.get();
            if (stats[id].appendStartTime == 0L) {
                stats[id].appendStartTime = System.nanoTime();
                // log.info("init append time");
            }
            stats[id].dataSize = size;
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
            stats[id].addSample(stats[id].dataSize);
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
            
            String appendStat = "";
            String getRangeStat = "";
            for (int i = 0; i < getNumOfThreads; i++){
                appendStat += String.format("%d,", curAppendCount[i]);
                getRangeStat += String.format("%d,", curGetRangeCount[i]);
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

}
