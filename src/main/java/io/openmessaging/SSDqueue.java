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
import java.util.Vector;
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
public class SSDqueue{
    private static final Logger logger = Logger.getLogger(SSDqueue.class);
    
    // FileChannel spaceMetaFc;

    //AtomicLong FREE_OFFSET = new AtomicLong();
    //AtomicLong META_FREE_OFFSET = new AtomicLong();
    //AtomicInteger currentNum = new AtomicInteger();

    
    // int QUEUE_NUM = 10000;
    // int TOPIC_NUM = 100;
    
    // int TOPIC_NAME_SZIE = 128;
    // Long topicArrayOffset; // 常量

    // ConcurrentHashMap<String, Long> topicNameQueueMetaMap;
    
    //FileChannel fileChannel;
    FileChannel metaFileChannel;
    // opt
    int numOfDataFileChannels;
    DataSpace[] dataSpaces;

    ConcurrentHashMap<String,DataMeta> qTopicQueueDataMap;
    MetaTopicQueue mqMeta;
    private String markSpilt = "$@#";
    ConcurrentHashMap<String, ConcurrentHashMap<Long,Long[]>> allDataOffsetMap; // 加速 getRange
    
    boolean RECOVER = false;
    ConcurrentHashMap<String, HotData> hotDataMap;

    TestStat testStat;

    AtomicLong hitHotCount = new AtomicLong(0L);
    AtomicLong totalReadCount = new AtomicLong(0L);

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
                logger.info("reover");
//                RECOVER = true; // 得分需取消注释此行
                this.metaFileChannel = new RandomAccessFile(new File(dirPath + "/meta"), "rw").getChannel();
                this.mqMeta = new MetaTopicQueue(this.metaFileChannel);

                for(int i=0; i < numOfDataFileChannels; i++){
                    String dbPath = dirPath + "/db" + i;
                    dataSpaces[i] = new DataSpace(new RandomAccessFile(new File(dbPath), "rw").getChannel());              
                }
                this.qTopicQueueDataMap = mqMeta.getMap();
            }else{
                // create new mq    
                logger.info("create a new queue");
                
                this.allDataOffsetMap = new ConcurrentHashMap<>();
                
                this.metaFileChannel = new RandomAccessFile(new File(dirPath + "/meta"), "rw").getChannel();
                this.mqMeta = new MetaTopicQueue(this.metaFileChannel, Long.BYTES);

                for(int i=0; i < numOfDataFileChannels; i++){
                    String dbPath = dirPath + "/db" + i;
                    dataSpaces[i] = new DataSpace(new RandomAccessFile(new File(dbPath), "rw").getChannel(), Long.BYTES);
                }
                this.qTopicQueueDataMap = new ConcurrentHashMap<>();
                this.hotDataMap = new ConcurrentHashMap<>();

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
        int dataSize = data.remaining();
        testStat.appendStart(data.remaining());
        byte[] hotData = new byte[data.remaining()];
        data.mark();
        data.get(hotData);
        data.reset();

        String key = topicName + markSpilt + queueId;
        Long result;
        try{
            DataMeta tmpD = qTopicQueueDataMap.get(key);
            // if(topicData == null){
            if(tmpD == null){
                
                /////////  自下而上, 持久化
                int fcId = Math.floorMod(topicName.hashCode(), numOfDataFileChannels);
                Data writeData = new Data(dataSpaces[fcId]);
                result = writeData.put(data);

                mqMeta.put(key, writeData.getMetaOffset());
                //////// 更新 DRAM map
                ConcurrentHashMap<Long, Long[]> tmp2 = new ConcurrentHashMap<>();
                tmp2.put(result,new Long[]{writeData.tail, (long) dataSize});
                allDataOffsetMap.put(key, tmp2);

                qTopicQueueDataMap.put(key, writeData.getMeta());            
                mqMeta.force();

            }else{
                int fcId = Math.floorMod(topicName.hashCode(), numOfDataFileChannels);
                Data writeData = new Data(dataSpaces[fcId], tmpD);
                result = writeData.put(data);
                
                ConcurrentHashMap<Long, Long[]> tmp2 = allDataOffsetMap.get(key);
                tmp2.put(result, new Long[]{writeData.tail, (long) dataSize});
                allDataOffsetMap.put(key, tmp2);
                
                qTopicQueueDataMap.put(key, writeData.getMeta());
            }
            
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        hotDataMap.put(key, new HotData(result, hotData));

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

            totalReadCount.incrementAndGet();
            if(!RECOVER && hotDataMap.get(key).offset == offset){
                hitHotCount.incrementAndGet();
                byte[] array = hotDataMap.get(key).data;
                ByteBuffer tmp = ByteBuffer.wrap(array);
                result.put(0, tmp);
            }else{
                int fcId = Math.floorMod(topicName.hashCode(), numOfDataFileChannels);
                Data resData = new Data(dataSpaces[fcId], dataMeta);
                result = RECOVER ? resData.getRange(offset, fetchNum) : resData.getRange(key, offset, fetchNum);
            
            }
            testStat.getRangeUpdateStat(topicName,queueId, offset, fetchNum);
        }catch(IOException e){
            logger.error(e);
        }
        return result;
    } 

    class MetaTopicQueue{
        private FileChannel metaFc;
        private AtomicLong META_FREE_OFFSET;
        private AtomicLong totalNum;
        long keySize = 64;
        
        long arrayStartOffset = Long.BYTES;
        
        MetaTopicQueue(FileChannel fc){
            // recover
            this.metaFc = fc;
            ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES);
            try {
                fc.read(tmp, 0L);
                tmp.flip();
                totalNum = new AtomicLong(tmp.getLong());
                META_FREE_OFFSET = new AtomicLong(0L);
            } catch (Exception e) {
                //TODO: handle exception
                e.printStackTrace();
            }
        }
        public void force() throws IOException{
            this.metaFc.force(true);
        }
        MetaTopicQueue(FileChannel fc, long startOffset){
            this.metaFc = fc;
            META_FREE_OFFSET = new AtomicLong(startOffset);
            this.totalNum = new AtomicLong(0L);
            //arrayStartOffset = startOffset;
        }
        synchronized public long put(String key, long offset) throws IOException{
            long res = META_FREE_OFFSET.getAndAdd( keySize + Long.BYTES);

            // TODO: 填充 key
            StringBuilder padStr = new StringBuilder(key);
            char[] help = new char[(int) (keySize - key.length())];
            for(int i=0;i<help.length; ++i){
                help[i] = ' ';
            }
            padStr.append(help);

            ByteBuffer buffer = ByteBuffer.allocate((int)(keySize + Long.BYTES));
            buffer.putLong(offset);
            buffer.put(padStr.toString().getBytes(), 0, (int)keySize);
            buffer.flip();
            metaFc.write(buffer, res);

            long ret = totalNum.getAndIncrement();

            // 持久化 totalNum
            buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.putLong(totalNum.get());
            buffer.flip();
            metaFc.write(buffer, 0L);

            //metaFc.force(true);

            return ret;
        }
        public ConcurrentHashMap<String, DataMeta> getMap(){
            ConcurrentHashMap<String, DataMeta> res = new ConcurrentHashMap<>();
            long startOffset = this.arrayStartOffset;
            ByteBuffer buffer = ByteBuffer.allocate((int)(keySize + Long.BYTES));
            for(int i=0; i<totalNum.get(); i++){
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
                startOffset += keySize + Long.BYTES;
            }
            return res;
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
            this.totalNum++;
            this.metaOffset = this.head;
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
            Map<Integer, ByteBuffer> res = new HashMap<>();

            Map<Long, Long[]> map = allDataOffsetMap.get(key);

            Long[] dataInfo = map.get(offset);
           
            for(int i=0; i<fetchNum && dataInfo != null; ++i){

                ByteBuffer tmp1 = ByteBuffer.allocate(dataInfo[1].intValue());
                int len2 = ds.read(tmp1, dataInfo[0]  + Long.BYTES + Long.BYTES);
                tmp1.flip();
                res.put(i, tmp1);
                offset++;
                dataInfo = map.get(offset);
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
    public class HotData{
        public long offset;
        public byte[] data;
        public HotData(long offset, byte[] data){
            this.offset = offset;
            this.data = data; // TODO
        }
    }

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

            String hitHotStr = String.format("Hit hot rate: %d / %d = %.3f",
                    hitHotCount.get(), totalReadCount.get(), ((double)hitHotCount.get())/totalReadCount.get());
            logger.info("appendStat   :"+appendStat);
            logger.info("getRangeStat :"+getRangeStat);
            logger.info("csvStat      :"+csvStat);
            logger.info("Memory Used (GiB) : "+memoryUsage.getUsed()/(double)(1024*1024*1024));
            logger.info(hitHotStr);

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


}
