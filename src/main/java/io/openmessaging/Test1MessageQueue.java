package io.openmessaging;

import java.io.IOException;

import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.io.RandomAccessFile;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;

import org.apache.log4j.spi.LoggerFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.lang.ThreadLocal;
import java.lang.Math;
import java.text.Format;
import java.util.concurrent.TimeoutException;

import java.util.concurrent.locks.Condition;

public class Test1MessageQueue {
    private static final Logger log = Logger.getLogger(Test1MessageQueue.class);
    private static class MQConfig {
        // // version 0: local SSD: 70 MiB/s   
        // int numOfDataFiles = 10;
        // int minBufNum = 20; // 无效
        // int minBufLength = 32768; // 无效
        // int timeOutMS = 150; // 无效
        // boolean fairLock = true;
        // boolean useWriteAgg = false; // 不使用写聚合

        // version 1: local SSD: 110MiB/s   
        // int numOfDataFiles = 4;
        // int minBufNum = 20;
        // int minBufLength = 28672;
        // int timeOutMS = 150;
        // boolean fairLock = true;
        // boolean useWriteAgg = true; // 使用写聚合

        // version 1: local SSD: 110MiB/s   
        // int numOfDataFiles = 4;
        // int minBufNum = 6;
        // int minBufLength = 28672;
        // int timeOutMS = 150;
        // boolean fairLock = true;
        // boolean useWriteAgg = true; // 使用写聚合



        // version 2: local SSD: 100MiB/s   for 40t
        // int numOfDataFiles = 4;
        // int minBufNum = 16;
        // int minBufLength = 20480+1024+1024;
        // int timeOutMS = 100;
        // boolean fairLock = true;
        // boolean useWriteAgg = true; // 使用写聚合

        // version 3: test for online
        int numOfDataFiles = 5;
        int minBufNum = 3;
        int minBufLength = 24576;
        int timeOutMS = 200;
        boolean fairLock = true;
        boolean useWriteAgg = true; // 使用写聚合



        // int minBufLength = 32768;
        
        // int minBufLength = 24576;
        // int minBufLength = 16384;
        // boolean useWriteAgg = true;
        // boolean fairLock = false;
        // boolean useWriteAgg = false;
        @Override
        public String toString() {
            return String.format("useWriteAgg=%b | numOfDataFiles=%d | minBufLength=%d | minBufNum=%d | timeOutMS=%d",useWriteAgg,numOfDataFiles,minBufLength,minBufNum,timeOutMS);
        }
    }
    private static MQConfig mqConfig = new MQConfig();

    private class TestStat {
        // report throughput per second
        ThreadLocal<Integer> threadId;
        AtomicInteger numOfThreads;
        Long startTime;
        Long endTime;
        Long opCount;
        AtomicBoolean reported;

        private class ThreadStat {
            Long appendStartTime;
            Long appendEndTime;
            int appendCount;
            Long getRangeStartTime;
            Long getRangeEndTime;
            int getRangeCount;
            Long writeBytes;

            ThreadStat() {
                appendStartTime = 0L;
                appendEndTime = 0L;
                appendCount = 0;
                getRangeStartTime = 0L;
                getRangeEndTime = 0L;
                getRangeCount = 0;
                writeBytes = 0L;
                reported.set(false);
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
                return ret;
            }
        }

        ThreadStat[] oldStats;
        Long oldEndTime;
        ThreadStat[] stats;

        // ThreadLocal< HashMap<Integer, Long> >
        // report operation per second
        TestStat() {
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
        }

        void updateThreadId() {
            if (threadId.get() == null) {
                int thisNumOfThread = numOfThreads.getAndAdd(1);
                threadId.set(thisNumOfThread);
                log.info("init thread id : " + thisNumOfThread);
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
            stats[id].appendEndTime = System.nanoTime();
            stats[id].appendCount += 1;
            stats[id].writeBytes += data.capacity();
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
            appendThroughput /= getNumOfThreads;
            getRangeThroughput /= getNumOfThreads;
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
                curAppendThroughput /= getNumOfThreads;
                curGetRangeThroughput /= getNumOfThreads;
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

            log.info("appendStat   :"+appendStat);
            log.info("getRangeStat :"+getRangeStat);
            log.info("csvStat      :"+csvStat);

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

    private class DataFile {
        // public String dataFileName;
        public FileChannel dataFileChannel;
        // public AtomicLong atomicCurPosition;
        public Long curPosition;
        public int writeMetaLength;
        public int readMetaLength;
        ThreadLocal<ByteBuffer> threadLocalWriteMetaBuf;
        ThreadLocal<ByteBuffer> threadLocalReadMetaBuf;
        ThreadLocal<ByteBuffer> readTmp;
        Lock fileLock;
        public int minBufLength;
        public int curBufLength;
        public int minBufNum;
        public int curBufNum;
        public Condition writeAggCondition;

        DataFile(String dataFileName) {
            // atomicCurPosition = new AtomicLong(0);
            File dataFile = new File(dataFileName);
            curPosition = 0L;
            try {
                // FIXME: resource leak ??
                dataFileChannel = new RandomAccessFile(dataFile, "rw").getChannel();
            } catch (IOException ie) {
                ie.printStackTrace();
            }
            writeMetaLength = Integer.BYTES;
            readMetaLength = Integer.BYTES;
            threadLocalWriteMetaBuf = new ThreadLocal<>();
            threadLocalReadMetaBuf = new ThreadLocal<>();
            // writeMeta = ByteBuffer.allocate(Integer.BYTES);
            // readMeta = ByteBuffer.allocate(Integer.BYTES);
            // readTmp = ByteBuffer.allocate(Integer.BYTES+17408);
            fileLock = new ReentrantLock(mqConfig.fairLock);
            writeAggCondition = fileLock.newCondition();
            // 写聚合
            minBufLength = mqConfig.minBufLength; // 缓冲区长度 32KiB
            curBufLength = 0;
            minBufNum = mqConfig.minBufNum; // 缓冲在buf中的数据的个数
            curBufNum = 0; // 缓冲在buf中的数据的个数
        }

        // public long allocate(long size) {
        // return atomicCurPosition.getAndAdd(size);
        // }

        // public void write(ByteBuffer data, long position) {
        // try {
        // dataFileChannel.write(data, position);
        // } catch (IOException ie) {
        // ie.printStackTrace();
        // }
        // }

        public Long syncSeqWrite(ByteBuffer data) {
            fileLock.lock();
            if (threadLocalWriteMetaBuf.get() == null) {
                threadLocalWriteMetaBuf.set(ByteBuffer.allocate(writeMetaLength));
                // log.info(threadLocalWriteMetaBuf.get());
                // log.info(threadLocalWriteMetaBuf);
            }
            ByteBuffer writeMeta = threadLocalWriteMetaBuf.get();

            int datalength = data.remaining();
            // int datalength = data.capacity();
            log.debug(writeMeta);
            writeMeta.clear();
            log.debug(datalength);
            writeMeta.putInt(datalength);
            writeMeta.position(0);
            long position = curPosition;
            log.debug("position : " + position);
            int ret = 0;
            try {
                // ByteBuffer buf = ByteBuffer.allocate(data.remaining());
                ret += dataFileChannel.write(writeMeta, position);
                ret += dataFileChannel.write(data, position + writeMeta.capacity());
                dataFileChannel.force(true);
            } catch (IOException ie) {
                ie.printStackTrace();
            }
            log.debug("write size : " + ret);
            log.debug("data size : " + datalength);
            curPosition += ret;
            log.debug("update position to: " + curPosition);
            fileLock.unlock();
            return position;
        }

        public Long syncSeqWriteAgg(ByteBuffer data) {
            fileLock.lock();

            long position = 0;
            try {
                if (threadLocalWriteMetaBuf.get() == null) {
                    threadLocalWriteMetaBuf.set(ByteBuffer.allocate(writeMetaLength));
                    // log.info(threadLocalWriteMetaBuf.get());
                    // log.info(threadLocalWriteMetaBuf);
                }
                ByteBuffer writeMeta = threadLocalWriteMetaBuf.get();

                int datalength = data.remaining();
                // int datalength = data.capacity();
                log.debug(writeMeta);
                writeMeta.clear();
                log.debug(datalength);
                writeMeta.putInt(datalength);
                writeMeta.position(0);
                position = curPosition;
                log.debug("position : " + position);
                int ret = 0;
                // ByteBuffer buf = ByteBuffer.allocate(data.remaining());
                ret += dataFileChannel.write(writeMeta, position);
                ret += dataFileChannel.write(data, position + writeMeta.capacity());
                curBufLength += ret;
                curBufNum += 1;
                log.debug("write size : " + ret);
                log.debug("data size : " + datalength);
                curPosition += ret;
                log.debug("update position to: " + curPosition);

                // TODO: 条件调优
                if (curBufNum >= minBufNum || curBufLength >= minBufLength) {
                    if (curBufNum >= minBufNum){
                        log.debug("Write Aggregate by number of data!");
                    }
                    if (curBufLength >= minBufLength){
                        log.debug("Write Aggregate by length of buffer!");
                    }
                    dataFileChannel.force(true);
                    writeAggCondition.signalAll();
                    curBufLength = 0;
                    curBufNum = 0;
                } else {
                    try {
                        // writeAggCondition.wait(10, 0);
                        // writeAggCondition.await();
                        Boolean isTimeOut = !writeAggCondition.await(mqConfig.timeOutMS, TimeUnit.MILLISECONDS);
                        if (isTimeOut){
                            log.debug("Time Out !!");
                            dataFileChannel.force(true);
                            writeAggCondition.signalAll();
                            curBufLength = 0;
                            curBufNum = 0;
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    // catch (TimeoutException e){
                    //     log.info("Time Out !!");
                    //     System.out.println("Time out !");
                    // }
                }
            } catch (IOException ie) {
                ie.printStackTrace();
            } finally {
                fileLock.unlock();
            }

            return position;
        }

        public ByteBuffer read(long position) {
            if (threadLocalReadMetaBuf.get() == null) {
                threadLocalReadMetaBuf.set(ByteBuffer.allocate(readMetaLength));
            }
            ByteBuffer readMeta = threadLocalReadMetaBuf.get();

            log.debug("read from position : " + position);
            readMeta.clear();
            try {
                int ret;
                // dataFileChannel.read(tmp);
                ret = dataFileChannel.read(readMeta, position);
                readMeta.position(0);
                int dataLength = readMeta.getInt();
                ByteBuffer tmp = ByteBuffer.allocate(dataLength);
                ret = dataFileChannel.read(tmp, position + readMeta.capacity());
                log.debug(ret);
                return tmp;
            } catch (IOException ie) {
                ie.printStackTrace();
            }

            return null;
        }

        public void close() {
            try {
                dataFileChannel.close();
            } catch (IOException ie) {
                ie.printStackTrace();
            }
        }

    }

    private String metadataFileName;
    private FileChannel metadataFileChannel;
    private ArrayList<DataFile> dataFiles;
    private int numOfDataFiles;
    private TestStat testStat;
    // private ConcurrentHashMap<String, Integer> topic2queueid;
    // private ConcurrentHashMap<String, HashMap<int, > > topic2queueid;
    // private ConcurrentHashMap<String, Long> topic2queueid;

    public class MQQueue {
        public Long maxOffset = 0L;
        public HashMap<Long, Long> queueMap;

        MQQueue() {
            maxOffset = 0L;
            queueMap = new HashMap<>();
        }
    }

    public class MQTopic {
        public String topicName;
        public HashMap<Integer, MQQueue> topicMap;

        MQTopic(String name) {
            topicName = name;
            topicMap = new HashMap<Integer, MQQueue>();
        }
    }

    private ConcurrentHashMap<String, MQTopic> mqMap;
    // private ConcurrentHashMap<String, HashMap<Integer, HashMap<Integer, Long> > >
    // mqMap;
    // private HashMap<Integer, HashMap<Integer, Long> > queueId2offset2data; //
    // queueId + offset -> data offset in SSD
    // private HashMap<Integer, Long> // offset to datablock(position in SSD)

    /**
     * 写入一条信息； 返回的long值为offset，用于从这个topic+queueId中读取这条数据
     * offset要求topic+queueId维度内严格递增，即第一条消息offset必须是0，第二条必须是1，第三条必须是2，第一万条必须是9999。
     * 
     * @param topic   topic的值，总共有100个topic
     * @param queueId topic下队列的id，每个topic下不超过10000个
     * @param data    信息的内容，评测时会随机产生
     */
    Test1MessageQueue(String dbDirPath) {
        // log.setLevel(Level.DEBUG);
        log.setLevel(Level.INFO);
        log.info("mqConfig : ");
        log.info(mqConfig);
        // dbDirPath = /essd
        log.info("start init MessageQueue!!");
        mqMap = new ConcurrentHashMap<String, MQTopic>();
        metadataFileName = dbDirPath + "/meta";

        Boolean crash = false;
        // whether the MQ is recover from existed file/db ?
        File metadataFile = new File(metadataFileName);
        if (metadataFile.exists() && !metadataFile.isDirectory()) {
            crash = true;
        }

        // init datafile
        numOfDataFiles = mqConfig.numOfDataFiles;
        dataFiles = new ArrayList<>();
        for (int i = 0; i < numOfDataFiles; i++) {
            String dataFileName = dbDirPath + "/db" + i;
            log.info("Initializing datafile: " + dataFileName);
            dataFiles.add(new DataFile(dataFileName));
        }

        if (crash) {
            // recover from crash
            log.info("recover from crash");
            // TODO: recover !!
        } else {
            // create the new MQ
            log.info("create new MQ");
            try {
                // FIXME: resource leak ??
                metadataFileChannel = new RandomAccessFile(metadataFile, "rw").getChannel();
            } catch (IOException ie) {
                ie.printStackTrace();
            }
        }

        testStat = new TestStat();

        log.info("init ok!");
    }

    @Override
    protected void finalize() throws Throwable {
        metadataFileChannel.close();
        for (int i = 0; i < dataFiles.size(); i++) {
            dataFiles.get(i).close();
        }

    }

    public long append(String topic, int queueId, ByteBuffer data) {
        testStat.appendStart();
        MQTopic mqTopic;
        MQQueue q;
        if (!mqMap.containsKey(topic)) {
            mqTopic = new MQTopic(topic);
            mqMap.put(topic, mqTopic);
        } else {
            mqTopic = mqMap.get(topic);
        }

        if (!mqTopic.topicMap.containsKey(queueId)) {
            q = new MQQueue();
            mqTopic.topicMap.put(queueId, q);
        } else {
            q = mqTopic.topicMap.get(queueId);
        }

        int dataFileId = Math.floorMod(topic.hashCode(), numOfDataFiles);
        // log.info(dataFileId);
        if (dataFileId < 0) {
            log.info(dataFileId);
        }

        DataFile df = dataFiles.get(dataFileId);
        long position = 0;
        if (mqConfig.useWriteAgg){
            position = df.syncSeqWriteAgg(data);
        } else {
            position = df.syncSeqWrite(data);
        }
        q.queueMap.put(q.maxOffset, position);
        Long ret = q.maxOffset;
        q.maxOffset++;

        testStat.appendUpdateStat(topic, queueId, data);
        return ret;
    }

    /**
     * 读取某个范围内的信息； 返回值中的key为消息在Map中的偏移，从0开始，value为对应的写入data。读到结尾处没有新数据了，要求返回null。
     * 
     * @param topic    topic的值
     * @param queueId  topic下队列的id
     * @param offset   写入消息时返回的offset
     * @param fetchNum 读取消息个数，不超过100
     */
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        testStat.getRangeStart();
        testStat.getRangeUpdateStat(topic, queueId, offset, fetchNum);
        Map<Integer, ByteBuffer> ret = new HashMap<>();
        MQTopic mqTopic;
        MQQueue q;
        mqTopic = mqMap.get(topic);
        if (mqTopic == null) {
            return ret;
        }
        q = mqTopic.topicMap.get(queueId);
        if (q == null) {
            return ret;
        }
        long pos = 0;
        int dataFileId = Math.floorMod(topic.hashCode(), numOfDataFiles);
        DataFile df = dataFiles.get(dataFileId);

        for (int i = 0; i < fetchNum; i++) {
            if (q.queueMap.containsKey(offset + i)) {
                pos = q.queueMap.get(offset + i);
                ByteBuffer bbf = df.read(pos);
                if (bbf != null) {
                    bbf.position(0);
                    bbf.limit(bbf.capacity());
                    ret.put(i, bbf);
                }
            }
        }

        return ret;
    }
}
