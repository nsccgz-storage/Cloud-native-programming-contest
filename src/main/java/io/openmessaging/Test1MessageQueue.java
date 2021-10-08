package io.openmessaging;

import java.io.IOException;

import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.io.RandomAccessFile;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.ArrayDeque;
import java.util.ArrayList;

import org.apache.log4j.spi.LoggerFactory;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.lang.ThreadLocal;
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

// import org.jctools.queues.MpmcArrayQueue;

public class Test1MessageQueue extends MessageQueue {
    private static final Logger log = Logger.getLogger(Test1MessageQueue.class);
    private static class MQConfig {
        boolean useStats = true; // 实测，对性能影响不大，挺神奇
        // Level logLevel = Level.DEBUG;
        Level logLevel = Level.INFO;

        // // version 0: local SSD: 70 MiB/s   
        // int numOfDataFiles = 10;
        // int minBufNum = 20; // 无效
        // int minBufLength = 32768; // 无效
        // int timeOutMS = 500; // 无效
        // boolean fairLock = true;
        // boolean useWriteAgg = false; // 不使用写聚合
        // boolean useWriteAggDirect = false;

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

        // // version 3: test for online
        // int numOfDataFiles = 5;
        // int minBufNum = 3;
        // int minBufLength = 24576;
        // int timeOutMS = 200;
        // boolean fairLock = true;
        // boolean useWriteAgg = true; // 使用写聚合

        // version 4: test for online
        // int numOfDataFiles = 4;
        // int minBufNum = 4;
        // int minBufLength = 32768;
        // int timeOutMS = 10;
        // boolean fairLock = true;
        // boolean useWriteAgg = true; // 使用写聚合
        // boolean useWriteAggDirect = false; 
        // boolean useWriteAggHeap = false;

        // version 5: test for online
        // int numOfDataFiles = 4;
        // int minBufNum = 4;
        // int minBufLength = 32768;
        // int timeOutMS = 10;
        // boolean fairLock = true;
        // boolean useWriteAgg = false; // 使用写聚合
        // boolean useWriteAggDirect = true;

        // int numOfDataFiles = 4;
        // int minBufNum = 5;
        // int minBufLength = 32768;
        // int timeOutMS = 8;
        // boolean fairLock = true;
        // int writeMethod = 1; 
        // 0: no write agg
        // 1: write agg (best)
        // 2: write agg direct
        // 3. write agg heap
        // 4. syncSeqWritePushQueue

        // int numOfDataFiles = 4;
        // int minBufNum = 5;
        // int minBufLength = 32768;
        // int timeOutMS = 8;
        // boolean fairLock = true;
        // int writeMethod = 4; 
    
        int numOfDataFiles = 4;
        int minBufNum = 6;
        int minBufLength = 50*1024;
        boolean fairLock = true;
        int writeMethod = 12; 
 
        // int numOfDataFiles = 3;
        // int minBufNum = 9;
        // int minBufLength = 72*1024;
        // boolean fairLock = true;
        // int writeMethod = 12; 
 
        // version just for test
        // int numOfDataFiles = 4;
        // int minBufNum = 4;
        // int minBufLength = 32768;
        // int timeOutMS = 20;
        // boolean fairLock = true;
        // boolean useWriteAgg = true; // 使用写聚合


        int timeOutMS = 8;


        // int minBufLength = 32768;
        
        // int minBufLength = 24576;
        // int minBufLength = 16384;
        // boolean useWriteAgg = true;
        // boolean fairLock = false;
        // boolean useWriteAgg = false;
        @Override
        public String toString() {
            return String.format("useStats=%b | writeMethod=%d | numOfDataFiles=%d | minBufLength=%d | minBufNum=%d | timeOutMS=%d | ",useStats,writeMethod,numOfDataFiles,minBufLength,minBufNum,timeOutMS);
            // return String.format("useStats=%b | writeMethod=%d | numOfDataFiles=%d | minBufLength=%d | minBufNum=%d | timeOutMS=%d | 6,48KiB",useStats,writeMethod,numOfDataFiles,minBufLength,minBufNum,timeOutMS);
            // return String.format("useStats=%b | writeMethod=%d | numOfDataFiles=%d | minBufLength=%d | minBufNum=%d | timeOutMS=%d | 12,88KiB (64KiB if data > 16KiB)",useStats,writeMethod,numOfDataFiles,minBufLength,minBufNum,timeOutMS);
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
                hitHotDataCount = 0;
                hotDataAllocCount = 0;
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
        void hotDataAlloc(String topic, int queueId){
            int id = threadId.get();
            stats[id].hotDataAllocCount += 1;
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
            log.info("[Total Append Data Dist]" + totalWriteBucketReport);

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

                log.info("[Current Append Data Dist]" + curWriteBucketReport);

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

            log.info("appendStat   :"+appendStat);
            log.info("getRangeStat :"+getRangeStat);
            log.info("csvStat      :"+csvStat);
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

            // report hit hot data ratio
            String hotDataReport = "";
            for (int i = 0; i < getNumOfThreads; i++){
                hotDataReport += String.format("%.2f,",(double)(stats[i].hitHotDataCount)/stats[i].getRangeCount);
            }
            log.info("[hit hot data] : " + hotDataReport);

            String hotDataAllocReport = "";
            for (int i = 0; i < getNumOfThreads; i++){
                hotDataAllocReport += stats[i].hotDataAllocCount + ",";
            }
            log.info("[hot data alloc] : " + hotDataAllocReport);



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
        public MappedByteBuffer dataFileMappedBuffer;
        // public AtomicLong atomicCurPosition;
        public long curPosition;
        public long curMappedPosition;
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

        public ByteBuffer writeAggDirectBuffer;
        public ByteBuffer writeAggHeapBuffer;
        public int writeAggDirectBufferCapacity;
        public int writeAggHeapBufferCapacity;


        private class Writer {
            ByteBuffer data;
            Condition cv;
            int done;
            long position;
            Thread currentThread;
            Writer(ByteBuffer d, Condition v){
                data = d;
                cv = v;
                done = 0;
                position = 0L;
            }
            Writer(ByteBuffer d, Thread t){
                data = d;
                currentThread = t;
                done = 0;
                position = 0L;
            }
        }

        public class WriteStat{
            public int[] bucketBound;
            public int[] bucketCount;
            public int emptyQueueCount;
            public int exceedBufNumCount;
            public int exceedBufLengthCount;
            WriteStat(){
                bucketBound = new int[]{100, 512, 1024, 2*1024, 4*1024, 8*1024, 16*1024, 32*1024, 48*1024, 56*1024, 64*1024, 80*1024 , 96*1024, 112*1024, 128*1024};
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

        public Deque<Writer> writerQueue;
        public Lock writerQueueLock;
        public Condition writerQueueCondition;


        public int writerQueueBufferCapacity;
        public ThreadLocal<ByteBuffer> writerQueueLocalBuffer;
        public ThreadLocal<ByteBuffer> writerQueueLocalDirectBuffer;


        public ThreadLocal<Writer[]> localBatchWriters;
        public ByteBuffer commonWriteBuffer;

        public WriteStat writeStat;

        public Queue<Writer> writerConcurrentQueue;
        public Queue<Writer> writerConcurrentPriorityQueue;
        public Thread backgroundFlushThread;
        Object backgroundFlushNotify;



        public class WriterComparator implements Comparator<Writer> {
            @Override
            public int compare(Writer x, Writer y) {
                // Assume neither string is null. Real code should
                // probably be more robust
                // You could also just return x.length() - y.length(),
                // which would be more efficient.

                return x.data.remaining() - y.data.remaining();
                // 小顶堆

                // return y.data.remaining() - x.data.remaining();
                // 大顶堆
            }
        }

        DataFile(String dataFileName) {
            // atomicCurPosition = new AtomicLong(0);
            File dataFile = new File(dataFileName);
            curPosition = 0L;
            try {
                // FIXME: resource leak ??
                dataFileChannel = new RandomAccessFile(dataFile, "rw").getChannel();
                dataFileChannel.truncate(100L*1024L*1024L*1024L); // 100GiB
                curPosition = (1*1024L+512)*1024L*1024L;
                dataFileMappedBuffer = dataFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, (1*1024L+512)*1024L*1024L);
                curMappedPosition = 0L;
                dataFileChannel.force(true);
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
            
            writeAggDirectBufferCapacity = 1024*1024;
            writeAggDirectBuffer = ByteBuffer.allocateDirect(writeAggDirectBufferCapacity); // 1MiB 的direct buffer
            writeAggDirectBuffer.position(0);

            writeAggHeapBufferCapacity = 1024*1024;
            writeAggHeapBuffer = ByteBuffer.allocate(writeAggHeapBufferCapacity);
            writeAggDirectBuffer.position(0);

            writerQueue = new ArrayDeque<>();
            writerQueueLock = new ReentrantLock(true);
            // writerQueueLock = new ReentrantLock(false);
            writerQueueCondition = writerQueueLock.newCondition();

            writerQueueBufferCapacity = 128*1024;
            writerQueueLocalBuffer = new ThreadLocal<>();
            commonWriteBuffer = ByteBuffer.allocate(writerQueueBufferCapacity);
            commonWriteBuffer.clear();


            writerQueueLocalDirectBuffer = new ThreadLocal<>();

            localBatchWriters = new ThreadLocal<>();

            writeStat = new WriteStat();

            WriterComparator comp = new WriterComparator();
            writerConcurrentQueue = new ConcurrentLinkedQueue<>();
            // writerConcurrentQueue = new MpmcArrayQueue<>(50);
            writerConcurrentPriorityQueue = new PriorityBlockingQueue<>(50, comp);

            Runnable backgroundFlushRunnable = new Runnable()
            {
                public void run()
                {
                    try {
                        if (writerQueueLocalBuffer.get() == null){
                            writerQueueLocalBuffer.set(ByteBuffer.allocate(writerQueueBufferCapacity));
                        }
                        ByteBuffer writerBuffer = writerQueueLocalBuffer.get();
                        int bufLength = 0;
                        int maxBufLength = mqConfig.minBufLength;
                        int bufNum = 0;
                        int maxBufNum = mqConfig.minBufNum;
                        boolean continueMerge = true;
                        Writer[] batchWriters = new Writer[maxBufNum];

                        while (true)
                        {
                            bufLength = 0;
                            bufNum = 0;
                            continueMerge = true;
                            while (writerConcurrentQueue.isEmpty()){
                                log.debug("the queue is empty !");
                                LockSupport.park();
                            }
                            log.debug("wake up and the queue is not empty");

                            int dataLength = 0;
                            short metadataLength = Short.BYTES;
                            int writeLength = 0;
                            while ( continueMerge ){
                                Writer w = writerConcurrentQueue.poll();
                                dataLength = w.data.remaining();
                                writeLength = dataLength  + metadataLength;
                                w.position = bufLength;
                                batchWriters[bufNum] = w;

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
                                if (writerConcurrentQueue.isEmpty()){
                                    continueMerge = false;
                                    if (mqConfig.useStats){
                                        writeStat.incEmptyQueueCount();
                                    }
                                }
                            }
                            if (mqConfig.useStats){
                                writeStat.addSample(bufLength);
                            }
                            long writePosition = curPosition;
                            bufLength = bufLength + (4096 - bufLength % 4096);
                            //  对齐到4K
                            // assert (curPosition % 4096 == 0);
                            curPosition += bufLength;
                            log.debug("need to flush!");
                            writerBuffer.clear();
                            for (int i = 0; i < bufNum; i++){
                                batchWriters[i].position += writePosition;
                                writerBuffer.putShort((short)batchWriters[i].data.remaining());
                                writerBuffer.put(batchWriters[i].data);
                            }
                            writerBuffer.flip();
                            // writerBuffer.limit(bufLength);
                            dataFileChannel.write(writerBuffer, writePosition);
                            dataFileChannel.force(true);
                            // trueWriteEndTime = System.nanoTime();
                            log.debug("flush ok!");
                            log.debug("bufNum : " + bufNum);
                            for (int i = 0; i < bufNum; i++){
                                batchWriters[i].done = 1;
                                LockSupport.unpark(batchWriters[i].currentThread);
                                log.debug("wake up the thread : " + batchWriters[i].currentThread);
                            }
                        }
                    } catch (IOException ie){
                        ie.printStackTrace();
                    }
                }
            };

            Runnable backgroundPriorityFlushRunnable = new Runnable()
            {
                public void run()
                {
                    try {
                        if (writerQueueLocalBuffer.get() == null){
                            writerQueueLocalBuffer.set(ByteBuffer.allocate(writerQueueBufferCapacity));
                        }
                        ByteBuffer writerBuffer = writerQueueLocalBuffer.get();
                        int bufLength = 0;
                        int maxBufLength = mqConfig.minBufLength;
                        int bufNum = 0;
                        int maxBufNum = mqConfig.minBufNum;
                        boolean continueMerge = true;
                        Writer[] batchWriters = new Writer[maxBufNum];

                        while (true)
                        {
                            bufLength = 0;
                            bufNum = 0;
                            continueMerge = true;
                            while (writerConcurrentPriorityQueue.isEmpty()){
                                log.debug("the queue is empty !");
                                LockSupport.park();
                            }
                            log.debug("wake up and the queue is not empty");

                            int dataLength = 0;
                            short metadataLength = Short.BYTES;
                            int writeLength = 0;
                            while ( continueMerge ){
                                Writer w = writerConcurrentPriorityQueue.poll();
                                dataLength = w.data.remaining();
                                writeLength = dataLength  + metadataLength;
                                w.position = bufLength;
                                batchWriters[bufNum] = w;

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
                                if (writerConcurrentPriorityQueue.isEmpty()){
                                    continueMerge = false;
                                    if (mqConfig.useStats){
                                        writeStat.incEmptyQueueCount();
                                    }
                                }
                            }
                            if (mqConfig.useStats){
                                writeStat.addSample(bufLength);
                            }
                            long writePosition = curPosition;
                            bufLength = bufLength + (4096 - bufLength % 4096);
                            //  对齐到4K
                            // assert (curPosition % 4096 == 0);
                            curPosition += bufLength;
                            log.debug("need to flush!");
                            writerBuffer.clear();
                            for (int i = 0; i < bufNum; i++){
                                batchWriters[i].position += writePosition;
                                writerBuffer.putShort((short)batchWriters[i].data.remaining());
                                writerBuffer.put(batchWriters[i].data);
                            }
                            writerBuffer.flip();
                            // writerBuffer.limit(bufLength);
                            dataFileChannel.write(writerBuffer, writePosition);
                            dataFileChannel.force(true);
                            // trueWriteEndTime = System.nanoTime();
                            log.debug("flush ok!");
                            log.debug("bufNum : " + bufNum);
                            for (int i = 0; i < bufNum; i++){
                                batchWriters[i].done = 1;
                                LockSupport.unpark(batchWriters[i].currentThread);
                                log.debug("wake up the thread : " + batchWriters[i].currentThread);
                            }
                        }
                    } catch (IOException ie){
                        ie.printStackTrace();
                    }
                }
            };

            // backgroundFlushThread = new Thread(backgroundFlushRunnable);
            // backgroundFlushThread = new Thread(backgroundPriorityFlushRunnable);
            // backgroundFlushThread.start();
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
        public long asyncSeqWritePushQueueHeapBatchBuffer(ByteBuffer data){
            Writer w = new Writer(data, Thread.currentThread());
            // only for debug            
            // fileLock.lock();
            // writeAggCondition.await(1000, TimeUnit.MILLISECONDS);
            // fileLock.unlock();
            writerConcurrentPriorityQueue.offer(w);
            // writerConcurrentQueue.offer(w);
            LockSupport.unpark(backgroundFlushThread);
            while (w.done != 1){
                log.debug("waiting : " + Thread.currentThread());
                LockSupport.park(w.currentThread);
                log.debug("w.done : " + w.done);
            }
            return w.position;
        }
 
 

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

        public Long syncSeqWriteDirect(ByteBuffer data) {
            fileLock.lock();
            if (threadLocalWriteMetaBuf.get() == null) {
                threadLocalWriteMetaBuf.set(ByteBuffer.allocateDirect(writeMetaLength));
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
                    // threadLocalWriteMetaBuf.set(ByteBuffer.allocateDirect(writeMetaLength));
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

        public Long syncSeqWriteAggDirect(ByteBuffer data) {
            fileLock.lock();

            int ret = 0;
            Long position = 0L;
            try {
                int datalength = data.remaining();
                writeAggDirectBuffer.putInt(datalength).put(data);
                position = curPosition + curBufLength;
                curBufLength += Integer.BYTES+datalength;
                curBufNum += 1;

                // TODO: 条件调优
                if (curBufNum >= minBufNum || curBufLength >= minBufLength) {
                    if (curBufNum >= minBufNum){
                        log.debug("Write Aggregate by number of data!");
                    }
                    if (curBufLength >= minBufLength){
                        log.debug("Write Aggregate by length of buffer!");
                    }
                    writeAggDirectBuffer.position(0);
                    writeAggDirectBuffer.limit(curBufLength);
                    ret = dataFileChannel.write(writeAggDirectBuffer, curPosition);
                    dataFileChannel.force(true);
                    curPosition += ret;
                    writeAggDirectBuffer.position(0);
                    writeAggDirectBuffer.limit(writeAggDirectBufferCapacity);
                    curBufLength = 0;
                    curBufNum = 0;
                    writeAggCondition.signalAll();
                } else {
                    try {
                        Boolean isTimeOut = !writeAggCondition.await(mqConfig.timeOutMS, TimeUnit.MILLISECONDS);
                        if (isTimeOut){
                            log.debug("Time Out !!");
                            writeAggDirectBuffer.position(0);
                            writeAggDirectBuffer.limit(curBufLength);
                            ret = dataFileChannel.write(writeAggDirectBuffer, curPosition);
                            dataFileChannel.force(true);
                            curPosition += ret;
                            writeAggDirectBuffer.position(0);
                            writeAggDirectBuffer.limit(writeAggDirectBufferCapacity);
                            curBufLength = 0;
                            curBufNum = 0;
                            writeAggCondition.signalAll();
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

        public Long syncSeqWriteAggHeap(ByteBuffer data) {
            fileLock.lock();

            int ret = 0;
            Long position = 0L;
            try {
                int datalength = data.remaining();
                writeAggHeapBuffer.putInt(datalength).put(data);
                position = curPosition + curBufLength;
                curBufLength += Integer.BYTES+datalength;
                curBufNum += 1;

                // TODO: 条件调优
                if (curBufNum >= minBufNum || curBufLength >= minBufLength) {
                    if (curBufNum >= minBufNum){
                        log.debug("Write Aggregate by number of data!");
                    }
                    if (curBufLength >= minBufLength){
                        log.debug("Write Aggregate by length of buffer!");
                    }
                    writeAggHeapBuffer.position(0);
                    writeAggHeapBuffer.limit(curBufLength);
                    ret = dataFileChannel.write(writeAggHeapBuffer, curPosition);
                    dataFileChannel.force(true);
                    curPosition += ret;
                    writeAggHeapBuffer.position(0);
                    writeAggHeapBuffer.limit(writeAggHeapBufferCapacity);
                    curBufLength = 0;
                    curBufNum = 0;
                    writeAggCondition.signalAll();
                } else {
                    try {
                        Boolean isTimeOut = !writeAggCondition.await(mqConfig.timeOutMS, TimeUnit.MILLISECONDS);
                        if (isTimeOut){
                            log.debug("Time Out !!");
                            writeAggHeapBuffer.position(0);
                            writeAggHeapBuffer.limit(curBufLength);
                            ret = dataFileChannel.write(writeAggHeapBuffer, curPosition);
                            dataFileChannel.force(true);
                            curPosition += ret;
                            writeAggHeapBuffer.position(0);
                            writeAggHeapBuffer.limit(writeAggHeapBufferCapacity);
                            curBufLength = 0;
                            curBufNum = 0;
                            writeAggCondition.signalAll();
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

        public long syncSeqWritePushQueue(ByteBuffer data){
            if (threadLocalWriteMetaBuf.get() == null) {
                threadLocalWriteMetaBuf.set(ByteBuffer.allocate(writeMetaLength));
            }

            ByteBuffer writeMeta = threadLocalWriteMetaBuf.get();

            long position = 0L;
            try {
                Writer w = new Writer(data, writerQueueCondition);
                writerQueueLock.lock();
                // only for debug
                // fileLock.lock();
                // writeAggCondition.await(1000, TimeUnit.MILLISECONDS);
                // fileLock.unlock();
 
                log.debug("try to new a writer to queue");
                writerQueue.addLast(w);
                log.debug(writerQueue);
                log.debug(writerQueue.getFirst());
                while (!(w.done == 1 || w.equals(writerQueue.getFirst()) )){
                    log.debug("wait for the leader of queue");
                    w.cv.await();
                }
                if (w.done == 1){
                    log.debug(w.position);
                    return w.position;
                }
                log.debug("I am the head");
                
                // TODO: 调参
                int bufLength = 0;
                int maxBufLength = mqConfig.minBufLength; // 36 KiB
                // if (w.data.remaining() < 1024){
                //     maxBufLength = 32*1024;
                // }
                // if (w.data.remaining() > 16*1024){
                //     maxBufLength = 64*1024;
                // }
                int bufNum = 0;
                int maxBufNum = mqConfig.minBufNum;
                boolean continueMerge = true;
                // I am the head of the queue and need to write buffer to SSD
                // build write batch
                Iterator<Writer> iter = writerQueue.iterator();

                int ret = 0;
                position = curPosition;
                Writer lastWriter = null;
                int metadataLength = Integer.BYTES;
                while (continueMerge ){
                    lastWriter = iter.next();
                    int dataLength = lastWriter.data.remaining();
                    int writeLength =  metadataLength + dataLength;
                    log.debug(lastWriter);
                    writeMeta.position(0);
                    writeMeta.putInt(dataLength);
                    log.debug("write to position : " + position);
                    writeMeta.position(0);
                    lastWriter.position = position;
                    ret = dataFileChannel.write(writeMeta, position);
                    position += ret;
                    log.debug("write meta size : "+ret);
                    ret = dataFileChannel.write(lastWriter.data, position);
                    position += ret;
                    log.debug("write data size : "+ret);

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
                if (mqConfig.useStats){
                    writeStat.addSample(bufLength);
                }

                curPosition = position;
                {
                    log.debug("need to flush, unlock !");
                    writerQueueLock.unlock();
                    dataFileChannel.force(true);
                    writerQueueLock.lock();
                    log.debug("flush ok , get the lock again!");
                }

                while(true){
                    Writer ready = writerQueue.removeFirst();
                    if (!ready.equals(w)){
                        ready.done = 1;
                        ready.cv.signal();
                    }
                    if (ready.equals(lastWriter)){
                        break;
                    }
                }

                if (!writerQueue.isEmpty()){
                    writerQueue.getFirst().cv.signal();
                }
                log.debug(w.position);
                position = w.position;

            } catch (IOException ie) {
                ie.printStackTrace();
            } catch (InterruptedException ie){
                ie.printStackTrace();
            } finally {
                writerQueueLock.unlock();
            }
            return position;

        }


        public long syncSeqWritePushQueueDirectBuffer(ByteBuffer data){
            if (writerQueueLocalBuffer.get() == null){
                writerQueueLocalBuffer.set(ByteBuffer.allocateDirect(writerQueueBufferCapacity));
            }
            ByteBuffer writerBuffer = writerQueueLocalBuffer.get();

            long position = 0L;
            try {
                Writer w = new Writer(data, writerQueueCondition);
                writerQueueLock.lock();
                // only for debug
                // fileLock.lock();
                // writeAggCondition.await(1000, TimeUnit.MILLISECONDS);
                // fileLock.unlock();
 
                log.debug("try to new a writer to queue");
                writerQueue.addLast(w);
                log.debug(writerQueue);
                log.debug(writerQueue.getFirst());
                while (!(w.done == 1 || w.equals(writerQueue.getFirst()) )){
                    log.debug("wait for the leader of queue");
                    w.cv.await();
                }
                if (w.done == 1){
                    log.debug(w.position);
                    return w.position;
                }
                log.debug("I am the head");
                
                // TODO: 调参
                int bufLength = 0;
                int maxBufLength = mqConfig.minBufLength; // 36 KiB
                // if (w.data.remaining() < 1024){
                //     maxBufLength = 32*1024;
                // }
                // if (w.data.remaining() > 16*1024){
                //     maxBufLength = 64*1024;
                // }
                int bufNum = 0;
                int maxBufNum = mqConfig.minBufNum;
                boolean continueMerge = true;
                // I am the head of the queue and need to write buffer to SSD
                // build write batch
                Iterator<Writer> iter = writerQueue.iterator();

                int metadataLength = Integer.BYTES;
                int dataLength = 0;
                int writeLength = 0;
                position = curPosition;
                writerBuffer.position(0);
                writerBuffer.limit(writerBuffer.capacity());
                Writer lastWriter = null;
                while ( continueMerge ){
                    lastWriter = iter.next();
                    dataLength = lastWriter.data.remaining();
                    writeLength = metadataLength + dataLength;
                    log.debug(lastWriter);
                    writerBuffer.putInt(dataLength);
                    writerBuffer.put(lastWriter.data);
                    lastWriter.position = position;
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
                curPosition += bufLength;
                if (mqConfig.useStats){
                    writeStat.addSample(bufLength);
                }
                {
                    log.debug("need to flush, unlock !");
                    writerQueueLock.unlock();
                    writerBuffer.position(0);
                    writerBuffer.limit(bufLength);
                    dataFileChannel.write(writerBuffer, writePosition);
                    dataFileChannel.force(true);
                    writerQueueLock.lock();
                    log.debug("flush ok , get the lock again!");
                }

                while(true){
                    Writer ready = writerQueue.removeFirst();
                    if (!ready.equals(w)){
                        ready.done = 1;
                        ready.cv.signal();
                    }
                    if (ready.equals(lastWriter)){
                        break;
                    }
                }

                if (!writerQueue.isEmpty()){
                    writerQueue.getFirst().cv.signal();
                }
                log.debug(w.position);
                position = w.position;

            } catch (IOException ie) {
                ie.printStackTrace();
            } catch (InterruptedException ie){
                ie.printStackTrace();
            } finally {
                writerQueueLock.unlock();
            }
            return position;

        }

        public long syncSeqWritePushQueueHeapBuffer(ByteBuffer data){
            if (writerQueueLocalBuffer.get() == null){
                writerQueueLocalBuffer.set(ByteBuffer.allocate(writerQueueBufferCapacity));
            }
            ByteBuffer writerBuffer = writerQueueLocalBuffer.get();

            long position = 0L;
            try {
                Writer w = new Writer(data, writerQueueCondition);
                writerQueueLock.lock();
                // only for debug
                // fileLock.lock();
                // writeAggCondition.await(1000, TimeUnit.MILLISECONDS);
                // fileLock.unlock();
 
                // log.debug("try to new a writer to queue");
                writerQueue.addLast(w);
                // log.debug(writerQueue);
                // log.debug(writerQueue.getFirst());
                while (!(w.done == 1 || w.equals(writerQueue.getFirst()) )){
                    // log.debug("wait for the leader of queue");
                    w.cv.await();
                }
                if (w.done == 1){
                    // log.debug(w.position);
                    return w.position;
                }
                log.debug("I am the head");
                
                // TODO: 调参
                int bufLength = 0;
                int maxBufLength = mqConfig.minBufLength; // 36 KiB
                // if (w.data.remaining() < 1024){
                //     maxBufLength = 32*1024;
                // }
                // if (w.data.remaining() > 16*1024){
                //     maxBufLength = 64*1024;
                // }
                int bufNum = 0;
                int maxBufNum = mqConfig.minBufNum;

                boolean continueMerge = true;
                // I am the head of the queue and need to write buffer to SSD
                // build write batch
                Iterator<Writer> iter = writerQueue.iterator();

                int metadataLength = Integer.BYTES;
                int dataLength = 0;
                int writeLength = 0;
                position = curPosition;
                writerBuffer.position(0);
                writerBuffer.limit(writerBuffer.capacity());
                Writer lastWriter = null;
                while ( iter.hasNext() && continueMerge ){
                    lastWriter = iter.next();
                    dataLength = lastWriter.data.remaining();
                    writeLength = metadataLength + dataLength;
                    // log.debug(lastWriter);
                    writerBuffer.putInt(dataLength);
                    // log.debug(lastWriter.data);
                    // int oriPos = lastWriter.data.position();
                    writerBuffer.put(lastWriter.data);
                    // log.debug(lastWriter.data);
                    // lastWriter.data.position(oriPos);
                    // log.debug(lastWriter.data);
                    lastWriter.position = position;
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
                if (mqConfig.useStats){
                    writeStat.addSample(bufLength);
                }

                long writePosition = curPosition;
                curPosition += bufLength;
                {
                    // log.debug("need to flush, unlock !");
                    writerQueueLock.unlock();
                    writerBuffer.position(0);
                    writerBuffer.limit(bufLength);
                    dataFileChannel.write(writerBuffer, writePosition);
                    dataFileChannel.force(true);
                    writerQueueLock.lock();
                    // log.debug("flush ok , get the lock again!");
                }

                while(true){
                    Writer ready = writerQueue.removeFirst();
                    if (!ready.equals(w)){
                        ready.done = 1;
                        ready.cv.signal();
                    }
                    if (ready.equals(lastWriter)){
                        break;
                    }
                }

                if (!writerQueue.isEmpty()){
                    writerQueue.getFirst().cv.signal();
                }
                // log.debug(w.position);
                position = w.position;

            } catch (IOException ie) {
                ie.printStackTrace();
            } catch (InterruptedException ie){
                ie.printStackTrace();
            } finally {
                writerQueueLock.unlock();
            }
            return position;

        }
    
        public long syncSeqWritePushQueueBatch(ByteBuffer data){
            if (threadLocalWriteMetaBuf.get() == null) {
                threadLocalWriteMetaBuf.set(ByteBuffer.allocate(writeMetaLength));
            }
            if (localBatchWriters.get() == null){
                localBatchWriters.set(new Writer[20]);
            }

            Writer[] batchWriters = localBatchWriters.get();
            ByteBuffer writeMeta = threadLocalWriteMetaBuf.get();

            long position = 0L;
            try {
                Writer w = new Writer(data, writerQueueCondition);
                writerQueueLock.lock();
                // only for debug
                // fileLock.lock();
                // writeAggCondition.await(1000, TimeUnit.MILLISECONDS);
                // fileLock.unlock();
 
                log.debug("try to new a writer to queue");
                writerQueue.addLast(w);
                log.debug(writerQueue);
                log.debug(writerQueue.getFirst());
                while (!(w.done == 1 || w.equals(writerQueue.getFirst()) )){
                    log.debug("wait for the leader of queue");
                    w.cv.await();
                }
                if (w.done == 1){
                    log.debug(w.position);
                    return w.position;
                }
                log.debug("I am the head");
                
                // TODO: 调参
                int bufLength = 0;
                int maxBufLength = mqConfig.minBufLength; // 36 KiB
                // if (w.data.remaining() < 1024){
                //     maxBufLength = 32*1024;
                // }
                // if (w.data.remaining() > 16*1024){
                //     maxBufLength = 64*1024;
                // }
                int bufNum = 0;
                int maxBufNum = mqConfig.minBufNum;
                boolean continueMerge = true;
                // I am the head of the queue and need to write buffer to SSD
                // build write batch
                Iterator<Writer> iter = writerQueue.iterator();
                Writer lastWriter = null;
                int dataLength = 0;
                int metadataLength = Integer.BYTES;
                int writeLength = 0;
                position = curPosition;
                while (continueMerge ){
                    lastWriter = iter.next();
                    dataLength = lastWriter.data.remaining();
                    writeLength =  metadataLength + dataLength;
                    // log.debug(lastWriter);
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
                if (mqConfig.useStats){
                    writeStat.addSample(bufLength);
                }

                curPosition = position;
                {
                    log.debug("need to flush, unlock !");
                    writerQueueLock.unlock();
                    for (int i = 0; i < bufNum; i++){
                        writeMeta.clear();
                        writeMeta.putInt(batchWriters[i].data.remaining());
                        writeMeta.flip();
                        dataFileChannel.write(writeMeta, batchWriters[i].position);
                        dataFileChannel.write(batchWriters[i].data, batchWriters[i].position+metadataLength);
                    }
                    dataFileChannel.force(true);
                    writerQueueLock.lock();
                    log.debug("flush ok , get the lock again!");
                }

                while(true){
                    Writer ready = writerQueue.removeFirst();
                    if (!ready.equals(w)){
                        ready.done = 1;
                        ready.cv.signal();
                    }
                    if (ready.equals(lastWriter)){
                        break;
                    }
                }

                if (!writerQueue.isEmpty()){
                    writerQueue.getFirst().cv.signal();
                }
                log.debug(w.position);
                position = w.position;

            } catch (IOException ie) {
                ie.printStackTrace();
            } catch (InterruptedException ie){
                ie.printStackTrace();
            } finally {
                writerQueueLock.unlock();
            }
            return position;

        }

        public long syncSeqWritePushQueueDirectBatchBuffer(ByteBuffer data){
            if (writerQueueLocalBuffer.get() == null){
                writerQueueLocalBuffer.set(ByteBuffer.allocateDirect(writerQueueBufferCapacity));
            }
            // if (localBatchWriters.get() == null){
            //     localBatchWriters.set(new Writer[20]);
            // }

            // Writer[] batchWriters = localBatchWriters.get();


            ByteBuffer writerBuffer = writerQueueLocalBuffer.get();

            long position = 0L;
            try {
                Writer w = new Writer(data, writerQueueCondition);
                writerQueueLock.lock();
                // only for debug
                // fileLock.lock();
                // writeAggCondition.await(1000, TimeUnit.MILLISECONDS);
                // fileLock.unlock();
 
                log.debug("try to add a new writer to queue");
                writerQueue.addLast(w);
                log.debug(writerQueue);
                // log.debug(writerQueue.getFirst());
                while (!(w.done == 1 || w.equals(writerQueue.getFirst()) )){
                    // log.debug("wait for the leader of queue");
                    w.cv.await();
                }
                if (w.done == 1){
                    // log.debug(w.position);
                    return w.position;
                }
                // log.debug("I am the head");
                
                // TODO: 调参
                int bufLength = 0;
                int maxBufLength = mqConfig.minBufLength; // 36 KiB
                // if (w.data.remaining() < 1024){
                //     maxBufLength = 32*1024;
                // }
                // if (w.data.remaining() > 16*1024){
                //     maxBufLength = 64*1024;
                // }
                int bufNum = 0;
                int maxBufNum = mqConfig.minBufNum;

                Writer[] batchWriters = new Writer[maxBufNum];

                boolean continueMerge = true;
                // I am the head of the queue and need to write buffer to SSD
                // build write batch
                Iterator<Writer> iter = writerQueue.iterator();
                Writer lastWriter = null;
                int dataLength = 0;
                int metadataLength = Integer.BYTES;
                int writeLength = 0;

                position = curPosition;
                while ( continueMerge ){
                    lastWriter = iter.next();
                    dataLength = lastWriter.data.remaining();
                    writeLength = metadataLength + dataLength;
                    // log.debug(lastWriter);
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
                curPosition += bufLength;
                if (mqConfig.useStats){
                    writeStat.addSample(bufLength);
                }
                {
                    // log.debug("need to flush, unlock !");
                    writerQueueLock.unlock();
                    writerBuffer.position(0);
                    writerBuffer.limit(writerBuffer.capacity());
                    for (int i = 0; i < bufNum; i++){
                        writerBuffer.putInt(batchWriters[i].data.remaining());
                        writerBuffer.put(batchWriters[i].data);
                    }
                    writerBuffer.position(0);
                    writerBuffer.limit(bufLength);
                    dataFileChannel.write(writerBuffer, writePosition);
                    dataFileChannel.force(true);
                    writerQueueLock.lock();
                    // log.debug("flush ok , get the lock again!");
                }

                while(true){
                    Writer ready = writerQueue.removeFirst();
                    if (!ready.equals(w)){
                        ready.done = 1;
                        ready.cv.signal();
                    }
                    if (ready.equals(lastWriter)){
                        break;
                    }
                }

                if (!writerQueue.isEmpty()){
                    writerQueue.getFirst().cv.signal();
                }
                // log.debug(w.position);
                position = w.position;

            } catch (IOException ie) {
                ie.printStackTrace();
            } catch (InterruptedException ie){
                ie.printStackTrace();
            } finally {
                writerQueueLock.unlock();
            }
            return position;

        }
        public long syncSeqWritePushQueueHeapBatchBuffer(ByteBuffer data){
            // long trueWriteStartTime = System.nanoTime();
            // long trueWriteEndTime = System.nanoTime();
            // long getLockTime = System.nanoTime();
            // long startTime = System.nanoTime();
            // long beforeGetLockTime = 0L;
            // long writeTime = 0L;
            // long addQueueTime = 0L;
            if (writerQueueLocalBuffer.get() == null){
                writerQueueLocalBuffer.set(ByteBuffer.allocate(writerQueueBufferCapacity));
            }
            // if (localBatchWriters.get() == null){
            //     localBatchWriters.set(new Writer[20]);
            // }

            // Writer[] batchWriters = localBatchWriters.get();


            ByteBuffer writerBuffer = writerQueueLocalBuffer.get();

            long position = 0L;
            try {
                Writer w = new Writer(data, writerQueueCondition);
                // while (!writerQueueLock.tryLock(2, TimeUnit.MILLISECONDS)){}
                // beforeGetLockTime = System.nanoTime();
                writerQueueLock.lock();
                // getLockTime = System.nanoTime();
                // only for debug
                // fileLock.lock();
                // writeAggCondition.await(1000, TimeUnit.MILLISECONDS);
                // fileLock.unlock();
 
                // log.debug("try to add a new writer to queue");
                writerQueue.add(w);
                // log.debug(writerQueue);
                // log.debug(writerQueue.getFirst());
                // addQueueTime = System.nanoTime();
                while (!(w.done == 1 || w.equals(writerQueue.peek()) )){
                    // log.debug("wait for the leader of queue");
                    w.cv.await();
                }
                if (w.done == 1){
                    // log.debug(w.position);
                    return w.position;
                }
                // log.debug("I am the head");
                // writeTime = System.nanoTime();
                
                // TODO: 调参
                int bufLength = 0;
                int maxBufLength = mqConfig.minBufLength; // 36 KiB
                // if (w.data.remaining() < 1024){
                //     maxBufLength = 32*1024;
                // }
                // if (w.data.remaining() > 16*1024){
                //     maxBufLength = 64*1024;
                // }
                int bufNum = 0;
                int maxBufNum = mqConfig.minBufNum;


                boolean continueMerge = true;
                Writer[] batchWriters = new Writer[maxBufNum];
                // I am the head of the queue and need to write buffer to SSD
                // build write batch
                Iterator<Writer> iter = writerQueue.iterator();
                Writer lastWriter = null;
                int dataLength = 0;
                short metadataLength = Short.BYTES;
                int writeLength = 0;

                position = curPosition;
                while ( continueMerge ){
                    lastWriter = iter.next();
                    dataLength = lastWriter.data.remaining();
                    writeLength = metadataLength + dataLength;
                    // log.debug(lastWriter);
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
                    // log.debug("need to flush, unlock !");
                    writerQueueLock.unlock();
                    // trueWriteStartTime = System.nanoTime();
                    writerBuffer.position(0);
                    writerBuffer.limit(writerBuffer.capacity());
                    for (int i = 0; i < bufNum; i++){
                        // writerBuffer.putInt(batchWriters[i].data.remaining());
                        writerBuffer.putShort((short)batchWriters[i].data.remaining());
                        writerBuffer.put(batchWriters[i].data);
                    }
                    writerBuffer.position(0);
                    writerBuffer.limit(bufLength);
                    dataFileChannel.write(writerBuffer, writePosition);
                    dataFileChannel.force(true);
                    // trueWriteEndTime = System.nanoTime();
                    writerQueueLock.lock();
                    // log.debug("flush ok , get the lock again!");
                }

                while(true){
                    Writer ready = writerQueue.poll();
                    if (!ready.equals(w)){
                        ready.done = 1;
                        ready.cv.signal();
                    }
                    if (ready.equals(lastWriter)){
                        break;
                    }
                }

                if (!writerQueue.isEmpty()){
                    writerQueue.getFirst().cv.signal();
                }
                // log.debug(w.position);
                position = w.position;

            } catch (IOException ie) {
                ie.printStackTrace();
            } catch (InterruptedException ie){
                ie.printStackTrace();
            } finally {
                long endTime = System.nanoTime();
                // log.info("latency before get lock (ms) : " + (double)(beforeGetLockTime - startTime)/(1000*1000) );
                // log.info("get lock then add queue (ms) : " + (double)(addQueueTime - getLockTime)/(1000*1000) );
                // log.info("latency in write (ns) : " + (double)(endTime-startTime)/(1000*1000) );
                // log.info("latency in lock (ns) : " + (double)(endTime-getLockTime)/(1000*1000) );
                // log.info("wait lock (ns) : " + (double)(getLockTime-startTime)/(1000*1000));
                // if (writeTime != 0L){
                //     // log.info("write time (ns) : " + (double)(endTime-writeTime)/(1000*1000));
                //     log.info("true write time (ns) : " + (double)(trueWriteEndTime-trueWriteStartTime)/(1000*1000));
                // }
                writerQueueLock.unlock();
            }
            return position;

        }

        public long syncSeqWritePushConcurrentQueueHeapBatchBuffer(ByteBuffer data){
            // if (writerQueueLocalBuffer.get() == null){
            //     writerQueueLocalBuffer.set(ByteBuffer.allocate(writerQueueBufferCapacity));
            // }

            ByteBuffer writerBuffer = commonWriteBuffer;

            long position = 0L;
            try {
                Writer w = new Writer(data, Thread.currentThread());
                // log.debug("try to add a new writer to queue");
                writerConcurrentQueue.offer(w);
                // writerConcurrentQueue.add(w);
                // log.debug(writerQueue);
                // log.debug(writerQueue.getFirst());
                // addQueueTime = System.nanoTime();
                while (!(w.done == 1 || w.equals(writerConcurrentQueue.peek()) )){
                    // log.debug("wait for the leader of queue");
                    LockSupport.park();
                }
                if (w.done == 1){
                    // log.debug(w.position);
                    return w.position;
                }
                // log.debug("I am the head");
                // writeTime = System.nanoTime();
                
                // TODO: 调参
                int bufLength = 0;
                int maxBufLength = mqConfig.minBufLength; // 36 KiB
                // if (w.data.remaining() < 1024){
                //     maxBufLength = 32*1024;
                // }
                // if (w.data.remaining() > 16*1024){
                //     maxBufLength = 64*1024;
                // }
                int bufNum = 0;
                int maxBufNum = mqConfig.minBufNum;


                boolean continueMerge = true;
                Writer[] batchWriters = new Writer[maxBufNum];
                // I am the head of the queue and need to write buffer to SSD
                // build write batch
                Iterator<Writer> iter = writerConcurrentQueue.iterator();
                Writer lastWriter = null;
                int dataLength = 0;
                short metadataLength = Short.BYTES;
                int writeLength = 0;

                position = curPosition;
                while ( continueMerge ){
                    lastWriter = iter.next();
                    dataLength = lastWriter.data.remaining();
                    writeLength = metadataLength + dataLength;
                    // log.debug(lastWriter);
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
                    // log.debug("need to flush, unlock !");
                    // trueWriteStartTime = System.nanoTime();
                    writerBuffer.clear();
                    for (int i = 0; i < bufNum; i++){
                        // writerBuffer.putInt(batchWriters[i].data.remaining());
                        writerBuffer.putShort((short)batchWriters[i].data.remaining());
                        writerBuffer.put(batchWriters[i].data);
                    }
                    writerBuffer.flip();
                    dataFileChannel.write(writerBuffer, writePosition);
                    dataFileChannel.force(true);
                    // trueWriteEndTime = System.nanoTime();
                    // log.debug("flush ok , get the lock again!");
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
                // log.debug(w.position);
                position = w.position;

            } catch (IOException ie) {
                ie.printStackTrace();
            } finally {
                // long endTime = System.nanoTime();
                // log.info("latency before get lock (ms) : " + (double)(beforeGetLockTime - startTime)/(1000*1000) );
                // log.info("get lock then add queue (ms) : " + (double)(addQueueTime - getLockTime)/(1000*1000) );
                // log.info("latency in write (ns) : " + (double)(endTime-startTime)/(1000*1000) );
                // log.info("latency in lock (ns) : " + (double)(endTime-getLockTime)/(1000*1000) );
                // log.info("wait lock (ns) : " + (double)(getLockTime-startTime)/(1000*1000));
                // if (writeTime != 0L){
                //     // log.info("write time (ns) : " + (double)(endTime-writeTime)/(1000*1000));
                //     log.info("true write time (ns) : " + (double)(trueWriteEndTime-trueWriteStartTime)/(1000*1000));
                // }
            }
            return position;

        }


        public long syncSeqWritePushQueueMapped(ByteBuffer data){
            // long trueWriteStartTime = System.nanoTime();
            // long trueWriteEndTime = System.nanoTime();
            // long getLockTime = System.nanoTime();
            // long startTime = System.nanoTime();
            // long beforeGetLockTime = 0L;
            // long writeTime = 0L;
            // long addQueueTime = 0L;
            if (writerQueueLocalBuffer.get() == null){
                writerQueueLocalBuffer.set(ByteBuffer.allocate(writerQueueBufferCapacity));
            }
            if (writerQueueLocalDirectBuffer.get() == null){
                writerQueueLocalDirectBuffer.set(ByteBuffer.allocateDirect(4096));
            }

            // if (localBatchWriters.get() == null){
            //     localBatchWriters.set(new Writer[20]);
            // }

            // Writer[] batchWriters = localBatchWriters.get();


            ByteBuffer writerBuffer = writerQueueLocalBuffer.get();
            ByteBuffer writerDirectBuffer = writerQueueLocalDirectBuffer.get();

            long position = 0L;
            try {
                Writer w = new Writer(data, writerQueueCondition);
                // while (!writerQueueLock.tryLock(2, TimeUnit.MILLISECONDS)){}
                // beforeGetLockTime = System.nanoTime();
                writerQueueLock.lock();
                // getLockTime = System.nanoTime();
                // only for debug
                // fileLock.lock();
                // writeAggCondition.await(1000, TimeUnit.MILLISECONDS);
                // fileLock.unlock();
 
                // log.debug("try to add a new writer to queue");
                writerQueue.add(w);
                // log.debug(writerQueue);
                // log.debug(writerQueue.getFirst());
                // addQueueTime = System.nanoTime();
                while (!(w.done == 1 || w.equals(writerQueue.peek()) )){
                    // log.debug("wait for the leader of queue");
                    w.cv.await();
                }
                if (w.done == 1){
                    // log.debug(w.position);
                    return w.position;
                }
                // log.debug("I am the head");
                // writeTime = System.nanoTime();
                
                // TODO: 调参
                int bufLength = 0;
                int maxBufLength = mqConfig.minBufLength; // 36 KiB
                // if (w.data.remaining() < 1024){
                //     maxBufLength = 32*1024;
                // }
                // if (w.data.remaining() > 16*1024){
                //     maxBufLength = 64*1024;
                // }
                int bufNum = 0;
                int maxBufNum = mqConfig.minBufNum;


                boolean continueMerge = true;
                Writer[] batchWriters = new Writer[maxBufNum];
                // I am the head of the queue and need to write buffer to SSD
                // build write batch
                Iterator<Writer> iter = writerQueue.iterator();
                Writer lastWriter = null;
                int dataLength = 0;
                short metadataLength = Short.BYTES;
                int writeLength = 0;

                position = curPosition;
                while ( continueMerge ){
                    lastWriter = iter.next();
                    dataLength = lastWriter.data.remaining();
                    writeLength = metadataLength + dataLength;
                    // log.debug(lastWriter);
                    lastWriter.position = bufLength;
                    batchWriters[bufNum] = lastWriter;
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

                if (mqConfig.useStats){
                    writeStat.addSample(bufLength);
                }

                if (bufLength >= 4096){
                    long writePosition = curPosition;
                    bufLength = bufLength + (4096 - bufLength % 4096);
                    //  对齐到4K
                    // assert (curPosition % 4096 == 0);
                    curPosition += bufLength;
                    {
                        // log.debug("need to flush, unlock !");
                        writerQueueLock.unlock();
                        // trueWriteStartTime = System.nanoTime();
                        writerBuffer.clear();
                        for (int i = 0; i < bufNum; i++){
                            batchWriters[i].position += writePosition;
                            // writerBuffer.putInt(batchWriters[i].data.remaining());
                            writerBuffer.putShort((short)batchWriters[i].data.remaining());
                            writerBuffer.put(batchWriters[i].data);
                        }
                        writerBuffer.flip();
                        // writerBuffer.position(0);
                        // writerBuffer.limit(bufLength);
                        dataFileChannel.write(writerBuffer, writePosition);
                        dataFileChannel.force(true);
                        // trueWriteEndTime = System.nanoTime();
                        writerQueueLock.lock();
                        // log.debug("flush ok , get the lock again!");
                    }
                } else {
                    long writePosition = curMappedPosition;
                    curMappedPosition += bufLength;
                    assert (curMappedPosition >= (1024+512)*1024L*1024L);
                    {
                        writerQueueLock.unlock();
                        writerDirectBuffer.clear();
                        for (int i = 0; i < bufNum; i++){
                            batchWriters[i].position += writePosition;
                            writerDirectBuffer.putShort((short)batchWriters[i].data.remaining());
                            writerDirectBuffer.put(batchWriters[i].data);
                        }
                        writerDirectBuffer.flip();
                        dataFileMappedBuffer.put(writerDirectBuffer);
                        dataFileMappedBuffer.force();
                        writerQueueLock.lock();
                    }

                }


                while(true){
                    Writer ready = writerQueue.poll();
                    if (!ready.equals(w)){
                        ready.done = 1;
                        ready.cv.signal();
                    }
                    if (ready.equals(lastWriter)){
                        break;
                    }
                }

                if (!writerQueue.isEmpty()){
                    writerQueue.getFirst().cv.signal();
                }
                // log.debug(w.position);
                position = w.position;

            } catch (IOException ie) {
                ie.printStackTrace();
            } catch (InterruptedException ie){
                ie.printStackTrace();
            } finally {
                long endTime = System.nanoTime();
                // log.info("latency before get lock (ms) : " + (double)(beforeGetLockTime - startTime)/(1000*1000) );
                // log.info("get lock then add queue (ms) : " + (double)(addQueueTime - getLockTime)/(1000*1000) );
                // log.info("latency in write (ns) : " + (double)(endTime-startTime)/(1000*1000) );
                // log.info("latency in lock (ns) : " + (double)(endTime-getLockTime)/(1000*1000) );
                // log.info("wait lock (ns) : " + (double)(getLockTime-startTime)/(1000*1000));
                // if (writeTime != 0L){
                //     // log.info("write time (ns) : " + (double)(endTime-writeTime)/(1000*1000));
                //     log.info("true write time (ns) : " + (double)(trueWriteEndTime-trueWriteStartTime)/(1000*1000));
                // }
                writerQueueLock.unlock();
            }
            return position;

        }




        public ByteBuffer read(long position) {
            if (threadLocalReadMetaBuf.get() == null) {
                threadLocalReadMetaBuf.set(ByteBuffer.allocate(readMetaLength));
            }
            ByteBuffer readMeta = threadLocalReadMetaBuf.get();

            // log.debug("read from position : " + position);
            readMeta.clear();
            try {
                int ret;
                // dataFileChannel.read(tmp);
                ret = dataFileChannel.read(readMeta, position);
                readMeta.position(0);
                int dataLength = readMeta.getShort();
                // int dataLength = readMeta.getInt();
                ByteBuffer tmp = ByteBuffer.allocate(dataLength);
                ret = dataFileChannel.read(tmp, position + Short.BYTES);
                // log.debug(ret);
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

    public class PMCache{
        Map<String, Map<Integer, PMQueue> > pmQueueMap;
        int numOfBlocks;
		PMMemoryBlock[] pmBlocks;
		Heap h;
        public class PMMemoryBlock{
            long curPosition;
            long capacity;
            MemoryBlock block;
            Lock lock;
            PMMemoryBlock(Heap h, long blockCapacity){
                curPosition = 0;
                capacity = blockCapacity;
                block = h.allocateMemoryBlock(blockCapacity);
                lock = new ReentrantLock();
            }
            long addData(ByteBuffer data){
                lock.lock();
                int dataLength = data.remaining();
                long addr = curPosition;
                long dataAddr = curPosition;
                block.setInt(addr, dataLength);
                addr += Integer.BYTES;
                block.copyFromArray(data.array(), 0, addr, dataLength);
                curPosition += Integer.BYTES + dataLength;
                lock.unlock();
                return dataAddr;
            }
            ByteBuffer readData(long addr){
                int dataLength = block.getInt(addr);
                ByteBuffer data = ByteBuffer.allocate(dataLength);
                addr += Integer.BYTES;
                block.copyToArray(addr, data.array(), 0, dataLength);
                return data;
            }
        }
        public class PMQueue{
            long headAddr;
            long tailAddr;
            long coldReadPosition;
            long hotReadPosition;
            long minOffset;
            long maxOffset;
            HashMap<Long, Long> addrMap;
            PMQueue(){
                headAddr = 0L;
                tailAddr = 0L;
                coldReadPosition = 0L;
                hotReadPosition = 0L;
                minOffset = 0L;
                maxOffset = 0L;
                addrMap = new HashMap<>();
            }
        }
        PMCache(String pmCachePath){
            long capacity = 60L*1024L*1024L*1024L;
            h = Heap.createHeap(pmCachePath, capacity);
            numOfBlocks = 4;
            pmBlocks = new PMMemoryBlock[numOfBlocks];
            for (int i = 0; i < numOfBlocks; i++){
                pmBlocks[i] = new PMMemoryBlock(h, capacity/numOfBlocks);
            }
            pmQueueMap = new ConcurrentHashMap<>();
        }
        public boolean append(String topic, int queueId, ByteBuffer data){
            Integer queueIdObject = queueId;
            int pmBlockId = Math.floorMod(topic.hashCode()+queueIdObject.hashCode(), numOfDataFiles);
            Map<Integer, PMQueue> pmQueue = pmQueueMap.get(topic);
            if (pmQueue == null){
                pmQueue = new HashMap<>();
                pmQueueMap.put(topic, pmQueue);
            }
            PMQueue q = pmQueue.get(queueId);
            if (q == null){
                q = new PMQueue();
            }

            // q ??
            // FIXME: 
            long addr = pmBlocks[pmBlockId].addData(data);
            if (addr != -1){
                q.addrMap.put(q.maxOffset, addr);
                q.maxOffset++;
                return true;
            }


            return false;
        }
        public boolean getRange(String topic, int queueId, long offset, int fetchNum, Map<Integer, ByteBuffer> result){
            Integer queueIdObject = queueId;
            int pmBlockId = Math.floorMod(topic.hashCode()+queueIdObject.hashCode(), numOfDataFiles);

            Map<Integer, PMQueue> pmQueue = pmQueueMap.get(topic);
            if (pmQueue == null){
                return false;
            }
            PMQueue q = pmQueue.get(queueId);
            if (q == null){
                return false;
            }

            // q ??



            return false;
        }

        public boolean getRangeByPmAddr(String topic, int queueId, long pmAddr, int fetchNum, Map<Integer, ByteBuffer> result){
            return false;
        }
    }

    private String metadataFileName;
    private FileChannel metadataFileChannel;
    private DataFile[] dataFiles;
    private int numOfDataFiles;
    private TestStat testStat;
    // private ConcurrentHashMap<String, Integer> topic2queueid;
    // private ConcurrentHashMap<String, HashMap<int, > > topic2queueid;
    // private ConcurrentHashMap<String, Long> topic2queueid;
    public class GetDataRetParameters{
        long offset;
        int fetchNum;
    }


    public class HotDataCircleBuffer {
        public int head;
        public int tail;
        public int maxLength;
        public int curLength;
        public long headOffset;
        public long tailOffset;
        public ByteBuffer[] datas;
        HotDataCircleBuffer(){
            maxLength = 2;
            curLength = 0;
            head = 0;
            tail = 0;
            headOffset = 0L;
            tailOffset = 0L;
            datas = new ByteBuffer[maxLength];
            for (int i = 0; i < maxLength; i++){
                datas[i] = ByteBuffer.allocate(17408);
            }
        }

        public void copyData(int thisHead, ByteBuffer data){
            int pos = data.position();
            log.debug(pos);
            // log.info(datas[head]);
            log.debug(data);
            log.debug(datas[thisHead]);
            datas[thisHead].clear();
            datas[thisHead].put(data);
            datas[thisHead].flip();
            log.debug(datas[thisHead]);
            log.debug(data);
            data.position(pos);
            log.debug(data);
        }

        public void addData(ByteBuffer data){
            log.debug("head : "+head+" tail :"+tail + "  curLength:"+curLength);
            log.debug("original data: " + data);
            if (curLength == 0){
                // headOffset = 0L;
                // tailOffset = 0L;
                copyData(head, data);
                // datas[head] = data.slice();
                log.debug("headOffset : "+headOffset + "  tailOffset: "+tailOffset);
                curLength = 1;
                return;
            }
            headOffset++;
            head++;
            head = head % maxLength;
            
            // method 1
            // log.debug(data);
            // datas[head] = data.slice();
            // // datas[head] = data.duplicate();
            // log.debug(datas[head]);

            // method 2
            copyData(head, data);
            log.debug(datas[head]);

            // method 3
            // datas[head] = data;


            if (curLength < 8){
                curLength++;
            } else {
                tailOffset++;
                tail++;
                tail = tail % maxLength;
            }
        }

        public GetDataRetParameters getData(long offset, int fetchNum, Map<Integer, ByteBuffer> results){
            GetDataRetParameters ret = new GetDataRetParameters();
            log.debug("head : "+head+" tail :"+tail + "  curLength:"+curLength);
            log.debug("headOffset : "+headOffset + "  tailOffset: "+tailOffset);
            log.debug("offset : "+offset + "  fetchNum : "+fetchNum);
            ret.offset = offset;
            ret.fetchNum = fetchNum;

            if (curLength == 0){
                return ret;
            }


            // [offset, offset+fetchNum-1]
            //                                  [tailOffset, headOffset]
            if (offset+fetchNum-1 < tailOffset){
                return ret;
            }



            //              [offset,    offset+fetchNum-1]
            //                    [tailOffset, headOffset]

            //                 [offset, offset+fetchNum-1]
            //             [tailOffset,        headOffset]



            //   [offset, offset+fetchNum-1]
            //                  [tailOffset, headOffset]

            //                 [offset, offset+fetchNum-1]
            // [tailOffset,                                       headOffset]


            long startOffset = Math.max(offset, tailOffset);
            long endOffset = offset+fetchNum-1;
            long num = endOffset - startOffset + 1; // 能够在缓冲区中找到多少个数据？
            if (endOffset == offset+fetchNum-1){
                ret.fetchNum -= num;
            }

            log.debug("startOffset : "+startOffset);
            for (long i = startOffset; i <= endOffset; i++){
                int bufIndex = (int)( (i - tailOffset + tail) % maxLength);
                int resultIndex =(int) (i - offset);
                log.debug(resultIndex);
                log.debug(bufIndex);
                log.debug(datas[bufIndex]);
                datas[bufIndex].position(0);
                // datas[bufIndex].limit(datas[bufIndex].capacity());
                log.debug(datas[bufIndex]);
                results.put(resultIndex, datas[bufIndex]);
            }

            return ret;

        }

    }

    public class MQQueue {
        public Long maxOffset = 0L;
        public HashMap<Long, Long> queueMap;
        public HotDataCircleBuffer hotDataCache;
        public boolean isHot;
        public ByteBuffer maxOffsetData;
        public int dataFileId;
        public DataFile df;

        MQQueue(int thisDataFileId) {
            isHot = false;
            maxOffset = 0L;
            queueMap = new HashMap<>();
            dataFileId = thisDataFileId;
            // hotDataCache = new HotDataCircleBuffer();
        }
        MQQueue(DataFile dataFile){
            isHot = false;
            maxOffset = 0L;
            queueMap = new HashMap<>();
            df = dataFile;
            // hotDataCache = new HotDataCircleBuffer();
        }

    }

    public class MQTopic {
        public String topicName;
        public HashMap<Integer, MQQueue> topicMap;
        public MQQueue[] queueArray;
        public int dataFileId;
        public DataFile df;

        MQTopic(String name, int thisDataFileId) {
            topicName = name;
            // topicMap = new HashMap<Integer, MQQueue>();
            queueArray = new MQQueue[5002];
            dataFileId = thisDataFileId;
        }
        MQTopic(String name, DataFile dataFile){
            topicName = name;
            // topicMap = new HashMap<Integer, MQQueue>();
            queueArray = new MQQueue[5002];
            df = dataFile;
        }

    }

    private ConcurrentHashMap<String, MQTopic> mqMap;
    private ThreadLocal<Integer> localThreadId;
    private AtomicInteger numOfThreads;
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
        log.setLevel(mqConfig.logLevel);
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
        dataFiles = new DataFile[numOfDataFiles];
        for (int i = 0; i < numOfDataFiles; i++) {
            String dataFileName = dbDirPath + "/db" + i;
            log.info("Initializing datafile: " + dataFileName);
            dataFiles[i] = new DataFile(dataFileName);
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

        if (mqConfig.useStats){
            testStat = new TestStat(dataFiles);
        }

        localThreadId = new ThreadLocal<>();
        numOfThreads = new AtomicInteger();
        numOfThreads.set(0);
        log.info("init ok!");
    }

    @Override
    protected void finalize() throws Throwable {
        metadataFileChannel.close();
        for (int i = 0; i < dataFiles.length; i++) {
            dataFiles[i].close();
        }

    }
    public int updateThreadId() {
        if (localThreadId.get() == null) {
            int thisNumOfThread = numOfThreads.getAndAdd(1);
            localThreadId.set(thisNumOfThread);
            log.info("init thread id : " + thisNumOfThread);
        }
        return localThreadId.get();
    }

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        log.debug("append : "+topic+","+queueId);
        if (mqConfig.useStats){
            testStat.appendStart();
            testStat.appendUpdateStat(topic, queueId, data);
        }
        MQTopic mqTopic;
        MQQueue q;
        mqTopic = mqMap.get(topic);
        if (mqTopic == null) {
            // int threadId = updateThreadId();
            // int dataFileId = threadId / 10; //   0b11
            int dataFileId = Math.floorMod(topic.hashCode(), numOfDataFiles);
            // mqTopic = new MQTopic(topic, dataFileId);
            mqTopic = new MQTopic(topic, dataFiles[dataFileId]);
            mqMap.put(topic, mqTopic);
        }
        data = data.slice();

        q = mqTopic.queueArray[queueId];
        if (mqTopic.queueArray[queueId] == null){
            Integer queueIdObject = queueId;
            int dataFileId = Math.floorMod(topic.hashCode()+queueIdObject.hashCode(), numOfDataFiles);
            // q = new MQQueue(dataFileId);
            q = new MQQueue(dataFiles[dataFileId]);
            mqTopic.queueArray[queueId] = q;
        }
        // if (q.isHot){
        //     // if (q.hotDataCache == null){
        //     //     testStat.hotDataAlloc(topic, queueId);
        //     //     q.hotDataCache = new HotDataCircleBuffer();
        //     //     q.hotDataCache.tailOffset = q.maxOffset;
        //     //     q.hotDataCache.headOffset = q.maxOffset;
        //     // }
        //     // q.hotDataCache.addData(data);

        //     if (q.maxOffsetData == null){
        //         q.maxOffsetData = ByteBuffer.allocate(17408);
        //         testStat.hotDataAlloc(topic, queueId);
        //     }
        //     q.maxOffsetData.clear();
        //     q.maxOffsetData.put(data);
        //     q.maxOffsetData.flip();
        //     data.flip();
        // }

        // if (!mqTopic.topicMap.containsKey(queueId)) {
        //     q = new MQQueue();
        //     mqTopic.topicMap.put(queueId, q);
        // } else {
        //     q = mqTopic.topicMap.get(queueId);
        // }

        // Integer queueIdObject = queueId;
        // int dataFileId = Math.floorMod(topic.hashCode()+queueIdObject.hashCode(), numOfDataFiles);
        // int dataFileId = Math.floorMod(topic.hashCode()+queueId, numOfDataFiles);
        // log.info(dataFileId);


        // DataFile df = dataFiles[mqTopic.dataFileId];
        // DataFile df = dataFiles[q.dataFileId];
        // DataFile df = dataFiles[dataFileId];
        DataFile df = q.df;
        
        long position = 0;
        switch (mqConfig.writeMethod) {
            case 0:
                position = df.syncSeqWrite(data);
                break;
            case 1:
                position = df.syncSeqWriteAgg(data);
                break;
            case 2:
                position = df.syncSeqWriteAggDirect(data);
                break;
            case 3:
                position = df.syncSeqWriteAggHeap(data);
                break;
            case 4:
                position = df.syncSeqWritePushQueue(data);
                break;
            case 5:
                position = df.syncSeqWritePushQueueDirectBuffer(data);
                break;
            case 6:
                position = df.syncSeqWritePushQueueHeapBuffer(data);
                break;
            case 7:
                position = df.syncSeqWritePushQueueDirectBatchBuffer(data);
                break;
            case 8:
                position = df.syncSeqWritePushQueueHeapBatchBuffer(data);
                break;
            case 9:
                position = df.syncSeqWritePushQueueBatch(data);
                break;
            case 10:
                position = df.syncSeqWritePushQueueMapped(data);
                break;
            case 11:
                position = df.asyncSeqWritePushQueueHeapBatchBuffer(data);
                break;
            case 12:
                position = df.syncSeqWritePushConcurrentQueueHeapBatchBuffer(data);
                break;
            default:
                position = df.syncSeqWrite(data);
                break;
        }
        q.queueMap.put(q.maxOffset, position);
        Long ret = q.maxOffset;
        q.maxOffset++;

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
        mqTopic = mqMap.get(topic);
        if (mqTopic == null) {
            return ret;
        }
        q = mqTopic.queueArray[queueId];
        if (mqTopic.queueArray[queueId] == null){
            return ret;
        }

        // q = mqTopic.topicMap.get(queueId);
        // if (q == null) {
        //     return ret;
        // }

        if (offset >= q.maxOffset){
            return ret;
        }
        if (offset + fetchNum-1 >= q.maxOffset){
            fetchNum = (int)(q.maxOffset-offset);
        }


        // if (offset >= q.maxOffset-2){
        //     testStat.hitHotData(topic, queueId);
        //     q.isHot = true;
        //     if (q.isHot && q.hotDataCache != null){
        //         q.hotDataCache.getData(offset, fetchNum, ret);
        //         GetDataRetParameters changes = q.hotDataCache.getData(offset, fetchNum, ret);
        //         log.debug("original fetchNum: " + fetchNum);
        //         fetchNum = changes.fetchNum;
        //         log.debug("updated fetchNum: " + fetchNum);
        //     }
        // }


        // if (offset >= q.maxOffset-1){
        //     testStat.hitHotData(topic, queueId);
        //     q.isHot = true;

        //     if (q.maxOffsetData != null){
        //         ret.put(0, q.maxOffsetData);
        //         return ret;
        //     }
        // }


        long pos = 0;

        // Integer queueIdObject = queueId;
        // int dataFileId = Math.floorMod(topic.hashCode()+queueIdObject.hashCode(), numOfDataFiles);
        // DataFile df = dataFiles[mqTopic.dataFileId];
        // DataFile df = dataFiles[q.dataFileId];
        // DataFile df = dataFiles[dataFileId];
        DataFile df = q.df;

        for (int i = 0; i < fetchNum; i++) {
            pos = q.queueMap.get(offset + i);
            ByteBuffer bbf = df.read(pos);
            if (bbf != null) {
                bbf.position(0);
                bbf.limit(bbf.capacity());
                // if (i >= changes.fetchNum){
                //     if (ret.get(i).compareTo(bbf) != 0){
                //         log.info(ret.get(i));
                //         log.info(bbf);
                //         log.error("hot data circle buffer data error !");
                //         System.exit(0);
                //     }
                // }
 
                ret.put(i, bbf);
            }
        }

        return ret;
    }
}
