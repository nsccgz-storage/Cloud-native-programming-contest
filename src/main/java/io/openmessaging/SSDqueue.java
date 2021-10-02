package io.openmessaging;

import java.io.IOException;

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
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.spi.LoggerFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.slf4j.Logger;

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
    
    ConcurrentHashMap<String, Lock> control; // topicName + topicId clip

    FileChannel fileChannel;
    FileChannel metaFileChannel;

    TestStat testStat;
    private class TestStat{
        // report throughput per second
        ThreadLocal<Long> appendStartTime;
        ThreadLocal<Long> appendEndTime;
        ThreadLocal<Long> getRangeStartTime;
        ThreadLocal<Long> getRangeEndTime;
        ThreadLocal<Long> opCount;
        ThreadLocal<Long> appendCount;
        ThreadLocal<Long> getRangeCount;
        // ThreadLocal< HashMap<Integer, Long> >
        // report operation per second
        TestStat(){
            appendStartTime = new ThreadLocal<>();
            appendEndTime = new ThreadLocal<>();
            getRangeStartTime = new ThreadLocal<>();
            getRangeEndTime = new ThreadLocal<>();

            appendCount = new ThreadLocal<>();
            getRangeCount = new ThreadLocal<>();
            opCount = new ThreadLocal<>();
        }
        void appendStart(){
            if(appendStartTime.get() == null || appendStartTime.get() == 0L){
                appendStartTime.set(System.nanoTime());
                logger.info("init append time");
            }
        }
        void getRangeStart(){
            if(getRangeStartTime.get() == null || getRangeStartTime.get() == 0L){
                getRangeStartTime.set(System.nanoTime());
                logger.info("init getRange time");
            }
        }

        void appendUpdateStat(String topic, int queueId, ByteBuffer data){
            if (appendCount.get() == null){
                appendCount.set(0L);
            }
            appendEndTime.set(System.nanoTime());
            appendCount.set(appendCount.get()+1);
            update();
        }
        void getRangeUpdateStat(String topic, int queueId, long offset, int fetchNum){
            if (getRangeCount.get() == null){
                getRangeCount.set(0L);
            }
            getRangeEndTime.set(System.nanoTime());
            getRangeCount.set(getRangeCount.get()+1);
            update();
        }
        void update(){
            if (opCount.get() == null){
                opCount.set(0L);
            }
            long curOpCount = opCount.get();
            if (curOpCount % 10000 == 0){
                report();
            }
            opCount.set(curOpCount+1);
        }
        void report(){
            double appendElapsedTimeMS = (double)(appendEndTime.get()-appendStartTime.get())/(1000*1000);
            double appendThroughput = (double)appendCount.get()/appendElapsedTimeMS;
            logger.info("[Append  ] op count : " + appendCount.get());
            logger.info("[Append  ] elapsed time (ms) : " + appendElapsedTimeMS);
            logger.info("[Append  ] Throughput (op/ms): " + appendThroughput);

            if (getRangeEndTime.get() == null){
                getRangeEndTime.set(0L);
            }
            if (getRangeStartTime.get() == null){
                getRangeStartTime.set(0L);
            }
            if (getRangeCount.get() == null){
                getRangeCount.set(0L);
            }
            double getRangeElapsedTimeMS = (double)(getRangeEndTime.get()-getRangeStartTime.get())/(1000*1000);
            double getRangeThroughput = (double)getRangeCount.get()/getRangeElapsedTimeMS;
            logger.info("[getRange] op count : " + getRangeCount.get());
            logger.info("[getRange] elapsed time (ms) : " + getRangeElapsedTimeMS);
            logger.info("[getRange] Throughput (op/ms): " + getRangeThroughput);
        }

        // report topic stat per second
    }

    public SSDqueue(FileChannel fileChannel, FileChannel metaFileChannel){
        this.fileChannel = fileChannel;
        this.metaFileChannel = metaFileChannel;

        // 划分起始的 Long.BYTES * 来存元数据
        currentNum.set(0);
        META_FREE_OFFSET.set(TOPIC_NUM * (TOPIC_NAME_SZIE + Long.BYTES) + Integer.BYTES);
        this.topicArrayOffset = 0L + Integer.BYTES;

        this.topicNameQueueMetaMap = new ConcurrentHashMap<>();
        this.queueTopicMap = new ConcurrentHashMap<>();
        FREE_OFFSET.set(0L);

        control = new ConcurrentHashMap<>();

        testStat = new TestStat();
        logger.info("initialize new SSDqueue, num: "+currentNum.get());
    }
    public SSDqueue(FileChannel fileChannel, FileChannel metaFileChannel, Boolean t)throws IOException{
        // 读盘，建表 
        // TODO, recover
        this.fileChannel = fileChannel;
        this.metaFileChannel = metaFileChannel;
        this.topicNameQueueMetaMap = new ConcurrentHashMap<>();
        this.queueTopicMap = new ConcurrentHashMap<>();
        this.topicArrayOffset = 0L + Integer.BYTES;

        ByteBuffer tmp = ByteBuffer.allocate(Integer.BYTES);
        metaFileChannel.read(tmp);
        tmp.flip();

        this.currentNum.set(tmp.getInt());

        Long startOffset = 0L + Integer.BYTES;
        
        for(int i=0; i<currentNum.get(); i++){
    
            Long offset = startOffset + i*(Long.BYTES + TOPIC_NAME_SZIE);
            tmp.clear();
            tmp = ByteBuffer.allocate(Long.BYTES);
            int len = metaFileChannel.read(tmp, offset);
            tmp.flip();

            Long queueMetaOffset = tmp.getLong();

            tmp.clear();
            tmp = ByteBuffer.allocate(TOPIC_NAME_SZIE);
            len = metaFileChannel.read(tmp,offset + Long.BYTES);
            tmp.flip();

            String topicName = new String(tmp.array()).trim();
            //logger.info("len: " + len + " topicName: " + topicName + " queueMetaOffset: " + queueMetaOffset + " num" + currentNum.get());
            //System.out.println("75： " + queueMetaOffset);

            topicNameQueueMetaMap.put(topicName, queueMetaOffset);
            
            // 遍历每个 topic 下的 queue
            queueTopicMap.put(topicName, readQueue(queueMetaOffset));

        }

        testStat = new TestStat();
        logger.info("recover a SSDqueue, num: "+currentNum.get());
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
                // 增加 topicIdArray

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
                metaFileChannel.force(true);
                
                tmp.clear();
                tmp = ByteBuffer.allocate(Integer.BYTES);
                tmp.putInt(currentNum.get());
                tmp.flip();
                int len = metaFileChannel.write(tmp, 0L);
                metaFileChannel.force(true);

                //System.out.println("110: " + len);
                //logger.info("num: "+ cur + " metaQueue: "+ queueArray.getMetaOffset());
                // 更新 DRAM map
                topicData = new HashMap<>();
                topicData.put(queueId, writeData.getMetaOffset());
                topicNameQueueMetaMap.put(topicName, queueArray.getMetaOffset());
                queueTopicMap.put(topicName, topicData);

                //control.put(topicName + queueId, new ReentrantLock());
                //System.out.println("112: w meta: "+ writeData.getMetaOffset());

//                return res;
            
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
                    control.put(topicName + queueId, new ReentrantLock());
                    
//                    return res;
                }else{
                    Data writeData = new Data(fileChannel, metaDataOffset);
                    result = writeData.put(data);
//                    return writeData.put(data);
                }
            }

        } catch (Exception e) {
            //TODO: handle exception
            e.printStackTrace();
            return null;
        }
        testStat.appendUpdateStat(topicName, queueId, data);
        return result;
    }
    public Map<Integer, ByteBuffer> getRange(String topicName, int queueId, Long offset, int fetchNum){
        try{
            testStat.getRangeStart();
            Map<Integer, Long> topicData = queueTopicMap.get(topicName);
            if(topicData == null) return null;
            Long metaDataOffset = topicData.get(queueId);
            if(metaDataOffset == null) return null;

            //System.out.println("143: r meta: "+ metaDataOffset);

            Data resData = new Data(fileChannel, metaDataOffset);
            testStat.getRangeUpdateStat(topicName,queueId, offset, fetchNum);
            return resData.getRange(offset, fetchNum);
        }catch(IOException e){
            return null;
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

            this.currentNum = 0;
            ByteBuffer tmp = ByteBuffer.allocate(Integer.BYTES);
            tmp.putInt(this.currentNum);
            tmp.flip();
            metaFileChannel.write(tmp, metaDataOffset);
            metaFileChannel.force(true);

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

            ByteBuffer tmpData = ByteBuffer.allocate(Integer.BYTES + Long.BYTES);
            tmpData.putInt(queueId);
            tmpData.putLong(dataMetaOffset);
            tmpData.flip();
            metaFileChannel.write(tmpData, offset);
            metaFileChannel.force(true);
            // TODO: 写回 SSD
            // 这个需要原子修改
            currentNum++;
            ByteBuffer tmp = ByteBuffer.allocate(Integer.BYTES);
            tmp.putInt(this.currentNum);
            tmp.flip();
            metaFileChannel.write(tmp, metaDataOffset);
            metaFileChannel.force(true);
            this.queueIdArray = this.metaDataOffset + Integer.BYTES;
            //System.out.println("w 213: " + this.toString());
            //logger.info(toString());
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
                //logger.info(toString());
                //System.out.println("222: " + len2);
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
            fileChannel.force(true);
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
        public Long put(ByteBuffer data) throws IOException{
            int tmpSize = Long.BYTES + Long.BYTES + data.array().length;
            long startOffset = FREE_OFFSET.getAndAdd(tmpSize);

            long nextOffset = -1L;
            ByteBuffer byteData = ByteBuffer.allocate(tmpSize);
            byteData.putLong(tmpSize - (Long.BYTES + Long.BYTES));
            byteData.putLong(nextOffset);
            byteData.put(data);
            byteData.flip();
            int lens = fileChannel.write(byteData,startOffset);
            fileChannel.force(true);

            if(tail == -1L){
                tail = startOffset;
                head = tail;

            }else{
                ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES);
                tmp.putLong(startOffset);
                tmp.flip();
                fileChannel.write(tmp, tail + Long.BYTES); // 更新 nextOffset
                fileChannel.force(true);
                tail = startOffset;
            }
            
            // 更新 totalNum, tail, head 进 SSD
            //System.out.println("275: "+ startOffset);
            totalNum++;
            ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES * 3);
            tmp.putLong(totalNum);
            //System.out.println(tmp + " " + new String(tmp.array()));
            tmp.putLong(head);
            //System.out.println(tmp + " " + new String(tmp.array()));
            tmp.putLong(tail);
            //System.out.println(tmp + " " + new String(tmp.array()));
            tmp.flip();

            //System.out.println(tmp + " " + new String(tmp.array()));

            int len = fileChannel.write(tmp, this.metaOffset);
            fileChannel.force(true);
            //System.out.println("w: " + this.toString() + " : " + len);

            return totalNum;
        }
        public Map<Integer, ByteBuffer> getRange(Long offset, int fetchNum) throws IOException{
            Long startOffset = head;
            Map<Integer, ByteBuffer> res = new HashMap<>();
            //ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES);
            for(int i=0; i<offset && startOffset != -1; ++i){
                Long nextOffset = startOffset + Long.BYTES;

                ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES);
                int len = fileChannel.read(tmp, nextOffset);
                tmp.flip();    
                startOffset = tmp.getLong();
                
            }

            for(int i=0; i<fetchNum && startOffset != -1L; ++i){
                ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES + Long.BYTES);
                int len1 = fileChannel.read(tmp, startOffset);
                //logger.info(len1 + " " + startOffset);
                tmp.flip();

                Long dataSize = tmp.getLong();
                Long nextOffset = tmp.getLong();

                ByteBuffer tmp1 = ByteBuffer.allocate(dataSize.intValue());
                int len2 = fileChannel.read(tmp1, startOffset + Long.BYTES + Long.BYTES);

                tmp1.flip();
                res.put(i, tmp1);
                startOffset = nextOffset;

            }
            return res;
        }
        public Long getMetaOffset(){
            return this.metaOffset;
        }
    }
}
