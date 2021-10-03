package io.openmessaging;


import org.apache.log4j.Logger;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
//import org.slf4j.LoggerFactory;
//import org.slf4j.Logger;
import java.io.RandomAccessFile;




public class SSDqueue{
    
    private static final Logger logger = Logger.getLogger(SSDqueue.class);
    
    FileChannel spaceMetaFc;


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

    // opt
    FileChannel[] fileChannels;
    int numOfDataFileChannels;
    DataSpace dataSpace;
    String path = "/home/wangxr/桌面/pmem_test/space"; //"/home/ubuntu/space"

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

    void updateSpace(){
        try {
            ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES * 2);
            tmp.putLong(FREE_OFFSET.get());
            tmp.putLong(META_FREE_OFFSET.get());
            tmp.flip();
            spaceMetaFc.write(tmp, 0L);
            spaceMetaFc.force(true);
        } catch (Exception e) {
            //TODO: handle exception
            e.printStackTrace();
        }
    }
    void init(){
        try {
            this.spaceMetaFc = new RandomAccessFile(new File(path), "rw").getChannel();
        } catch (Exception e) {
            //TODO: handle exception
            e.printStackTrace();
        }
    }
    public SSDqueue(FileChannel fileChannel, FileChannel metaFileChannel){
        init();
        this.fileChannel = fileChannel;
        this.metaFileChannel = metaFileChannel;
        dataSpace = new DataSpace(fileChannel, 0L);

        // 划分起始的 Long.BYTES * 来存元数据
        currentNum.set(0);
        META_FREE_OFFSET.set(TOPIC_NUM * (TOPIC_NAME_SZIE + Long.BYTES) + Integer.BYTES);
        this.topicArrayOffset = 0L + Integer.BYTES;

        this.topicNameQueueMetaMap = new ConcurrentHashMap<>();
        this.queueTopicMap = new ConcurrentHashMap<>();
        FREE_OFFSET.set(0L);

        testStat = new TestStat();
        logger.info("initialize new SSDqueue, num: "+currentNum.get());
    }
    public SSDqueue(FileChannel fileChannel, FileChannel metaFileChannel, Boolean t)throws IOException{
        init();
        dataSpace = new DataSpace(fileChannel);
        
        ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES * 2);
        int size = spaceMetaFc.read(tmp, 0L);
        tmp.flip();
        FREE_OFFSET.set(tmp.getLong());
        META_FREE_OFFSET.set(tmp.getLong());
        
        // 读盘，建表 
        this.fileChannel = fileChannel;
        this.metaFileChannel = metaFileChannel;
        this.topicNameQueueMetaMap = new ConcurrentHashMap<>();
        this.queueTopicMap = new ConcurrentHashMap<>();
        this.topicArrayOffset = 0L + Integer.BYTES;

        tmp = ByteBuffer.allocate(Integer.BYTES);
        metaFileChannel.read(tmp);
        tmp.flip();
        this.currentNum.set(tmp.getInt());

        Long offset = 0L + Integer.BYTES;
        tmp = ByteBuffer.allocate(Long.BYTES + TOPIC_NAME_SZIE);
        for(int i=0; i<currentNum.get(); i++, offset += Long.BYTES + TOPIC_NAME_SZIE){
            int len = metaFileChannel.read(tmp, offset);
            tmp.flip();
            Long queueMetaOffset = tmp.getLong();

            byte [] bytes = new byte[TOPIC_NAME_SZIE];
            tmp.get(bytes);
            String topicName = new String(bytes).trim();

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
    
        ByteBuffer metaTmp = ByteBuffer.allocate(Integer.BYTES);
        metaTmp.putInt(currentNum.get());
        metaTmp.flip();
        
        try {
            metaFileChannel.write(tmp, this.topicArrayOffset + (currentNum.getAndIncrement()) * (TOPIC_NAME_SZIE + Long.BYTES));
            int len = metaFileChannel.write(tmp, 0L);
            metaFileChannel.force(true);
        } catch (Exception e) {
            //TODO: handle exception
            e.printStackTrace();
        }
        
    }

    public Long setTopic(String topicName, int queueId, ByteBuffer data){
        testStat.appendStart();
        Long result;
        try{
            Map<Integer, Long> topicData = queueTopicMap.get(topicName);
            if(topicData == null){
                // 自下而上
                //int fcId = Math.floorMod(topicName.hashCode(), numOfDataFileChannels);
                //Data writeData = new Data(fileChannels[fcId]);
                Data writeData = new Data(fileChannel);
                result = writeData.put(data);

                QueueId q =  new QueueId(metaFileChannel, QUEUE_NUM);
                Long qOffset = q.put(queueId, writeData.getMetaOffset());

                this.put(topicName, q.metaDataOffset);

                //logger.info("num: "+ cur + " metaQueue: "+ queueArray.getMetaOffset());
                // 更新 DRAM map
                topicData = new HashMap<>();
                topicData.put(queueId, writeData.getMetaOffset());
                topicNameQueueMetaMap.put(topicName, q.getMetaOffset());
                queueTopicMap.put(topicName, topicData);

                //control.put(topicName + queueId, new ReentrantLock());
                //System.out.println("112: w meta: "+ writeData.getMetaOffset());  
            }else{
                Long metaDataOffset = topicData.get(queueId);
                if(metaDataOffset == null){
                    // 增加 queueIdArray
                    // 自下而上
                    Data writeData = new Data(fileChannel);
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
            Map<Integer, Long> topicData = queueTopicMap.get(topicName);
            if(topicData == null) return result;

            Long metaDataOffset = topicData.get(queueId);
            if(metaDataOffset == null) return result;

            // int fcId = Math.floorMod(topicName.hashCode(), numOfDataFileChannels);
            // Data resData = new Data(fileChannels[fcId], metaDataOffset);
            Data resData = new Data(fileChannel, metaDataOffset);
            result = resData.getRange(offset, fetchNum);

            testStat.getRangeUpdateStat(topicName,queueId, offset, fetchNum);
        }catch(IOException e){
            logger.error(e);
        }
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
            updateSpace();

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
            long offset = queueIdArray + (long) currentNum * (Integer.BYTES + Long.BYTES);

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
            return offset;
        }
        public Long getMetaOffset(){
            return this.metaDataOffset;
        }
        public Map<Integer, Long> readAll() throws IOException{
            Map<Integer, Long> res = new HashMap<>();
            ByteBuffer tmp = ByteBuffer.allocate(Integer.BYTES + Long.BYTES);
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
            this.metaOffset = dataSpace.FREE_OFFSET.getAndAdd(Long.BYTES * 3);
            //updataSpace();
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
            long startOffset = dataSpace.writeAgg(data);
            updateSpace(); // TODO: 多线程并发问题？

            // TODO： 上一个 block 指向当前 block 的指针更新如何优化
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 3);
            if(tail == -1L){ // update an empty data queue
                tail = startOffset;
                head = tail;
            }else{
                buffer.putLong(startOffset);
                buffer.flip();
                fileChannel.write(buffer, tail + Long.BYTES); // 更新 nextOffset
//                fileChannel.force(true);
                tail = startOffset;
            }

            //////////////////////////////////////////////////////////////
//            int tmpSize = Long.BYTES + Long.BYTES + data.remaining();
//            long startOffset = FREE_OFFSET.getAndAdd(tmpSize);
//            updateSpace();
//
//            long nextOffset = -1L;
//            ByteBuffer buffer = ByteBuffer.allocate(tmpSize);
//            buffer.putLong(tmpSize - (Long.BYTES + Long.BYTES));
//            buffer.putLong(nextOffset);
//            buffer.put(data);
//            buffer.flip();
//            int lens = fileChannel.write(buffer,startOffset);
//
//            if(tail == -1L){
//                tail = startOffset;
//                head = tail;
//            }else{
//                buffer.putLong(startOffset);
//                buffer.flip();
//                fileChannel.write(buffer, tail + Long.BYTES); // 更新 nextOffset
////                fileChannel.force(true);
//                tail = startOffset;
//            }
            ///////////////////////////////////////////////////////
            // 更新 totalNum, tail, head 进 SSD
            totalNum++;
            buffer.clear();
            buffer.putLong(totalNum);
            buffer.putLong(head);
            buffer.putLong(tail);
            buffer.flip();
            int len = fileChannel.write(buffer, this.metaOffset);
            fileChannel.force(true);

            return totalNum-1;
        }
        public Map<Integer, ByteBuffer> getRange(Long offset, int fetchNum) throws IOException{
            Long startOffset = head;
            Map<Integer, ByteBuffer> res = new HashMap<>();
            ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES);
            for(int i=0; i<offset && startOffset != -1L; ++i){
                Long nextOffset = startOffset + Long.BYTES;
                int len = fileChannel.read(tmp, nextOffset);
                tmp.flip();    
                startOffset = tmp.getLong();
            }
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2);
            for(int i=0; i<fetchNum && startOffset != -1L; ++i){
                buffer.clear();
                int len1 = fileChannel.read(buffer, startOffset);
                buffer.flip();

                long dataSize = buffer.getLong();
                long nextOffset = buffer.getLong();

                ByteBuffer tmp1 = ByteBuffer.allocate((int) dataSize);
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
