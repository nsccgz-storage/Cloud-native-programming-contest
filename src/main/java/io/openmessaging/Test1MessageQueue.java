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

import org.slf4j.LoggerFactory;
import org.apache.log4j.pattern.ThrowableInformationPatternConverter;
import org.slf4j.Logger;

public class Test1MessageQueue {
    private static final Logger log = LoggerFactory.getLogger(Test1MessageQueue.class);

    private class DataFile {
        // public String dataFileName;
        public FileChannel dataFileChannel;
        // public AtomicLong atomicCurPosition;
        public Long curPosition;
    
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
        }
    
        // public long allocate(long size) {
        //     return atomicCurPosition.getAndAdd(size);
        // }
    
        // public void write(ByteBuffer data, long position) {
        //     try {
        //         dataFileChannel.write(data, position);
        //     } catch (IOException ie) {
        //         ie.printStackTrace();
        //     }
        // }
    
        public synchronized Long syncSeqWrite(ByteBuffer data) {
            ByteBuffer tmp = ByteBuffer.allocate(Integer.BYTES+data.capacity());
            tmp.putInt(data.capacity());
            tmp.put(data);
            long position = curPosition;
            log.debug("position : " + position);
            try {
                dataFileChannel.write(tmp, position);
            } catch (IOException ie) {
                ie.printStackTrace();
            }
            log.debug("write size : " + tmp.capacity());
            log.debug("data size : " + data.capacity());
            curPosition += tmp.capacity();
            log.debug("update position to: " + curPosition);
            return position;
        }
    
        public ByteBuffer read(long position) {
            ByteBuffer tmp = ByteBuffer.allocate(Integer.BYTES+17408);
            try {
                dataFileChannel.read(tmp, position);
            } catch (IOException ie) {
                ie.printStackTrace();
            }
            int dataLength = tmp.getInt();
            byte[] data = new byte[dataLength];
            tmp.get(data);
    

            return ByteBuffer.wrap(data);
        }
    
    }
    


    private String metadataFileName;
    private String dataFileName;
    private FileChannel metadataFileChannel;
    private ArrayList<DataFile> dataFiles;
    private int numOfDataFiles;
    // private ConcurrentHashMap<String, Integer> topic2queueid;
    // private ConcurrentHashMap<String, HashMap<int, > > topic2queueid;
    // private ConcurrentHashMap<String, Long> topic2queueid;

    public class MQQueue {
        public Long maxOffset = 0L;
        public HashMap<Long, Long> queueMap;
        MQQueue(){
            maxOffset = 0L;
            queueMap = new HashMap<>();
        }
    }

    public class MQTopic {
        public String topicName;
        public HashMap<Integer, MQQueue> topicMap;
        MQTopic(String name){
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
        numOfDataFiles = 1;
        dataFiles = new ArrayList<>();
        for (int i = 0; i < numOfDataFiles; i++){
            String dataFileName = dbDirPath+"/db"+i;
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

        log.info("init ok!");
    }

    // @Override
    // protected void finalize() throws Throwable {
    // metadataFileChannel.close();
    // dataFileChannel.close();
    // }

    public long append(String topic, int queueId, ByteBuffer data){
        MQTopic mqTopic;
        MQQueue q;
        if (!mqMap.containsKey(topic)){
            mqTopic = new MQTopic(topic);
            mqMap.put(topic, mqTopic);
        } else {
            mqTopic = mqMap.get(topic);
        }

        if (!mqTopic.topicMap.containsKey(queueId)){
            q = new MQQueue();
            mqTopic.topicMap.put(queueId, q);
        } else {
            q = mqTopic.topicMap.get(queueId);
        }

        int dataFileId = queueId % numOfDataFiles;
        DataFile df = dataFiles.get(dataFileId);
        long position = df.syncSeqWrite(data);
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
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        MQTopic mqTopic;
        MQQueue q;
        if (!mqMap.containsKey(topic)){
            return null;
        } else {
            mqTopic = mqMap.get(topic);
        }

        if (!mqTopic.topicMap.containsKey(queueId)){
            return null;
        } else {
            q = mqTopic.topicMap.get(queueId);
        }

        long pos = 0;
        int dataFileId = queueId % numOfDataFiles;
        DataFile df = dataFiles.get(dataFileId);

        Map<Integer, ByteBuffer> ret = new HashMap<Integer, ByteBuffer>();
        for (int i = 0; i < fetchNum; i++){
            pos = q.queueMap.get(offset+i);
            ByteBuffer bbf = df.read(pos);
            ret.put(i, bbf);
        }

        return ret;
    }
}
