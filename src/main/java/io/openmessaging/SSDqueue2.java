package io.openmessaging;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import com.intel.pmem.llpl.util.ConcurrentLongART;

public class SSDqueue2 {
    AtomicLong DATA_FREE_OFFSET = new AtomicLong(0);
    AtomicLong META_FREE_OFFSET = new AtomicLong(0);
    AtomicInteger currentNum = new AtomicInteger(0);

    ConcurrentHashMap<String, Lock> dataControl;
    ConcurrentHashMap<String, Lock> queueIdControl;

    ConcurrentHashMap<String, Long> topicNameQueueMetaMap;
    ConcurrentHashMap<String, Map<Integer, Long>> queueTopicMap;

    int QUEUE_NUM = 10000;
    int TOPIC_NUM = 100;
    int TOPIC_NAME_SZIE = 128;
    Long topicArray;


    FileChannel metaFc;
    FileChannel dataFc;
    
    public SSDqueue2(FileChannel metaFc, FileChannel dataFc){
        this.metaFc = metaFc;
        this.dataFc = dataFc;
        // currentNum
        META_FREE_OFFSET.set(Integer.BYTES +  TOPIC_NUM *(TOPIC_NAME_SZIE + Long.BYTES));

        this.topicNameQueueMetaMap = new ConcurrentHashMap<>();
        this.queueTopicMap = new ConcurrentHashMap<>();

        dataControl = new ConcurrentHashMap<>();
        queueIdControl = new ConcurrentHashMap<>();

        
    }
    int getTopicPostion(int idx){
        return idx * (Integer.BYTES +  idx *(TOPIC_NAME_SZIE + Long.BYTES));
    }
    long createQueue(){
        long queueArray = META_FREE_OFFSET.getAndAdd(Integer.BYTES +  QUEUE_NUM*( Long.BYTES + Integer.BYTES) );
        /*
        try{
            ByteBuffer tmp = ByteBuffer.allocate(Integer.BYTES);
            tmp.putInt(0);
            metaFc.write(tmp, queueArray);
        }catch(IOException ie){
            ie.printStackTrace();
        }
        */
        return queueArray;
    }
    long getQueuePosition(int idx, long queueArray){
        return queueArray + Integer.BYTES + idx*(Integer.BYTES + Long.BYTES);
    }
    long createDataLink(int size){
        return DATA_FREE_OFFSET.getAndAdd( Long.BYTES * 3 + (Long.BYTES * 2 + size));
    }

    public Long setTopic(String topicName, int queueId, ByteBuffer data){
        Map<Integer, Long> topicData = queueTopicMap.get(topicName);
            if(topicData == null){
                int idx = currentNum.getAndIncrement();
                //////
                // set data
                int size = data.array().length;
                Long dataMeta = createDataLink(size);
                Long head = dataMeta + 3*Long.BYTES;
                Long tail = head;
                Long dataNum = 1L;
                ByteBuffer writeData = ByteBuffer.allocate(Long.BYTES * 3 + (Long.BYTES * 2 + size));
                writeData.putLong(dataNum);
                writeData.putLong(head);
                writeData.putLong(tail);
                writeData.putLong(size);
                writeData.putLong(-1L);
                writeData.put(data);
                writeData.flip();   
                try{
                    dataFc.write(writeData, dataMeta);
                }catch(IOException ie){
                    ie.printStackTrace();
                }
                //////
                // queueId set
                Long queueMeta = createQueue();
                ByteBuffer tmp = ByteBuffer.allocate(Integer.BYTES + Integer.BYTES + Long.BYTES);
                tmp.putInt(1);
                tmp.putInt(queueId);
                tmp.putLong(dataMeta);
                tmp.flip();
                try{
                    metaFc.write(tmp, queueMeta);
                    //metaFc.force();
                }catch(IOException ie){
                    ie.printStackTrace();
                }
                tmp.clear();
                //////
                // topic set
                tmp = ByteBuffer.allocate(Long.BYTES + TOPIC_NAME_SZIE);
                tmp.putLong(queueMeta);
                tmp.put(topicName.getBytes());
                tmp.flip();

                ByteBuffer tmp1 = ByteBuffer.allocate(Integer.BYTES);
                tmp1.putInt(currentNum.get());
                tmp1.flip();
                try{
                    metaFc.write(tmp, getTopicPostion(idx));
                    metaFc.write(tmp1, 0);
                }catch(IOException ie){
                    ie.printStackTrace();
                }
                
                
            }else{
                Long metaDataOffset = topicData.get(queueId);
                if(metaDataOffset == null){
                    
                }else{
                    
                }
            }
            return null;
    }

    public Map<Integer, ByteBuffer> getRange(String topicName, int queueId, Long offset, int fetchNum){
        return null;
    }
}
