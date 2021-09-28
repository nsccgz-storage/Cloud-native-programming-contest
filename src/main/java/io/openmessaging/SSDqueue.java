package io.openmessaging;

import java.io.IOException;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;



public class SSDqueue{
    static Long FREE_OFFSET = 0L;
    static Long META_FREE_OFFSET = 0L;
    int QUEUE_NUM = 10000;
    int TOPIC_NUM = 100;
    int currentNum;
    int TOPIC_NAME_SZIE = 128;
    Long topicArrayOffset;
    
    
    ConcurrentHashMap<String, Long> topicNameQueueMetaMap;
    ConcurrentHashMap<String, Map<Integer, Long>> queueTopicMap;
    

    FileChannel fileChannel;
    FileChannel metaFileChannel;

    public SSDqueue(FileChannel fileChannel, FileChannel metaFileChannel){
        this.fileChannel = fileChannel;
        this.metaFileChannel = metaFileChannel;

        // 划分起始的 Long.BYTES * 来存元数据
        this.currentNum = 0;     
        META_FREE_OFFSET += TOPIC_NUM * (TOPIC_NAME_SZIE + Long.BYTES) + Integer.BYTES;
        this.topicArrayOffset = 0L + Integer.BYTES;
        this.topicNameQueueMetaMap = new ConcurrentHashMap<>();
        this.queueTopicMap = new ConcurrentHashMap<>();

    }
    public SSDqueue(FileChannel fileChannel, FileChannel metaFileChannel, Boolean t)throws IOException{
        // 读盘，建表
        this.fileChannel = fileChannel;
        this.metaFileChannel = fileChannel;
        this.topicNameQueueMetaMap = new ConcurrentHashMap<>();
        this.queueTopicMap = new ConcurrentHashMap<>();

        ByteBuffer tmp = ByteBuffer.allocate(Integer.BYTES);
        metaFileChannel.read(tmp);

        this.currentNum = tmp.getInt();

        Long startOffset = 0L + Integer.BYTES;
        for(int i=0; i<this.currentNum; i++){
    
            Long offset = startOffset + i*(Integer.BYTES + TOPIC_NAME_SZIE);
            tmp = ByteBuffer.allocate(Integer.BYTES);
            metaFileChannel.read(tmp,offset);
            Long queueMetaOffset = tmp.getLong();

            tmp = ByteBuffer.allocate(TOPIC_NAME_SZIE);
            metaFileChannel.read(tmp,offset + Long.BYTES);

            String topicName = new String(tmp.array()).trim();
            topicNameQueueMetaMap.put(topicName, queueMetaOffset);
            
            // 遍历每个 topic 下的 queue
            queueTopicMap.put(topicName, readQueue(queueMetaOffset));

        }


    }
    public Map<Integer, Long> readQueue(Long queueMetaOffset) throws IOException{
        QueueId resData = new QueueId(fileChannel, queueMetaOffset);
        return resData.readAll();

    }
    public Long setTopic(String topicName, int queueId, ByteBuffer data)throws IOException{
        Map<Integer, Long> topicData = queueTopicMap.get(topicName);
        if(topicData == null){
            // 增加 topicIdArray

            // 自下而上
            Data writeData = new Data(fileChannel);
            Long res = writeData.put(data);

            QueueId queueArray =  new QueueId(metaFileChannel, QUEUE_NUM);
            Long queueOffset = queueArray.put(queueId, writeData.getMetaOffset());

            // 
            ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES + TOPIC_NAME_SZIE); // offset : name
            tmp.putLong(queueArray.getMetaOffset());
            tmp.put(topicName.getBytes(), Long.BYTES, topicName.length());
            metaFileChannel.write(tmp, this.topicArrayOffset + currentNum * (TOPIC_NAME_SZIE + Long.BYTES));
            currentNum++;

            tmp = ByteBuffer.allocate(Integer.BYTES);
            tmp.putInt(0, currentNum);
            metaFileChannel.write(tmp, 0L);

            // 更新 DRAM map
            topicData = new HashMap<>();
            topicData.put(queueId, queueOffset);
            topicNameQueueMetaMap.put(topicName, queueArray.getMetaOffset());
            queueTopicMap.put(topicName, topicData);

            return res;

        }else{
            Long metaDataOffset = topicData.get(queueId);
            if(metaDataOffset == null){
                // 增加 queueIdArray
                // 自下而上
                Data writeData = new Data(fileChannel);
                Long res = writeData.put(data);
                Long queueMetaOffset = topicNameQueueMetaMap.get(topicName);
                QueueId queueArray =  new QueueId(metaFileChannel, queueMetaOffset); // 写入 SSD
                Long queueOffset = queueArray.put(queueId, writeData.getMetaOffset());

                // 插入 DRAM 哈希表
                topicData.put(queueId, queueOffset);
                queueTopicMap.put(topicName, topicData);
                
                return res; 
            }else{
                Data writeData = new Data(fileChannel, metaDataOffset);
                return writeData.put(data);
            }
        }

    }
    public Map<Integer, ByteBuffer> getRange(String topicName, int queueId, Long offset, int fetchNum)throws IOException{
        Map<Integer, Long> topicData = queueTopicMap.get(topicName);
        if(topicData == null) return null;
        Long metaDataOffset = topicData.get(queueId);
        if(metaDataOffset == null) return null;
        Data resData = new Data(fileChannel, metaDataOffset);

        return resData.getRange(offset, fetchNum);
    }


    private class QueueId{
        FileChannel metaFileChannel;
        
        Long metaDataOffset;
        int currentNum;
        Long queueIdArray;
        
        public QueueId(FileChannel metaFileChannel, int totalNum)throws IOException{
            this.metaFileChannel = metaFileChannel;
            // should consider atomic
            this.metaDataOffset = META_FREE_OFFSET;

            META_FREE_OFFSET += totalNum * (Long.BYTES + Integer.BYTES) + Integer.BYTES;

            this.currentNum = 0;
            ByteBuffer tmp = ByteBuffer.allocate(Integer.BYTES);
            tmp.putInt(this.currentNum);
            metaFileChannel.write(tmp, metaDataOffset);
            this.queueIdArray = this.metaDataOffset + Integer.BYTES;
        }
        public QueueId(FileChannel metaFileChannel, Long metaDataOffset)throws IOException{
            this.metaFileChannel = metaFileChannel;
            this.metaDataOffset = metaDataOffset;
            this.queueIdArray = metaDataOffset + Integer.BYTES;

            // 恢复 currentNum
            ByteBuffer[] tmp =  new ByteBuffer[1];
            tmp[0] = ByteBuffer.allocate(Integer.BYTES);
            metaFileChannel.read(tmp, metaDataOffset.intValue(), Integer.BYTES);
            this.currentNum = tmp[0].getInt();

        }
        public Long put(int queueId, Long dataMetaOffset)throws IOException{
            Long offset = queueIdArray + currentNum * (Integer.BYTES + Long.BYTES);

            ByteBuffer tmpData = ByteBuffer.allocate(Integer.BYTES + Long.BYTES);
            tmpData.putInt(queueId);
            tmpData.putLong(Integer.BYTES, dataMetaOffset);
            metaFileChannel.write(tmpData, offset);
            // 写回 SSD
            currentNum++;
            ByteBuffer tmp = ByteBuffer.allocate(Integer.BYTES);
            tmp.putInt(this.currentNum);
            metaFileChannel.write(tmp, metaDataOffset);
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
                fileChannel.read(tmp, this.queueIdArray + i*(Integer.BYTES + Long.BYTES));
                res.put(tmp.getInt(), tmp.getLong(Integer.BYTES));
            }
            return res;
        }
    }
    /*
    * 目前先考虑写入 SSD，而不考虑使用 DRAM 优化，所以先存所有的数据。
    */
    private class Data{
        long totalNum; // 存
        FileChannel fileChannel;
        long tail; // 存
        long head; // 存
        long metaOffset;

        public Data(FileChannel fileChannel) throws IOException{
            this.metaOffset = FREE_OFFSET;

            FREE_OFFSET += Long.BYTES * 3;

            this.totalNum = 0L;
            this.tail = this.metaOffset + Long.BYTES * 3;
            this.head = this.tail;

            ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES * 3);
            tmp.putLong(totalNum);
            tmp.putLong(Long.BYTES, head);
            tmp.putLong(Long.BYTES * 2, tail);


        }

        public Data(FileChannel fileChannel, Long metaOffset) throws IOException{
            this.fileChannel = fileChannel;
            this.metaOffset = metaOffset;
            
            // 恢复 totalNum, tail, head
            ByteBuffer[] tmp = new ByteBuffer[1];
            tmp[0] = ByteBuffer.allocate(Integer.BYTES + Long.BYTES + Long.BYTES);
            fileChannel.read(tmp, metaOffset.intValue(), Integer.BYTES + Long.BYTES + Long.BYTES);
            this.totalNum = tmp[0].getLong();
            this.head = tmp[0].getLong(Integer.BYTES);
            this.tail = tmp[0].getLong(Integer.BYTES + Integer.BYTES);
        }
        public Long put(ByteBuffer data) throws IOException{
            long startOffset = FREE_OFFSET;
            // 需要原子增加 FREE_OFFSET
            int tmpSize = Long.BYTES + Long.BYTES + data.array().length;
            FREE_OFFSET += tmpSize;

            long nextOffset = -1L;
            ByteBuffer byteData = ByteBuffer.allocate(tmpSize);
            byteData.putLong(0, tmpSize - (Long.BYTES + Long.BYTES));
            byteData.putLong(Long.BYTES, nextOffset);
            byteData.put(Long.BYTES + Long.BYTES, data, 0, data.array().length);
            fileChannel.write(byteData,startOffset);


            // 更新 tail
            ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES);
            tmp.putLong(0, startOffset);
            fileChannel.write(tmp, tail + Long.BYTES);
            tail = startOffset;

            totalNum++; // 需要写入 SSD ? 写吧

            return totalNum;
        }
        public Map<Integer, ByteBuffer> getRange(Long offset, int fetchNum) throws IOException{
            Long startOffset = head;
            Map<Integer, ByteBuffer> res = new HashMap<>();
            //ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES);
            for(int i=0; i<fetchNum && startOffset != -1L; ++i){
                Long length = startOffset + Long.BYTES;
                // TODO: fileChannel.read 
                ByteBuffer[] tmp =  new ByteBuffer[1];
                tmp[0] = ByteBuffer.allocate(Long.BYTES);
                fileChannel.read(tmp, length.intValue(), Long.BYTES);
                startOffset = tmp[0].getLong();
            }

            for(int i=0; i<fetchNum && startOffset != -1L; ++i){
                ByteBuffer[] tmp =  new ByteBuffer[1];
                tmp[0] = ByteBuffer.allocate(Long.BYTES + Long.BYTES);
                fileChannel.read(tmp, startOffset.intValue(), Long.BYTES + Long.BYTES);

                Long dataSize = tmp[0].getLong();
                Long nextOffset = tmp[0].getLong(Long.BYTES);

                ByteBuffer[] tmp1 =  new ByteBuffer[1];
                tmp1[0] = ByteBuffer.allocate(dataSize.intValue());
                fileChannel.read(tmp1, startOffset.intValue() + Long.BYTES + Long.BYTES, dataSize.intValue());
                res.put(i, tmp1[0]);

                startOffset = nextOffset;

            }
            return res;
        }
        public Long getMetaOffset(){
            return this.metaOffset;
        }
    }
}
