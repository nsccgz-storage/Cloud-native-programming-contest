package io.openmessaging;

import java.io.IOException;
import java.lang.System.Logger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SSDqueue{
    static volatile  Long FREE_OFFSET = 0L;
    static volatile  Long META_FREE_OFFSET = 0L;

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
        this.metaFileChannel = metaFileChannel;
        this.topicNameQueueMetaMap = new ConcurrentHashMap<>();
        this.queueTopicMap = new ConcurrentHashMap<>();
        this.topicArrayOffset = 0L + Integer.BYTES;

        ByteBuffer tmp = ByteBuffer.allocate(Integer.BYTES);
        metaFileChannel.read(tmp);
        tmp.flip();

        this.currentNum = tmp.getInt();

        //System.out.println("55 " + this.currentNum);
 
        Long startOffset = 0L + Integer.BYTES;
        for(int i=0; i<this.currentNum; i++){
    
            Long offset = startOffset + i*(Long.BYTES + TOPIC_NAME_SZIE);
            tmp = ByteBuffer.allocate(Long.BYTES);
            int len = metaFileChannel.read(tmp, offset);
            tmp.flip();
            //System.out.println("65: " + len);

            Long queueMetaOffset = tmp.getLong();

            tmp = ByteBuffer.allocate(TOPIC_NAME_SZIE);
            metaFileChannel.read(tmp,offset + Long.BYTES);
            tmp.flip();

            String topicName = new String(tmp.array()).trim();

            //System.out.println("75： " + queueMetaOffset);

            topicNameQueueMetaMap.put(topicName, queueMetaOffset);
            
            // 遍历每个 topic 下的 queue
            queueTopicMap.put(topicName, readQueue(queueMetaOffset));

        }


    }
    public Map<Integer, Long> readQueue(Long queueMetaOffset) throws IOException{
        QueueId resData = new QueueId(metaFileChannel, queueMetaOffset);
        return resData.readAll();

    }
    public Long setTopic(String topicName, int queueId, ByteBuffer data){
    
        try {
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
                tmp.put(topicName.getBytes(), 0, topicName.length());
                tmp.flip();
                metaFileChannel.write(tmp, this.topicArrayOffset + currentNum * (TOPIC_NAME_SZIE + Long.BYTES));
                currentNum++;

                tmp.clear();
                tmp = ByteBuffer.allocate(Integer.BYTES);
                tmp.putInt(currentNum);
                tmp.flip();
                int len = metaFileChannel.write(tmp, 0L);

                //System.out.println("110: " + len);

                // 更新 DRAM map
                topicData = new HashMap<>();
                topicData.put(queueId, writeData.getMetaOffset());
                topicNameQueueMetaMap.put(topicName, queueArray.getMetaOffset());
                queueTopicMap.put(topicName, topicData);

                //System.out.println("112: w meta: "+ writeData.getMetaOffset());

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
                    topicData.put(queueId, writeData.getMetaOffset());
                    queueTopicMap.put(topicName, topicData);
                    
                    return res; 
                }else{
                    Data writeData = new Data(fileChannel, metaDataOffset);
                    return writeData.put(data);
                }
            }
        } catch (Exception e) {
            //TODO: handle exception
            return null;
        }
    }
    public Map<Integer, ByteBuffer> getRange(String topicName, int queueId, Long offset, int fetchNum){
        try{
            Map<Integer, Long> topicData = queueTopicMap.get(topicName);
            if(topicData == null) return null;
            Long metaDataOffset = topicData.get(queueId);
            if(metaDataOffset == null) return null;

            //System.out.println("143: r meta: "+ metaDataOffset);

            Data resData = new Data(fileChannel, metaDataOffset);
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
            // should consider atomic
            this.metaDataOffset = META_FREE_OFFSET;

            META_FREE_OFFSET += totalNum * (Long.BYTES + Integer.BYTES) + Integer.BYTES;

            this.currentNum = 0;
            ByteBuffer tmp = ByteBuffer.allocate(Integer.BYTES);
            tmp.putInt(this.currentNum);
            tmp.flip();
            metaFileChannel.write(tmp, metaDataOffset);
            this.queueIdArray = this.metaDataOffset + Integer.BYTES;
        }
        public QueueId(FileChannel metaFileChannel, Long metaDataOffset)throws IOException{
            this.metaFileChannel = metaFileChannel;
            this.metaDataOffset = metaDataOffset;
            this.queueIdArray = metaDataOffset + Integer.BYTES;

            // 恢复 currentNum
            ByteBuffer tmp =  ByteBuffer.allocate(Integer.BYTES);
            metaFileChannel.read(tmp, Integer.BYTES);
            tmp.flip();

            this.currentNum = tmp.getInt();

            //System.out.println("194: r " + this.toString());

        }
        public Long put(int queueId, Long dataMetaOffset)throws IOException{
            Long offset = queueIdArray + currentNum * (Integer.BYTES + Long.BYTES);

            ByteBuffer tmpData = ByteBuffer.allocate(Integer.BYTES + Long.BYTES);
            tmpData.putInt(queueId);
            tmpData.putLong(dataMetaOffset);
            tmpData.flip();
            metaFileChannel.write(tmpData, offset);
            // 写回 SSD
            currentNum++;
            ByteBuffer tmp = ByteBuffer.allocate(Integer.BYTES);
            tmp.putInt(this.currentNum);
            tmp.flip();
            metaFileChannel.write(tmp, metaDataOffset);
            this.queueIdArray = this.metaDataOffset + Integer.BYTES;

            //System.out.println("w 213: " + this.toString());

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

                //System.out.println("222: " + len2);

                tmp.flip();
                res.put(tmp.getInt(), tmp.getLong());
                tmp.flip();
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
            this.metaOffset = FREE_OFFSET;
            this.fileChannel = fileChannel;
            FREE_OFFSET += Long.BYTES * 3;

            this.totalNum = 0L;
            this.tail = -1L;
            this.head = this.tail;

            ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES * 3);
            tmp.putLong(totalNum);
            tmp.putLong(head);
            tmp.putLong(tail);
            tmp.flip();
            fileChannel.write(tmp, this.metaOffset);
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
            long startOffset = FREE_OFFSET;
            // 需要原子增加 FREE_OFFSET
            int tmpSize = Long.BYTES + Long.BYTES + data.array().length;
            FREE_OFFSET += tmpSize;

            long nextOffset = -1L;
            ByteBuffer byteData = ByteBuffer.allocate(tmpSize);
            byteData.putLong(tmpSize - (Long.BYTES + Long.BYTES));
            byteData.putLong(nextOffset);
            byteData.put(data);
            byteData.flip();
            int lens = fileChannel.write(byteData,startOffset);

            if(tail == -1L){
                tail = startOffset;
                head = tail;

            }else{
                ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES);
                tmp.putLong(startOffset);
                tmp.flip();
                fileChannel.write(tmp, tail + Long.BYTES); // 更新 nextOffset
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
            //System.out.println("w: " + this.toString() + " : " + len);

            return totalNum;
        }
        public Map<Integer, ByteBuffer> getRange(Long offset, int fetchNum) throws IOException{
            Long startOffset = head;
            Map<Integer, ByteBuffer> res = new HashMap<>();
            //ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES);
            for(int i=0; i<offset && startOffset != -1; ++i){
                Long nextOffset = startOffset + Long.BYTES;
                // TODO: fileChannel.read 
                ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES);
                int len = fileChannel.read(tmp, nextOffset);
                tmp.flip();    
                startOffset = tmp.getLong();
                
            }

            for(int i=0; i<fetchNum && startOffset != -1L; ++i){
                ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES + Long.BYTES);
                fileChannel.read(tmp, startOffset);
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
