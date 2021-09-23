import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.intel.pmem.llpl.TransactionalHeap;
import com.intel.pmem.llpl.TransactionalMemoryBlock;

public class TopicId {

    int topicNum = 100;
    int topicNameSize = 512;
    int queueIdNum = 10000;

    long metaDataHandle; // int + long
    TransactionalMemoryBlock topicIdArray;
    int currentNum;
    TransactionalHeap heap;

    public TopicId(TransactionalHeap heap, long metaDataHandle){
        this.metaDataHandle = metaDataHandle;
        TransactionalMemoryBlock metaBlock = heap.memoryBlockFromHandle(metaDataHandle);
        currentNum = metaBlock.getInt(0);
        metaDataHandle = metaBlock.getLong(Integer.BYTES);
        this.topicIdArray = heap.memoryBlockFromHandle(metaDataHandle);
        this.heap = heap;
    }

    public TopicId(TransactionalHeap heap){
        TransactionalMemoryBlock metaBlock = heap.allocateMemoryBlock(Integer.BYTES + Long.BYTES);
        this.metaDataHandle = metaBlock.handle();

        this.topicIdArray = heap.allocateMemoryBlock(topicNum * (topicNameSize + Long.BYTES));
        currentNum = 0;
        this.heap = heap;

        metaBlock.setInt(0, currentNum);
        metaBlock.setLong(Integer.BYTES, this.topicIdArray.handle());
    }

    public long getHandle(){
        return this.metaDataHandle;
    }

    public long setTopic(String topicName, int queueId, ByteBuffer data){ // 需要考虑初始化的情况
        int offset = find(topicName);
        long resOffset;
        if(offset == -1){ // add new one topic
            StringBuilder tmp = new StringBuilder(topicName);
            char[] help = new char[topicNameSize-topicName.length() + 1];
            for(int i=0;i<help.length; ++i){
                help[i] = ' ';
            }
            tmp.append(help);
            
            topicIdArray.copyFromArray(tmp.toString().getBytes(), 0, currentNum*(topicNameSize + Long.BYTES), topicNameSize);
            QueueId queue = new QueueId(heap, queueIdNum);
            resOffset = queue.put(queueId, data);
            topicIdArray.setLong(currentNum*(topicNameSize + Long.BYTES) + topicNameSize, queue.getHandle());
            
            currentNum++;

            //System.out.println(new Integer(currentNum).toString() +  "@56");

            TransactionalMemoryBlock metaBlock = heap.memoryBlockFromHandle(metaDataHandle);
            metaBlock.setInt(0, currentNum);
            metaBlock.setLong(Integer.BYTES, topicIdArray.handle());
            
        }else{ // add to a topic queue which exsits;
            QueueId queue = new QueueId(heap, topicIdArray.getLong(offset + topicNameSize));
            resOffset = queue.put(queueId, data);
            topicIdArray.setLong(offset+topicNameSize, queue.getHandle());
        }
        return resOffset;
    }

    // return offset 
    public int find(String topicName){
        
        int offset = 0;
        int length = topicNameSize + Long.BYTES;
        for(int i=0; i<currentNum; i++){
            byte[] tmpData = new byte[length];
            topicIdArray.copyToArray(offset, tmpData, 0, topicNameSize);
            String tmpName = new String(tmpData).trim();

            if(tmpName.equals(topicName)){
                return offset;
            }
            offset += length;
        }
        return -1;
    }

    public String toString(){
        return "topic_num: " + currentNum;
    }

    public Map<Integer, ByteBuffer> getRange(String topicName, int queueId, Long offset, int fetchNum){
        int place = find(topicName);
        if(place == -1) return null;
        QueueId queue = new QueueId(heap, topicIdArray.getLong(place + topicNameSize));
        return queue.getRange(queueId, offset, fetchNum);
    }


    /*
        storage layout:
            - handle : long
            - name : int
    */
    private class QueueId{
        //int queueIdSize = 128; // pre set

        TransactionalMemoryBlock queueIdArray; 
        int currentNum = 0;
        Long metaDataHandle;
        TransactionalHeap heap;

        public QueueId(TransactionalHeap heap, int nums){
            this.heap = heap;
            TransactionalMemoryBlock metaDataBlock =  heap.allocateMemoryBlock(Integer.BYTES + Long.BYTES);
            this.metaDataHandle = metaDataBlock.handle();
            queueIdArray = heap.allocateMemoryBlock(nums * (Integer.BYTES + Long.BYTES));
            currentNum = 0;

            metaDataBlock.setInt(0,currentNum); // currentNum;
            metaDataBlock.setLong(Integer.BYTES, queueIdArray.handle());
           
        }
        public QueueId(TransactionalHeap heap, Long metaDataHandle){
            this.heap = heap;
            this.metaDataHandle = metaDataHandle;
            TransactionalMemoryBlock metaDataBlock = heap.memoryBlockFromHandle(metaDataHandle);
            this.currentNum = metaDataBlock.getInt(0);
            this.queueIdArray = heap.memoryBlockFromHandle(metaDataBlock.getLong(Integer.BYTES));

        }
    
        public long getHandle(){
            return metaDataHandle;
        }

        long put(int queueId, ByteBuffer data){
            int offset = find(queueId);
            long resOffset;
            if(offset == -1){ // add new one
                queueIdArray.setInt(currentNum *(Integer.BYTES + Long.BYTES) + Long.BYTES, queueId);
                Data helpData = new Data(heap);
                resOffset = helpData.put(data);
                queueIdArray.setLong(currentNum *(Integer.BYTES + Long.BYTES), helpData.getHandle());
                currentNum++;

                TransactionalMemoryBlock metaDataBlock = heap.memoryBlockFromHandle(metaDataHandle);
                metaDataBlock.setInt(0, currentNum);

            }else{ // 
                Long dataHandle = queueIdArray.getLong(offset);
                Data helpData = new Data(heap, dataHandle);
                resOffset = helpData.put(data);
            }
            return resOffset;
        }

        int find(int queueId){
            int offset = 0;
            for(int i=0; i<currentNum; ++i){
                int id = queueIdArray.getInt(offset + Long.BYTES);
                if(id == queueId) return offset;
                offset += Integer.BYTES + Long.BYTES;
            }
            return -1;
        }
        public Map<Integer, ByteBuffer> getRange(int queueId, Long offset, int fetchNum){
            int  place = find(queueId);
            if(place == -1) return null;
            Long dataHandle = queueIdArray.getLong(place);
            Data helpData = new Data(heap, dataHandle);

            return helpData.getRange(offset, fetchNum);
        }
        public String toString(){
            return "queue_num: " +  currentNum;
        }
    }
    /*
    * 数据使用链式存储进行，保留一个尾指针，方便插入
    */
    private class Data{
        // head 和 tail 要考虑持久化
        Long head;  
        Long tail;
        Long totalNum;
        TransactionalHeap heap;
        Long metaDataHandle;

        public Data(TransactionalHeap heap, Long metaDataHandle){
            this.heap = heap;
            TransactionalMemoryBlock metaData = heap.memoryBlockFromHandle(metaDataHandle);
            this.head = metaData.getLong(0);
            this.tail = metaData.getLong(Long.BYTES);
            this.totalNum = metaData.getLong(Long.BYTES + Long.BYTES);
            this.metaDataHandle = metaDataHandle;
        }
        public Data(TransactionalHeap heap){
            this.heap = heap;
            TransactionalMemoryBlock metaData = heap.allocateMemoryBlock(Long.BYTES + Long.BYTES + Long.BYTES);
            metaData.setLong(0, -1L);
            metaData.setLong(Long.BYTES, -1);
            metaData.setLong(Long.BYTES + Long.BYTES, 0L);
            this.head = -1L;
            this.tail = -1L;
            this.totalNum = 0L;
            this.metaDataHandle = metaData.handle();

        }
        public long put(ByteBuffer data){
            // 在链表表尾插入
            TransactionalMemoryBlock memoryBlock = heap.allocateMemoryBlock(data.array().length + Long.BYTES);
            memoryBlock.setLong(0, -1L);
            memoryBlock.copyFromArray(data.array(), 0, Long.BYTES, data.array().length);
            if(tail == -1L){
                tail = memoryBlock.handle(); 
                head = tail;
                TransactionalMemoryBlock metaData = heap.memoryBlockFromHandle(metaDataHandle);
                metaData.setLong(0, head);
                metaData.setLong(Long.BYTES, tail);
                this.totalNum++;
                metaData.setLong(Long.BYTES + Long.BYTES, this.totalNum);
                //System.out.println("192_topicId");
            }else{
                TransactionalMemoryBlock tmp = heap.memoryBlockFromHandle(tail);
                tmp.setLong(0, memoryBlock.handle());
                tail = memoryBlock.handle();
                TransactionalMemoryBlock metaData = heap.memoryBlockFromHandle(metaDataHandle);
                metaData.setLong(Long.BYTES, tail);
                this.totalNum++;
                metaData.setLong(Long.BYTES + Long.BYTES, this.totalNum);
                //System.out.println("199_topicId");
            }     
            return this.totalNum;
        }

        public Long getHandle(){
            return this.metaDataHandle;
        }
        
        public Map<Integer, ByteBuffer>  getRange(Long offset, int fetchNum){
            //fetchNum = 1; // just for test
            Long startHandle = head;
            Map<Integer, ByteBuffer> res = new HashMap<>();
            for(int i=0;i<offset && startHandle != -1L; ++i){
                TransactionalMemoryBlock block =  heap.memoryBlockFromHandle(startHandle);
                startHandle = block.getLong(0);
            }
            
            for(int i=0; i<fetchNum && startHandle != -1L; ++i){
                TransactionalMemoryBlock block =  heap.memoryBlockFromHandle(startHandle);
                 // 10000 is just for test;
                Long length = block.size() - Long.BYTES;
                byte[] tmp = new byte[length.intValue()];
                block.copyToArray(Long.BYTES, tmp, 0,  length.intValue());
                res.put(i, ByteBuffer.wrap(tmp));

                startHandle = block.getLong(0);
            }
            return res;
        }
    }
}

