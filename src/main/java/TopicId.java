import java.nio.ByteBuffer;
import com.intel.pmem.llpl.TransactionalHeap;
import com.intel.pmem.llpl.TransactionalMemoryBlock;

public class TopicId {

    long metaDataHandle; // int + long
    TransactionalMemoryBlock topicIdArray;
    int currentNum;
    TransactionalHeap heap;

    int topicNum = 100;
    int topicNameSize = 512;
    int queueIdNum = 10000;

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

    public void setTopic(String topicName, String queueId, ByteBuffer data){ // 需要考虑初始化的情况
        int offset = find(topicName);

        if(offset == -1){ // add new one topic
            StringBuilder tmp = new StringBuilder(topicName);
            char[] help = new char[topicNameSize-queueId.length() + 1];
            for(int i=0;i<help.length; ++i){
                help[i] = ' ';
            }
            tmp.append(help);
            
            topicIdArray.copyFromArray(tmp.toString().getBytes(), 0, currentNum*(topicNameSize + Long.BYTES), topicNameSize);
            QueueId queue = new QueueId(heap, queueIdNum);
            queue.put(queueId, data);
            topicIdArray.setLong(currentNum*(topicNameSize + Long.BYTES) + topicNameSize, queue.getHandle());
            
            currentNum++;

            //System.out.println(new Integer(currentNum).toString() +  "@56");

            TransactionalMemoryBlock metaBlock = heap.memoryBlockFromHandle(metaDataHandle);
            metaBlock.setInt(0, currentNum);
            metaBlock.setLong(Integer.BYTES, topicIdArray.handle());
            
        }else{ // add to a topic queue which exsits;
            QueueId queue = new QueueId(heap, topicIdArray.getLong(offset + topicNameSize));
            queue.put(queueId, data);
            topicIdArray.setLong(offset+topicNameSize, queue.getHandle());
        }
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

    public ByteBuffer getRange(String topicName, String queueId, Long offset, int fetchNum){
        int place = find(topicName);
        if(place == -1) return null;
        QueueId queue = new QueueId(heap, topicIdArray.getLong(place + topicNameSize));
        return queue.getRange(queueId, offset, fetchNum);
    }


    /*
        storage layout:
            - handle : long
            - name : byte[]
    */
    private class QueueId{
        TransactionalMemoryBlock queueIdArray; 
        int queueIdSize = 128; // pre set
        int currentNum = 0;
        Long metaDataHandle;
        TransactionalHeap heap;

        public QueueId(TransactionalHeap heap, int nums){
            this.heap = heap;
            TransactionalMemoryBlock metaDataBlock =  heap.allocateMemoryBlock(Integer.BYTES + Long.BYTES);
            this.metaDataHandle = metaDataBlock.handle();
            queueIdArray = heap.allocateMemoryBlock(nums * (queueIdSize + Long.BYTES));
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

        void put(String queueId, ByteBuffer data){
            int offset = find(queueId);
            if(offset == -1){ // add new one
                StringBuilder tmp = new StringBuilder(queueId);
                char[] help = new char[queueIdSize-queueId.length()+2];
                for(int i=0;i<help.length; ++i){
                    help[i] = ' ';
                }
                tmp.append(help);
                
                //System.out.println("165@ " + tmp.toString().length());
                //System.out.println("165@ " + tmp.toString());

                queueIdArray.copyFromArray(queueId.getBytes(), 0, currentNum *(queueIdSize + Long.BYTES) + Long.BYTES, queueId.length()); //这里可以会有 bug
                Data helpData = new Data(heap);
                helpData.put(data);
                queueIdArray.setLong(currentNum *(queueIdSize + Long.BYTES), helpData.getHandle());
                currentNum++;

                TransactionalMemoryBlock metaDataBlock = heap.memoryBlockFromHandle(metaDataHandle);
                metaDataBlock.setInt(0, currentNum);

            }else{ // 
                Long dataHandle = queueIdArray.getLong(offset);
                Data helpData = new Data(heap, dataHandle);
                helpData.put(data);
            }
        }

        int find(String queueId){
            int offset = 0;
            for(int i=0; i<currentNum; ++i){
                byte[] tmp = new byte[queueIdSize];
                
                queueIdArray.copyToArray(offset + Long.BYTES, tmp, 0, queueIdSize);


                String name = new String(tmp).trim();

                //System.out.println("172@ " + name);
                //System.out.println("173@ " + queueId);

                if(name.equals(queueId)) return offset;
                offset += queueIdSize + Long.BYTES;
            }
            return -1;
        }
        public ByteBuffer getRange(String queueId, Long offset, int fetchNum){
            int  place = find(queueId);
            
            //System.out.println("178@: " + place);

            if(place == -1) return null;
            Long dataHandle = queueIdArray.getLong(place);
            Data helpData = new Data(heap, dataHandle);

            //System.out.println("152_topicId");

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
        TransactionalHeap heap;
        Long metaDataHandle;

        public Data(TransactionalHeap heap, Long metaDataHandle){
            this.heap = heap;
            TransactionalMemoryBlock metaData = heap.memoryBlockFromHandle(metaDataHandle);
            this.head = metaData.getLong(0);
            this.tail = metaData.getLong(Long.BYTES);
            this.metaDataHandle = metaDataHandle;
        }
        public Data(TransactionalHeap heap){
            this.heap = heap;
            TransactionalMemoryBlock metaData = heap.allocateMemoryBlock(Long.BYTES + Long.BYTES);
            metaData.setLong(0, -1L);
            metaData.setLong(Long.BYTES, -1);
            this.head = -1L;
            this.tail = -1L;
            this.metaDataHandle = metaData.handle();

        }
        public void put(ByteBuffer data){
            // 在链表表尾插入
            TransactionalMemoryBlock memoryBlock = heap.allocateMemoryBlock(data.array().length + Long.BYTES);
            memoryBlock.setLong(0, -1L);
            memoryBlock.copyFromArray(data.array(), 0, Long.BYTES, data.array().length);
            if(tail == -1){
                tail = memoryBlock.handle(); 
                head = tail;
                TransactionalMemoryBlock metaData = heap.memoryBlockFromHandle(metaDataHandle);
                metaData.setLong(0, head);
                metaData.setLong(Long.BYTES, tail);
                //System.out.println("192_topicId");
            }else{
                TransactionalMemoryBlock tmp = heap.memoryBlockFromHandle(tail);
                tmp.setLong(0, memoryBlock.handle());
                tail = memoryBlock.handle();
                TransactionalMemoryBlock metaData = heap.memoryBlockFromHandle(metaDataHandle);
                metaData.setLong(Long.BYTES, tail);
                //System.out.println("199_topicId");
            }     
        }

        public Long getHandle(){
            return this.metaDataHandle;
        }
        
        public ByteBuffer getRange(Long offset, int fetchNum){
            fetchNum = 1; // just for test
            Long startHandle = head;
            for(int i=0;i<offset && i<currentNum; ++i){
                TransactionalMemoryBlock block =  heap.memoryBlockFromHandle(startHandle);
                startHandle = block.getLong(0);
            }
            byte[] tmp = new byte[10000];
            for(int i=0; i<fetchNum && i+offset < currentNum; ++i){
                TransactionalMemoryBlock block =  heap.memoryBlockFromHandle(startHandle);
                 // 10000 is just for test;
                Long length = block.size() - Long.BYTES;
                block.copyToArray(Long.BYTES, tmp, 0,  length.intValue());
            }
            //System.out.println("221_topicId");
            return ByteBuffer.wrap(tmp);
        }
    }
}

