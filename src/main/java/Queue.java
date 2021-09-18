import com.intel.pmem.llpl.Range;
import com.intel.pmem.llpl.TransactionalHeap;
import com.intel.pmem.llpl.TransactionalMemoryBlock;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/*
 * 先实现简单的版本。
 * 写方法 put 和 读方法 fetch 都是直接与持久化内存进行操作。
 * 未完成把数据保存在 SSD
 */
public class Queue{
    private long max_offset = 0L;
    TransactionalHeap heap;
    ConcurrentHashMap<Long, Long> handles;
    DataBlock old_block;

    Queue(TransactionalHeap heap){
        this.heap = heap;
        handles = new ConcurrentHashMap<>();
    }

    // put data to the queue
    long put(ByteBuffer data){
        DataBlock block = new DataBlock(heap, data);
        handles.put(max_offset, block.handle());
        if(max_offset > 0){
            old_block.setNextHandle(block.handle());
        }
        old_block = block;
        max_offset++;
        return max_offset;
    }

    long getOffset(){ return max_offset;}

    // 从 start_offset 开始取 fetch_num 份数据
    // 返回Map中的key为offset（offset为消息在Map中的顺序偏移，从0开始），value为对应的写入data
    Map<Integer, ByteBuffer> fetch(long start_offset, int fetch_num){
        Map<Integer, ByteBuffer> result = new HashMap<>();
        DataBlock data_block; //= DataBlock.fromHandle(heap, handles.get(start_offset));
        for(int i = 0;i < fetch_num && start_offset+i <= max_offset;i++){
            data_block = DataBlock.fromHandle(heap,handles.get(start_offset+i));
            result.put(i, data_block.getData());
            //data_block = DataBlock.fromHandle(heap, data_block.getNextHandle());
        }
        return result;
    }

    /**
     * 实现一个内部类，对数据进行封装。
     * 保存在 pmem 上的数据块格式为{下一个块的handle（long, 8B), 数据大小(int ,4B)，数据(bytes)}
     */
    private static class DataBlock{
        private static final long NEXT_HANDLE_OFFSET = 0;
        private static final long DATA_SIZE_OFFSET = 8;
        private static final long DATA_OFFSET = 12;
        public TransactionalMemoryBlock block;

        public DataBlock(TransactionalHeap heap, ByteBuffer data) {
            int size = data.remaining();
            byte[] data_bytes = new byte[size];
            data.get(data_bytes);
            block = heap.allocateMemoryBlock(data.remaining(), (Range range) -> {
                range.setLong(NEXT_HANDLE_OFFSET, 0); // TODO: check handle == 0 means no handle
                range.setInt(DATA_SIZE_OFFSET, size);
                range.copyFromArray(data_bytes, 0, DATA_OFFSET, size);
            });
        }
        public DataBlock(TransactionalMemoryBlock block){
            this.block = block;
        }

        public long handle() {
            return block.handle();
        }

        public long getNextHandle(){
            return block.getLong(NEXT_HANDLE_OFFSET);
        }

        public ByteBuffer getData(){
            int length = block.getInt(DATA_SIZE_OFFSET);
            byte[] data_bytes = new byte[length];
            block.copyToArray(DATA_OFFSET, data_bytes, 0, length);
            ByteBuffer byte_buffer = ByteBuffer.allocate(length); // HeapByteBuffer 
            byte_buffer.put(data_bytes);
            return byte_buffer;
        }

        public void setNextHandle(long handle){
            block.setLong(NEXT_HANDLE_OFFSET, handle); // Automatically add to transaction
        }

        public static DataBlock fromHandle(TransactionalHeap heap, long handle) {
            return new DataBlock(heap.memoryBlockFromHandle(handle));
        }
    }
}