import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.intel.pmem.llpl.Transaction;
import com.intel.pmem.llpl.TransactionalHeap;


/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageQueueImpl extends MessageQueue {
    // Initialization

    private TopicId queueMessage;

    public DefaultMessageQueueImpl(String path){

        // String pmemPath = path;
        String pmemPath = "/home/ubuntu/ContestForAli/pmem_test_llpl/messageQueue";
        
        boolean initialized  = TransactionalHeap.exists(pmemPath);
        TransactionalHeap heap = initialized ? TransactionalHeap.openHeap(pmemPath) : TransactionalHeap.createHeap(pmemPath, 100_000_000);
        if(!initialized){
            queueMessage = new TopicId(heap);
            heap.setRoot(queueMessage.getHandle());
        }else{
            Long metaHandle = heap.getRoot();
            queueMessage = new TopicId(heap, metaHandle);
        }

    }


    @Override
    public long append(String topic, int queueId, ByteBuffer data){
        return 0L;
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        return null;
    }
}
