import com.intel.pmem.llpl.TransactionalHeap;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageQueueImpl extends MessageQueue {
    ConcurrentHashMap<String, Map<Integer, Queue>> topics = new ConcurrentHashMap<>();

    private static String path = "/home/wangxr/pmem_test_llpl"; // pmem directory 
    private static TransactionalHeap heap; 

    // Initialization
    public DefaultMessageQueueImpl(){
        if(TransactionalHeap.exists(path)){
            heap = TransactionalHeap.openHeap(path);
        }
        else heap = TransactionalHeap.createHeap(path, 102400); // heap size = 100 MB
    }


    @Override
    public long append(String topic, int queueId, ByteBuffer data){
        Map<Integer, Queue> topicOffset = topics.get(topic);
        if(topicOffset == null){
            topicOffset = new HashMap<>();
            topics.put(topic, topicOffset);
        }

        Queue q = topicOffset.get(queueId);
        if(q == null){
            q = new Queue(heap);
            topicOffset.put(queueId, q);
        }

        return q.put(data);
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        Map<Integer, Queue> topicOffset = topics.get(topic);
        Queue q = topicOffset.get(queueId);
        if(q != null) {
            return q.fetch(offset, fetchNum);
        }
        return new HashMap<>();
    }
}
