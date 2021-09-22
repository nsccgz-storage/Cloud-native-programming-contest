import java.nio.ByteBuffer;
import java.util.Map;

import com.intel.pmem.llpl.TransactionalHeap;

public class Main {
    public static void main(String args[]) {
        System.out.println("Hello World!");

        String path = "/home/ubuntu/ContestForAli/pmem_test_llpl/messageQueue";
        
        boolean initialized  = TransactionalHeap.exists(path);
        TransactionalHeap heap = initialized ? TransactionalHeap.openHeap(path) : TransactionalHeap.createHeap(path, 100_000_000);
        if(!initialized){
            TopicId queueMessage = new TopicId(heap);
            heap.setRoot(queueMessage.getHandle());
        }else{
            Long metaHandle = heap.getRoot();
            TopicId queueMessage = new TopicId(heap, metaHandle);
            
            System.out.println("has one");
            ByteBuffer data = ByteBuffer.wrap("10008611".getBytes());
            queueMessage.setTopic("wyk", "1002", data);
            
            System.out.println(queueMessage);
            Map<Integer, ByteBuffer> tmpRes = queueMessage.getRange("wyk", "1002", 0L, 9);
            
            for(Map.Entry<Integer, ByteBuffer> entry: tmpRes.entrySet()){
                String t = new String(entry.getValue().array());
                System.out.println("key: "+ entry.getKey() + " value: " + t);
            }
        }
        

        System.out.println("succ");

    }

}

