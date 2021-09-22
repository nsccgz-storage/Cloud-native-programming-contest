import java.nio.ByteBuffer;

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

            byte[] tmpRes = queueMessage.getRange("wyk", "1002", 0L, 1).array();
            String res = new String(tmpRes);

            System.out.println(res);
        }
        

        System.out.println("succ");

    }

}

