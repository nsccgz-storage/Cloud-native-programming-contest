package io.openmessaging;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import com.intel.pmem.llpl.TransactionalHeap;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.File;

import io.openmessaging.SSDBench;


/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageQueueImpl extends MessageQueue {
    // Initialization

    private TopicId queueMessage;
    private String ssdBenchPath;
    private FileChannel ssdBenchFileChannel;
    private long ssdBenchTotalSize;

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

    public DefaultMessageQueueImpl(){
        // for SSD Benchmark
		try {
            // ssdBenchPath = "./ssdBench";
            ssdBenchPath = "/essd/bench";
            ssdBenchFileChannel = new RandomAccessFile(new File(ssdBenchPath), "rw").getChannel();
            ssdBenchTotalSize = 256L*1024L*1024L; //256MiB 
		} catch(IOException ie) {
			ie.printStackTrace();
		}  
    }

    @Override
    public long append(String topic, int queueId, ByteBuffer data){
        // return queueMessage.setTopic(topic, queueId, data);

        // for SSD Benchmark
		try {
            int[] ioSizes = {4*1024, 8*1024, 16*1024, 32*1024, 64*1024, 128*1024, 256*1024, 512*1024,1024*1024};
            for (int t = 1; t <= 50; t+=1){
                for (int i = 0; i < ioSizes.length; i++){
                    SSDBench.benchFileChannelWriteThreadPoolRange(ssdBenchFileChannel, ssdBenchTotalSize, t, ioSizes[i]);
                }
            }
		} catch(IOException ie) {
			ie.printStackTrace();
		}  
        return 0;

    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        return null;
    }
}
