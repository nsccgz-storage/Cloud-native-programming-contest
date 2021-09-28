package io.openmessaging;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import com.intel.pmem.llpl.TransactionalHeap;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.File;

import io.openmessaging.SSDBench;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
// import java.util.concurrent.locks.ReentrantReadWriteLock;


// import org.apache.log4j.spi.LoggerFactory;
// import org.apache.log4j.Logger;


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
    private static final Logger log = LoggerFactory.getLogger(MessageQueue.class);
    private Lock bigLock;
    private int benchFinished;
    

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
        log.info("针对冷热读写场景的RocketMQ存储系统设计");
        benchFinished = 0;
        // for SSD Benchmark
		try {
            // ssdBenchPath = "./ssdBench";
            ssdBenchPath = "/mnt/pmem/ssdBench";
            // ssdBenchPath = "/essd/bench";
            ssdBenchFileChannel = new RandomAccessFile(new File(ssdBenchPath), "rw").getChannel();
            ssdBenchTotalSize = 256L*1024L*1024L; //256MiB 
		} catch(IOException ie) {
			ie.printStackTrace();
		}  
        bigLock = new ReentrantLock();
    }

    @Override
    public long append(String topic, int queueId, ByteBuffer data){
        if (benchFinished == 1){
            return 0;
        }
        bigLock.lock();
        if (benchFinished == 1){
            bigLock.unlock();
            return 0;
        }

        // return queueMessage.setTopic(topic, queueId, data);

        // for SSD Benchmark
		try {
            SSDBench.benchFileChannelWriteThreadPoolRange(ssdBenchFileChannel, ssdBenchTotalSize, 16, 1024*1024);
            // int[] ioSizes = {4*1024, 8*1024, 16*1024, 32*1024, 64*1024, 128*1024, 256*1024, 512*1024,1024*1024};
            // for (int t = 1; t <= 50; t+=1){
                // for (int i = 0; i < ioSizes.length; i++){
                    // SSDBench.benchFileChannelWriteThreadPoolRange(ssdBenchFileChannel, ssdBenchTotalSize, t, ioSizes[i]);
                // }
            // }
		} catch(IOException ie) {
			ie.printStackTrace();
		}
        benchFinished = 1;
        bigLock.unlock();
        return 0;

    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        return null;
    }
}
