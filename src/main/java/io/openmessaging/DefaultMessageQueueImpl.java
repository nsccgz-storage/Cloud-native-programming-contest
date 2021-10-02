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

import io.openmessaging.Test1MessageQueue;
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
    private Test1MessageQueue mq;
    

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
        // 可以用来藏分？？
        SSDBench.runBench("/essd");
        mq = new Test1MessageQueue("/essd");
    }

    @Override
    public long append(String topic, int queueId, ByteBuffer data){
        return mq.append(topic, queueId, data);
        // return 0;

    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        return mq.getRange(topic, queueId, offset, fetchNum);
        // return null;
    }
}
