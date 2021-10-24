package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.Map;

// import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageQueueImpl extends MessageQueue {
    // Initialization

    public MessageQueue mq;
    public DefaultMessageQueueImpl(){

       String dirPath = "/essd";
       String pmDirPath = "/pmem";
        mq = new LSMessageQueue(dirPath, pmDirPath);
    }
    @Override
    public long append(String topic, int queueId, ByteBuffer data){
        return mq.append(topic, queueId, data);
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum){
        return mq.getRange(topic, queueId, offset, fetchNum);
    }
}
