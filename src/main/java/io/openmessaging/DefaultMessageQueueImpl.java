package io.openmessaging;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import com.intel.pmem.llpl.TransactionalHeap;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.File;

import io.openmessaging.SSDBench;

//import org.slf4j.LoggerFactory;
//import org.slf4j.Logger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.net.ssl.SSLHandshakeException;

import io.openmessaging.Test1MessageQueue;
// import java.util.concurrent.locks.ReentrantReadWriteLock;


import org.apache.log4j.spi.LoggerFactory;
import org.apache.log4j.Logger;


/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageQueueImpl extends MessageQueue {
    // Initialization
    
    private SSDqueue ssdQueue;
    public DefaultMessageQueueImpl(){
        
        String dirPath = "/home/ubuntu/test";
        ssdQueue = new SSDqueue(dirPath);
    }
    @Override
    public long append(String topic, int queueId, ByteBuffer data){
        return ssdQueue.setTopic(topic, queueId, data);
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum){
        return ssdQueue.getRange(topic, queueId, offset, fetchNum);
    }
}
