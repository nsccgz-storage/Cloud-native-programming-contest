package io.openmessaging;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import com.intel.pmem.llpl.TransactionalHeap;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.File;

import io.openmessaging.SSDBench;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.net.ssl.SSLHandshakeException;

import io.openmessaging.Test1MessageQueue;
import io.openmessaging.SSDqueue;


import org.apache.log4j.Logger;


/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultTestMessageQueueImpl extends MessageQueue {
    // Initialization
    
    public MessageQueue mq;
    public SSDqueue ssdQueue;

    public DefaultTestMessageQueueImpl(){

//        String dirPath = "/essd";
        //String dirPath = "/mnt/nvme/mq";
        String dirPath = "/mnt/ssd/wyk";
        //String dirPath = "/home/ubuntu/test";
        init(dirPath);
    }

    public DefaultTestMessageQueueImpl(String dirPath){
        init(dirPath);
    }

    public void init(String dirPath){
        ssdQueue = new SSDqueue(dirPath);
        // mq = new Test1MessageQueue(dirPath);
    }




    @Override
    public long append(String topic, int queueId, ByteBuffer data){
        return ssdQueue.append(topic, queueId, data);
        // return mq.append(topic, queueId, data);
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum){
        return ssdQueue.getRange(topic, queueId, offset, fetchNum);
        // return mq.getRange(topic, queueId, offset, fetchNum);
    }
}
