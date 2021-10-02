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

    private TopicId queueMessage;
    private String ssdBenchPath;
    private FileChannel ssdBenchFileChannel;
    private long ssdBenchTotalSize;
    private Lock bigLock;
    private int benchFinished;
    private Test1MessageQueue mq;
    
    private SSDqueue ssdQueue;
    public DefaultMessageQueueImpl(){
        try{
            String metaPath = "/home/wangxr/桌面/pmem_test/MetaData";
            String dataPath = "/home/wangxr/桌面/pmem_test/data";
            boolean flag = new File(metaPath).exists();
            if(flag){
                //System.out.println(" 28 ");
                
                FileChannel fileChannel = new RandomAccessFile(new File(dataPath), "rw").getChannel();
                FileChannel metaFileChannel = new RandomAccessFile(new File(metaPath), "rw").getChannel();
                ssdQueue = new SSDqueue(fileChannel, metaFileChannel, false);
                
            }else{
                FileChannel fileChannel = new RandomAccessFile(new File(dataPath), "rw").getChannel();
                FileChannel metaFileChannel = new RandomAccessFile(new File(metaPath), "rw").getChannel();
                ssdQueue = new SSDqueue(fileChannel, metaFileChannel);
            }

        }catch(IOException e){
            e.printStackTrace();
        }
   
        
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
