package io.openmessaging;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;

public class Main {
    public static void main(String args[]) throws IOException{
        System.out.println("Hello World!");
        String metaPath = "/home/wangxr/桌面/pmem_test/MetaData";
        String dataPath = "/home/wangxr/桌面/pmem_test/data";
        
        boolean flag = new File(metaPath).exists();
        SSDqueue ssdQueue;
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


        String t = "1234567890123456789012345678901234567890";
         ssdQueue.setTopic("12345", 123, ByteBuffer.wrap(t.getBytes()));
//         ssdQueue.setTopic("12345", 123, ByteBuffer.wrap(t.getBytes()));
//         ssdQueue.setTopic("12345", 123, ByteBuffer.wrap(t.getBytes()));
        ssdQueue.setTopic("dsfsf", 123, ByteBuffer.wrap(t.getBytes()));

         ssdQueue.setTopic("32424", 256, ByteBuffer.wrap(t.getBytes()));

        Map<Integer, ByteBuffer> res = ssdQueue.getRange("12345", 123, 0L, 100);

        for(Map.Entry<String, Long> entry: ssdQueue.topicNameQueueMetaMap.entrySet()){
            System.out.println("" + entry.getKey() + " : " + entry.getValue());
        }
        for(Map.Entry<Integer, ByteBuffer> entry: res.entrySet()){
            System.out.println("" + entry.getKey() + " : " + new String(entry.getValue().array()));
        }
        
    }

}

