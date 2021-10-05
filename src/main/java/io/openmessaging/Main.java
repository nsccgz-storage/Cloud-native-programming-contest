package io.openmessaging;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;

public class Main {
    public static void main(String args[]) throws IOException{
        // System.out.println("Hello World!");
        // String dirPath = "/home/ubuntu/test";


        // DefaultMessageQueueImpl mq = new DefaultMessageQueueImpl();
        // String t = "1234567890123456789012345678901234567890";
        // System.out.println(mq.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        // System.out.println(mq.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        // System.out.println(mq.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        // System.out.println(mq.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        // System.out.println(mq.append("12345", 123, ByteBuffer.wrap(t.getBytes())));

        // Map<Integer, ByteBuffer> res = mq.getRange("12345", 123, 0L, 100);
        // for(Map.Entry<Integer, ByteBuffer> entry: res.entrySet()){
        //     System.out.println("" + entry.getKey() + " : " + new String(entry.getValue().array()));
        // }

        
        String dirPath = "/home/ubuntu/test";
        SSDqueue ssdQueue = new SSDqueue(dirPath);
        String t = "1234567890123456789012345678901234567890";
        System.out.println( t.hashCode());
        System.out.println(ssdQueue.setTopic("12345", 123, ByteBuffer.wrap(t.getBytes())));
        

        Map<Integer, ByteBuffer> res = ssdQueue.getRange("12345", 123, 0L, 100);


        System.out.println("---------****-------------------------");
        for(Map.Entry<String, Long> entry: ssdQueue.topicNameQueueMetaMap.entrySet()){
            System.out.println("" + entry.getKey() + " : " + entry.getValue());
        }
        for(Map.Entry<Integer, ByteBuffer> entry: res.entrySet()){
            System.out.println("" + entry.getKey() + " : " + new String(entry.getValue().array()));
        }
        
    }

}

