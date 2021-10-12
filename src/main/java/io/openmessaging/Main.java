package io.openmessaging;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;

public class Main {
    public static void main(String args[]) throws IOException{
        // DefaultMessageQueueImpl mq = new DefaultMessageQueueImpl();
        // String t = "1234567890123456789012345678901234567890";
        // System.out.println( t.hashCode());
        // System.out.println(mq.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        // System.out.println(mq.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        // System.out.println(mq.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        // System.out.println(mq.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        // System.out.println(mq.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        // System.out.println(mq.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        // System.out.println(mq.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        // System.out.println(mq.append("dsfsf", 123, ByteBuffer.wrap(t.getBytes())));
        // System.out.println(mq.append("32424", 256, ByteBuffer.wrap(t.getBytes())));

        // Map<Integer, ByteBuffer> res = mq.getRange("12345", 123, 0L, 100);

        DefaultTestMessageQueueImpl ssdQueue = new DefaultTestMessageQueueImpl();
//        String dirPath = "/home/ubuntu/test";
//        SSDqueue ssdQueue = new SSDqueue(dirPath);
        String t = "1234567890123456789012345678901234567890";
        System.out.println( t.hashCode());
        t = "a"+t;
        System.out.println(ssdQueue.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        t = "b"+t;
        System.out.println(ssdQueue.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        t = "c"+t;
        System.out.println(ssdQueue.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        t = "d"+t;
        System.out.println(ssdQueue.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        System.out.println(ssdQueue.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        System.out.println(ssdQueue.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        System.out.println(ssdQueue.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        System.out.println(ssdQueue.append("dsfsf", 123, ByteBuffer.wrap(t.getBytes())));
        System.out.println(ssdQueue.append("32424", 256, ByteBuffer.wrap(t.getBytes())));

        for(long idx=0; idx < 10; idx++){
            Map<Integer, ByteBuffer> res = ssdQueue.getRange("12345", 123, idx, 100);
            System.out.println("---------****-------------------------");
            for(Map.Entry<Integer, ByteBuffer> entry: res.entrySet()){
                System.out.println("" + entry.getKey() + " : " + new String(entry.getValue().array()));
            }
        }

    }

}
