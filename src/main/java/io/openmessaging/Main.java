package io.openmessaging;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;

import javax.swing.plaf.synth.SynthStyle;

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

        
        // String dirPath = "/home/ubuntu/test";
        // String pmemPath = "/pmem";
        // SSDqueue ssdQueue = new SSDqueue(dirPath, pmemPath);
        // String t = "1234567890123456789012345678901234567890";
        // System.out.println( t.hashCode());
        // // System.out.println(ssdQueue.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        // // System.out.println(ssdQueue.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        // // System.out.println(ssdQueue.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        // // System.out.println(ssdQueue.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        // // System.out.println(ssdQueue.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        // // System.out.println(ssdQueue.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        // // System.out.println(ssdQueue.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        // // System.out.println(ssdQueue.append("dsfsf", 123, ByteBuffer.wrap(t.getBytes())));
        // // System.out.println(ssdQueue.append("32424", 256, ByteBuffer.wrap(t.getBytes())));

        // for(long idx=0; idx < 10; idx++){
        //     Map<Integer, ByteBuffer> res = ssdQueue.getRange("12345", 123, idx, 100);
        //     System.out.println("---------****-------------------------");
        //     for(Map.Entry<Integer, ByteBuffer> entry: res.entrySet()){
        //         System.out.println("" + entry.getKey() + " : " + new String(entry.getValue().array()));
        //     }
        // }
        
    //     ExecutorService executorService = Executors.newCachedThreadPool();

    //     Task task = new Task();

    //     FutureTask<Integer> futureTask = new FutureTask<Integer>(task);

    //     executorService.submit(futureTask);

    //     executorService.shutdown();
    //     try {
    //         // while(!futureTask.isDone()){
    //             // System.out.println("false");
    //         // }
    //         System.out.println("wait...");
    //         System.out.printf("main thread get result: %d\n", futureTask.get() );
    //     } catch (Exception e) {
    //         //TODO: handle exception
    //         e.printStackTrace();
    //     }
        String dbDirPath = "/mnt/ssd/wyk";
        String pmDirPath = "/mnt/pmem/wyk";
        MyLSMessageQueue ssdQueue = new MyLSMessageQueue(dbDirPath, pmDirPath);
        
        String t = "1234567890123456789012345678901234567890";
        System.out.println( t.hashCode());
        System.out.println(ssdQueue.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        System.out.println(ssdQueue.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        System.out.println(ssdQueue.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
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

class Task implements Callable<Integer>{
    @Override
    public Integer call() throws Exception{
        System.out.println("子线程正在计算");
        Thread.sleep(3000);
        Thread.currentThread().interrupt();
 
        int sum = 0;
        for(int i=0;i<100;i++){
            sum += i;
        }
        return sum;
    }
}
