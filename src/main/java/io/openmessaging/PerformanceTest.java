package io.openmessaging;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

/*
* 简单实现性能测试部分，能保证不同线程的写入量大致相等
* */
public class PerformanceTest {
    static final int MAX_DATA_SIZE = 17*1024;
    static final int MAX_QUEUE_NUM = 5000;
    final static String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    static ArrayList<ArrayList<String>> topicList;
    static int maxTopicNum = 0;
    static ConcurrentHashMap<String, long[]> queueDataNumOfTopic = new ConcurrentHashMap<>();
    private static final Logger logger = Logger.getLogger(CorrectTest.class);

    // 性能测试的样例程序
    public static void main(String[] args){
        String localPath = "/home/wangxr/桌面/pmem_test/";
        try {
            if(Files.exists(Paths.get(localPath+"MetaData")))
                Files.delete(Paths.get(localPath+"MetaData"));
            if(Files.exists(Paths.get(localPath+"data")))
                Files.delete(Paths.get(localPath+"data"));
        }catch (IOException e){e.printStackTrace();}

        DefaultMessageQueueImpl messageQueue = new DefaultMessageQueueImpl();

        int threadNum = (int)(Math.random() * (50 - 10 + 1)) + 10;
        threadNum = 40; // 仿照评测程序，固定线程数=40
        logger.info("Write thread number is "+ threadNum);

        long startTime = System.nanoTime();
        PerformanceTest.writeTest(threadNum, 7 * 1024 * 1024 * 1024L, messageQueue); // 写入75GB数据
        logger.info("Elapsed Time: Write test "+ (System.nanoTime()-startTime)/1_000_000.0 + "ms");

        startTime = System.nanoTime();
        PerformanceTest.ReadAndWriteTest(threadNum, 5 * 1024 * 1024 * 1024L, messageQueue); // 写入50GB数据并读取
        logger.info("Elapsed Time: Write and read test "+ (System.nanoTime()-startTime)/1_000_000.0 + "ms");
    }

    public static void writeTest(int threadNum, long totalSize, DefaultMessageQueueImpl messageQueue){
        topicList = getRandomTopicList(threadNum);

        ExecutorService executorService = Executors.newFixedThreadPool(threadNum);
        List<Callable<Object>> tasks = new ArrayList<Callable<Object>>(threadNum);
        for(int i = 0;i < threadNum;i++){
            tasks.add(Executors.callable(new WriteThread(totalSize / threadNum, messageQueue, topicList.get(i))));
        }
        try {
            executorService.invokeAll(tasks);
        }catch(InterruptedException e){
            e.printStackTrace();
        }
        executorService.shutdown();
    }

    public static void ReadAndWriteTest(int threadNum, long writeTotalSize, DefaultMessageQueueImpl messageQueue){
        ExecutorService executorService = Executors.newFixedThreadPool(threadNum); // 额外增加一些线程
        List<Callable<Object>> tasks = new ArrayList<Callable<Object>>(threadNum);
        for(int i = 0;i < threadNum;i++){
            tasks.add(Executors.callable(new ReadWriteThread(writeTotalSize / threadNum, messageQueue, topicList.get(i))));
        }
        try {
            executorService.invokeAll(tasks);
        }catch(InterruptedException e){
            e.printStackTrace();
        }
        executorService.shutdown();
    }

    private static class WriteThread extends Thread{

        public Random random;
        public  ArrayList<String> topics; // 线程需要写入的topic列表
        public long writeSize;
        public DefaultMessageQueueImpl messageQueue;

        public WriteThread(long writeSize, DefaultMessageQueueImpl mq, ArrayList<String>t){
            this.writeSize = writeSize; // 需写入数据的大小
            random = new Random();
            this.messageQueue = mq;
            topics = t;
        }

        @Override
        public void run() {
            long[] topicSizeList = randomAllocate(writeSize / 100, topics.size());
            for (int i = 0; i < topics.size(); i++) {
                int[] queueIdArray = getQueueId(random);
                long[] queueSizeList = randomAllocate(topicSizeList[i]*100, queueIdArray.length);
                for (int j = 0; j < queueIdArray.length; j++) {
                    doOneAppend(topics.get(i), queueIdArray[j], queueSizeList[j]);
//                int[] queueIdArray = getQueueId(random);
//                long[] queueSizeList = randomAllocate(topicSizeList[i]*100, queueIdArray.length);
//                for(int j = 0;j < queueIdArray.length;j++){
//                    long dataSize = queueSizeList[j];
//                    if(dataSize < 100) System.out.println("Ouch"); // TODO: to delete
//
//                    while(dataSize >= 100){
//                        int size; // 写入数据大小
//                        if(dataSize <= MAX_DATA_SIZE){
//                            size = (int) dataSize;
//                            dataSize = 0;
//                        }
//                        else if(dataSize < MAX_DATA_SIZE + 100){
//                            size = (int) (dataSize / 2);
//                            dataSize -= size;
//                        }
//                        else{
//                            size = MAX_DATA_SIZE;
//                            dataSize -= MAX_DATA_SIZE;
//                        }
//                        ByteBuffer buffer = ByteBuffer.wrap(getRandomBytes(size,random));
//
//                        long offset = messageQueue.append(topics.get(i), queueIdArray[j], buffer);
//                        long[] queueDataNum = queueDataNumOfTopic.computeIfAbsent(topics.get(i), k -> new long[MAX_QUEUE_NUM]);
//                        queueDataNum[queueIdArray[j]] = offset+1;
//                    }
                }
            }
        }
        public void doOneAppend(String topic, int queueId, long dataSize){
//            if(dataSize < 100) System.out.println("Ouch"); // TODO: to delete
            while(dataSize >= 100){
                int size; // 写入数据大小
                if(dataSize <= MAX_DATA_SIZE){
                    size = (int) dataSize;
                    dataSize = 0;
                }
                else if(dataSize < MAX_DATA_SIZE + 100){
                    size = (int) (dataSize / 2);
                    dataSize -= size;
                }
                else{
                    size = MAX_DATA_SIZE;
                    dataSize -= MAX_DATA_SIZE;
                }
                ByteBuffer buffer = ByteBuffer.wrap(getRandomBytes(size,random));

                long offset = messageQueue.append(topic, queueId, buffer);
                long[] queueDataNum = queueDataNumOfTopic.computeIfAbsent(topic, k -> new long[MAX_QUEUE_NUM]);
                queueDataNum[queueId] = offset+1;
            }
        }
    }

    private static class ReadWriteThread extends WriteThread{
        public ReadWriteThread(long size, DefaultMessageQueueImpl mq, ArrayList<String> t){
            super(size, mq, t);
        }

        @Override
        public void run() {
            long[] topicSizeList = randomAllocate(writeSize/100, topics.size());
            for(int i = 0;i < topics.size();i++){
                int[] queueIdArray = getQueueId(random);
                long[] queueSizeList = randomAllocate(topicSizeList[i]*100, queueIdArray.length);

                long[] queueDataNum = queueDataNumOfTopic.get(topics.get(i));

                for(int j = 0;j < queueIdArray.length;j++){
                    super.doOneAppend(topics.get(i), queueIdArray[j], queueSizeList[j]);


                    int queueId = random.nextInt(MAX_QUEUE_NUM);
                    while(queueDataNum[queueId] == 0){
                        queueId = random.nextInt(MAX_QUEUE_NUM);
                        // tip: 如果topic的所有队列都没有数据，就会死循环
                    }
                    doOneGetRange(topics.get(i), queueId, queueDataNum[queueId]);
                }
            }
        }

        public void doOneGetRange(String topic, int queueId, long dataNum){
            int fetchNum = random.nextInt(100) + 1;
            if(random.nextBoolean()){ // read from head
                for(long offset = 0L;offset < dataNum;offset += fetchNum){
                    messageQueue.getRange(topic, queueId, offset, fetchNum);
                }
            }
            else{ // read from tail
                long startOffset;
                for(long offset = dataNum - 1;offset >= 0;offset -= fetchNum){
                    startOffset = offset-fetchNum+1 > 0 ? offset-fetchNum+1 : 0;
                    messageQueue.getRange(topic, queueId, startOffset, fetchNum);
                }
            }
        }
    }

    private static ArrayList<ArrayList<String>> getRandomTopicList(int threadNum){
        Random random = new Random();
        ArrayList<ArrayList<String>> res = new ArrayList<>(threadNum);
        for(int i = 0;i < threadNum;i++){
            int topicNum = random.nextInt(100) + 1;
            ArrayList<String> topics = new ArrayList<>(topicNum);
            for(int j = 0;j < topicNum;j++){
                topics.add("topic"+maxTopicNum);
                maxTopicNum++;
            }
            res.add(topics);
        }
        return res;
    }

    // 随机生成queueId数组
    // 为了便于存储queue offset，这里生成queueId范围是[0, 5000)
    private static int [] getQueueId(Random random){
        int queueNum = random.nextInt(MAX_QUEUE_NUM)+1;
        int [] queueIdArray = new int[queueNum];
        for(int i = 0;i < queueNum;i++){
            queueIdArray[i] = random.nextInt(MAX_QUEUE_NUM);
        }
        return queueIdArray;
    }

    // 把 totalNum 个元素分给 count 个对象的随机分配方法
    private static long[] randomAllocate(long totalNum, int count){
        long[] results = new long[count];
        long remain = totalNum;
        Random random = new Random();
        for(int i = 0;i < count-1;i++){
            long allocate = 0;
            if(remain > 0){
                if(remain / (count - i) * 2 < 1){
                    allocate = remain;
                }else{
                    allocate = 1 + (long)(random.nextDouble() * (remain / (count - i) * 2));
                }
            }
            remain -= allocate;
            results[i] = allocate;
        }
        results[count-1] = remain;
        return results;
    }

    private static byte[] getRandomBytes(int size, Random random){
        StringBuilder sb = new StringBuilder(size);
        for (int j = 0; j < size; j++) {
            sb.append(str.charAt(random.nextInt(str.length())));
        }
        return sb.toString().getBytes();
    }

}
