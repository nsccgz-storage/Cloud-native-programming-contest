package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/*
* 简单实现性能测试部分，能保证不同线程的写入量大致相等
* */
public class PerformanceTest {
    static final int MAX_DATA_SIZE = 17*1024;
    static final int MAX_QUEUE_NUM = 5000;
    static ArrayList<String> topicList;
    static ConcurrentHashMap<String, long[]>queueOffsetOfTopic = new ConcurrentHashMap<>();

    // 性能测试的样例程序
    public static void main(String[] args){
        DefaultMessageQueueImpl messageQueue = new DefaultMessageQueueImpl();

        int threadNum = (int)(Math.random() * (50 - 10 + 1)) + 10;
        threadNum = 40; // just for test
        System.out.println("Write thread number is "+ threadNum);

        long startTime = System.nanoTime();
        PerformanceTest.writeTest(threadNum, 75 * 1024 * 1024 * 1024L, messageQueue); // 写入75GB数据
        System.out.println("Elapsed Time: Write test "+ (System.nanoTime()-startTime)/1_000_000.0 + "ms");

        startTime = System.nanoTime();
        PerformanceTest.ReadAndWriteTest(threadNum, 50 * 1024 * 1024 * 1024L, messageQueue); // 写入50GB数据并读取
        System.out.println("Elapsed Time: Write and read test "+ (System.nanoTime()-startTime)/1_000_000.0 + "ms");
    }

    public static void writeTest(int threadNum, long totalSize, DefaultMessageQueueImpl messageQueue){
        topicList = getRandomTopicList(threadNum);

        ExecutorService executorService = Executors.newFixedThreadPool(threadNum);
        List<Callable<Object>> tasks = new ArrayList<Callable<Object>>(threadNum);
        for(int i = 0;i < threadNum;i++){
            tasks.add(Executors.callable(new WriteThread(totalSize / threadNum, messageQueue)));
        }
        try {
            executorService.invokeAll(tasks);
        }catch(InterruptedException e){
            e.printStackTrace();
        }
        executorService.shutdown();
    }

    public static void ReadAndWriteTest(int writeThreadNum, long writeTotalSize, DefaultMessageQueueImpl messageQueue){
        int readThreadNum = 10; // 先设定额外增加线程数固定；

        ExecutorService executorService = Executors.newFixedThreadPool(writeThreadNum + readThreadNum); // 额外增加一些线程
        List<Callable<Object>> tasks = new ArrayList<Callable<Object>>(writeThreadNum + readThreadNum);
        for(int i = 0;i < writeThreadNum;i++){
            tasks.add(Executors.callable(new WriteThread(writeTotalSize / writeThreadNum, messageQueue)));
        }
        for(int i = 0;i < readThreadNum;i++){
            tasks.add(Executors.callable(new ReadThread(messageQueue)));
        }
        try {
            executorService.invokeAll(tasks);
        }catch(InterruptedException e){
            e.printStackTrace();
        }
        executorService.shutdown();
    }

    private static class WriteThread extends Thread{

        private Random random;
        private  ArrayList<String> topics; // 线程需要写入的topic列表
        private long writeSize;
        private DefaultMessageQueueImpl messageQueue;

        public WriteThread(long writeSize, DefaultMessageQueueImpl messageQueue){
            this.writeSize = writeSize; // 需写入数据的大小
            random = new Random();
            this.messageQueue = messageQueue;
        }

        @Override
        public void run(){
            int topicNum = random.nextInt(100) + 1;
            topics = new ArrayList<String>(topicNum);
            for(int i = 0;i < topicNum;i++){
                topics.add(topicList.get(random.nextInt(topicList.size())));
            }


            long[] topicSizeList = randomAllocate(writeSize/100, topicNum);
            for(int i = 0;i < topicNum;i++){
                int[] queueIdArray = getQueueId(random);
                long[] queueSizeList = randomAllocate(topicSizeList[i]*100, queueIdArray.length);
                for(int j = 0;j < queueIdArray.length;j++){
                    long dataSize = queueSizeList[j];
                    while(dataSize > 0){
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
                        ByteBuffer buffer = ByteBuffer.allocate(size);
                        byte[] bytes = new byte[size];

                        random.nextBytes(bytes);
                        buffer.put(bytes);
                        buffer.flip();
                        long offset = messageQueue.append(topics.get(i), queueIdArray[j], buffer);
                        queueOffsetOfTopic.get(topics.get(i))[queueIdArray[j]] = offset;
                    }
                }
            }

        }

    }

    private static class ReadThread extends Thread{
        private Random random;
        private static AtomicInteger topicIndex = new AtomicInteger(0);
        private DefaultMessageQueueImpl messageQueue;

        public ReadThread(DefaultMessageQueueImpl messageQueue){
            this.messageQueue = messageQueue;
            random = new Random();
        }

        @Override
        public void run() {
            int index;
            while((index = topicIndex.getAndIncrement()) < topicList.size()){
                String topic = topicList.get(index);

                long[] queueId = queueOffsetOfTopic.get(topic);
                for(int i = 0;i < queueId.length;i++){
                    if(queueId[i] == -1)continue;
                    int fetchNum;
                    if(random.nextBoolean()){ // read from head
                        for(long offset = 0L;offset <= queueId[i];offset += fetchNum){
                            fetchNum = random.nextInt(100) + 1;
                            messageQueue.getRange(topic,i, offset, fetchNum);
                        }
                    }
                    else{ // read from tail
                        // TODO: 如何理解从末尾开始读取，先按照从末尾读取，每次只读取1个的思路实现
                        for(long offset = queueId[i];offset >= 0;offset--){
                            // fetchNum = random.nextInt(100) + 1;
                            messageQueue.getRange(topic,i, offset, 1);
                        }
                    }
                }
            }
            // System.out.println("Read thread["+getName()+"] finish");
        }
    }

    private static ArrayList<String> getRandomTopicList(int threadNum){
        String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        final int MAX_TOPIC_SIZE = 64;
        Random random = new Random();
        int topicNum = threadNum * (random.nextInt(100) + 1); // 随机生成topic总数
        ArrayList<String> topicList_ = new ArrayList<>(topicNum);

        for (int i = 0; i < topicNum; i++) {
            StringBuilder sb = new StringBuilder();
            int len = random.nextInt(MAX_TOPIC_SIZE) + 1;
            for (int j = 0; j < len; j++) {
                sb.append(str.charAt(random.nextInt(str.length())));
            }
            topicList_.add(sb.toString());
            long [] queueOffset = new long[MAX_QUEUE_NUM];
            for(int j = 0;j <  MAX_QUEUE_NUM;j++){
                queueOffset[i] = -1L;
            }
            queueOffsetOfTopic.put(sb.toString(), queueOffset);
        }
        return topicList_;
    }

    // 随机生成queueId数组
    // 为了便于存储queue offset，这里生成queueId范围是[0, 5000)
    private static int [] getQueueId(Random random){
        int queueNum = random.nextInt(5000)+1;
        int [] queueIdArray = new int[queueNum];
        for(int i = 0;i < queueNum;i++){
            queueIdArray[i] = random.nextInt(5000)+1;
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
}
