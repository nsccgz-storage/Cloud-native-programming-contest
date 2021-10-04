package io.openmessaging;

import org.apache.log4j.Logger;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CorrectTest {
    static String messagePath = "./message.txt";
    static final int MAX_DATA_SIZE = 17*1024;
    static final int MAX_QUEUE_NUM = 1000; // just for test 缩小QueueId的范围
    static ArrayList<String> topicList;
//    static ConcurrentHashMap<String, long[]> queueOffsetOfTopic = new ConcurrentHashMap<>();
    static BufferedWriter writer;
    final static String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    private static final Logger logger = Logger.getLogger(CorrectTest.class);
    static Lock lock = new ReentrantLock(true);

    public static void main(String[] args) {
        // just for test
//        try {
//            Files.delete(Paths.get("/home/wangxr/桌面/pmem_test/MetaData"));
//            Files.delete(Paths.get("/home/wangxr/桌面/pmem_test/data"));
////            Files.delete(Paths.get("/home/wangxr/桌面/pmem_test/space"));
//            Files.delete(Paths.get(messagePath));
//        }catch (IOException e){e.printStackTrace();}

        DefaultMessageQueueImpl messageQueue = new DefaultMessageQueueImpl();

        try {
            if(!Files.exists(Paths.get(messagePath))){
                Files.createFile(Paths.get(messagePath));
                System.out.println("Stage 1: write data");
                writer = new BufferedWriter(new FileWriter(messagePath));
                writeTest(10, 1L*1024L*1024L, messageQueue); // 40 thread, 1 MiB test data
                writer.close();
            }
            else{
                System.out.println("Stage 2: check recovery data");
                BufferedReader reader= new BufferedReader(new FileReader(messagePath));
                ArrayList<Message> msgs = loadMessage(reader);

                int count = 0, errorCount = 0;
                for(Message msg:msgs){
                    Map<Integer, ByteBuffer> mp = messageQueue.getRange(msg.topic, msg.queueId, msg.offset, 1);
                    ByteBuffer buffer = mp.get(0);
                    if(buffer != null && !msg.compare(buffer)){
                        System.out.println("error");
//                        System.out.println(new String(buffer.array()));
                        errorCount++;
                    }
//                    else{
//                        System.out.println("correct");
//                    }
                    count++;
                }
                System.out.println("count = "+count+" errorCount = "+errorCount);

                reader.close();
            }
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    private static class Message{
        String topic;
        int queueId;
        long offset;
        byte[] data;
        Message(String t, int q, long o,byte[] b){
            topic = t;
            queueId = q;
            offset = o;
            data = b;
        }

        // TODO: check
        Message(String message){
            String[] split = message.split(",");
            assert split.length == 4;
            topic = split[0];
            queueId = Integer.parseInt(split[1]);
            offset = Long.parseLong(split[2]);
            data = split[3].getBytes();
//            System.out.println(new String(data));
        }

        public String toString(){
            StringBuilder sb = new StringBuilder(topic);
            sb.append(",");
            sb.append(queueId);
            sb.append(",");
            sb.append(offset);
            sb.append(",");
//            System.out.println(new String(data));
            sb.append(new String(data));
            return sb.toString();
        }

        public boolean compare(ByteBuffer buffer){
            byte[] ans = new byte[buffer.remaining()];
            buffer.get(ans);
            if(ans.length != data.length){
                System.out.println("ans =  "+ans.length+" "+ Arrays.toString(ans));
                System.out.println("data = "+data.length+" "+  Arrays.toString(data));
                System.out.println("msg = "+this);
                return false;
            }
            boolean flag = true;
            for(int i = 0;i < ans.length;i++){
                if(ans[i] != data[i]) flag = false;
            }
            if(!flag){
                for(int i = 0;i < ans.length;i++){
                    System.out.println(ans[i] +" vs. " +data[i]);
                }
            }
            return flag;
        }
    }

    private static ArrayList<String> getRandomTopicList(int threadNum){
//        String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
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
                queueOffset[j] = -1L;
            }
//            queueOffsetOfTopic.put(sb.toString(), queueOffset);
        }
        return topicList_;
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
                int[] queueIdArray = getQueueId(random); // queueId 可能重复
                long[] queueSizeList = randomAllocate(topicSizeList[i]*100, queueIdArray.length);
                for(int j = 0;j < queueIdArray.length;j++){
                    long dataSize = queueSizeList[j];

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

                        byte[] bytes = getRandomBytes(size,random);

                        ByteBuffer buffer = ByteBuffer.allocate(size);
                        buffer.put(bytes);
                        buffer.flip();
                        long offset = messageQueue.append(topics.get(i), queueIdArray[j], buffer);
//
                        try {
                            saveMessage(topics.get(i), queueIdArray[j], offset, bytes);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
//                        queueOffsetOfTopic.get(topics.get(i))[queueIdArray[j]] = offset;
                    }
                }
            }
        }
    }

    private static void saveMessage(String t, int q, long o, byte[] b) throws IOException {
        lock.lock();
        Message msg = new Message(t, q, o, b);
//        System.out.println(msg);

        writer.write(msg.toString()+"\n");
        lock.unlock();
    }
    static ArrayList<Message> loadMessage(BufferedReader reader) throws IOException {
        ArrayList<Message> msgs = new ArrayList<>();
        String msgStr = null;
        while((msgStr = reader.readLine()) != null){
            msgs.add(new Message(msgStr));
        }
        return msgs;
    }

    // 随机生成queueId数组
    // 为了便于存储queue offset，这里生成queueId范围是[0, MAX_QUEUE_NUM)
    private static int [] getQueueId(Random random){
        int queueNum = random.nextInt(MAX_QUEUE_NUM)+1;
        int [] queueIdArray = new int[queueNum];
        for(int i = 0;i < queueNum;i++){
            queueIdArray[i] = random.nextInt(MAX_QUEUE_NUM);
        }
        return queueIdArray;
    }

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
