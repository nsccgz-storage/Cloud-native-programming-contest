package io.openmessaging;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DataSpace {
    private static final Logger logger = Logger.getLogger(DataSpace.class);

    private AtomicLong FREE_OFFSET;
    FileChannel fc;

    public ThreadLocal<ByteBuffer> writerQueueLocalBuffer;
    public ThreadLocal<long[]> writerMetaDataList;
    final int writerQueueBufferCapacity = 70 * 1024;
    public Lock lock; // 写队列的锁
    public Condition queueCondition;
    public Deque<Writer> writerQueue;
    public WriteStat writeStat;

    final int MAX_BUF_NUM = 6;
    final int MAX_BUF_LENGTH = 48 * 1024; // 200KiB + 17KiB < writerQueueBufferCapacity = 256KiB

    private class Writer{
        ByteBuffer data;
        boolean done;
        long offset;
        Condition cv;
        DataMeta meta;
        Writer(ByteBuffer d, Condition c, DataMeta m){
            data = d;
            done = false;
            cv = c;
            offset = 0L;
            meta = m;
        }
    }

    // 记录写聚合时的状态，以便调参分析
    public class WriteStat{
        public int[] bucketBound;
        public int[] bucketCount;
        public int emptyQueueCount;
        public int exceedBufNumCount;
        public int exceedBufLengthCount;
        WriteStat(){
            bucketBound = new int[]{100, 512, 1024, 2*1024, 4*1024, 8*1024, 16*1024, 32*1024, 48*1024, 64*1024, 80*1024 , 96*1024, 112*1024, 128*1024};

            bucketCount = new int[bucketBound.length-1];
            for (int i = 0; i < bucketCount.length; i++){
                bucketCount[i] = 0;
            }
            emptyQueueCount = 0;
            exceedBufNumCount = 0;
            exceedBufLengthCount = 0;
        }
        public void addSample(int len){
            for (int i = 0; i < bucketCount.length; i++){
                if (len < bucketBound[i+1]){
                    bucketCount[i]++;
                    break;
                }
            }
        }
        public void incEmptyQueueCount(){
            emptyQueueCount++;
        }
        public void incExceedBufNumCount(){
            exceedBufNumCount++;
        }
        public void incExceedBufLengthCount(){
            exceedBufLengthCount++;
        }

        @Override
        public String toString() {
            String ret = "";
            ret += bucketBound[0] + " < ";
            for (int i = 0; i < bucketCount.length; i++){
                ret += "[" + bucketCount[i] + "]";
                ret += " < " + bucketBound[i+1] + " < ";
            }
            return ret;
        }
        public void report(){
            logger.info(this.toString());
        }
        public WriteStat clone(){
            WriteStat ret = new WriteStat();
            ret.emptyQueueCount = emptyQueueCount;
            ret.exceedBufLengthCount = exceedBufLengthCount;
            ret.exceedBufNumCount = exceedBufNumCount;
            ret.bucketBound = bucketBound.clone();
            ret.bucketCount = bucketCount.clone();
            return ret;
        }
    }

    SSDqueue.TestStat testStat;

    public DataSpace(FileChannel fc, long startSpace) {
        this.fc = fc;
        this.FREE_OFFSET = new AtomicLong(startSpace);
        //update();

        writerQueueLocalBuffer = new ThreadLocal<>();
        writerMetaDataList = new ThreadLocal<>();
        lock = new ReentrantLock(false); // false 非公平锁
        queueCondition = lock.newCondition();
        writerQueue = new ArrayDeque<>();

        writeStat = new WriteStat();
    }
    public DataSpace(FileChannel fc) throws IOException {
        this.fc = fc;
        // read metaData
//        ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES);
//        int size = fc.read(tmp, 0L);
//        //logger.info(size);
//        tmp.flip();
//        this.FREE_OFFSET = new AtomicLong(tmp.getLong());
        this.FREE_OFFSET = new AtomicLong(-1);// TODO: 如果恢复中包含写操作，请重新写此处代码

        writerQueueLocalBuffer = new ThreadLocal<>();
        writerMetaDataList = new ThreadLocal<>();
        lock = new ReentrantLock(false); // false 非公平锁
        queueCondition = lock.newCondition();
        writerQueue = new ArrayDeque<>();

        writeStat = new WriteStat();
    }

//    public long write(ByteBuffer data) throws IOException{
////        long size = data.remaining() + Long.BYTES * 2;
////
////        long offset = FREE_OFFSET.getAndAdd(size);
////
////        long nextOffset = -1L;
////        ByteBuffer byteData = ByteBuffer.allocate((int)size);
////        byteData.putLong(size - (Long.BYTES + Long.BYTES));
////        byteData.putLong(nextOffset);
////        byteData.put(data);
////        byteData.flip();
////        int len = fc.write(byteData,offset);
////        //update();
////        fc.force(true);
////        return offset;
//        return writeAgg(data);
//    }
    public int read(ByteBuffer res, long offset) throws IOException{
        return fc.read(res, offset);
    }

    public DataMeta writeAgg(ByteBuffer data, DataMeta srcMeta){
        if (writerQueueLocalBuffer.get() == null) {
            writerQueueLocalBuffer.set(ByteBuffer.allocateDirect(writerQueueBufferCapacity)); // 分配堆外内存
        }
        if(writerMetaDataList.get() == null){
            writerMetaDataList.set(new long[Long.BYTES*MAX_BUF_NUM*4]);
        }
        ByteBuffer writerBuffer = writerQueueLocalBuffer.get();
        long[] writerMetaList = writerMetaDataList.get();

        lock.lock();
        Writer w = new Writer(data, queueCondition, srcMeta);
        try {
            writerQueue.addLast(w);
            while (!w.done && !w.equals(writerQueue.getFirst())) {
                w.cv.await();
            }
            if (w.done) {
                return w.meta;
            }

            // 执行批量写操作
            int bufLength = 0;
            int bufNum = 0;
            boolean continueMerge = true;
            Iterator<Writer> iter = writerQueue.iterator();
            Writer lastWriter = w;
            writerBuffer.clear();
//            writerMetaBuffer.clear();

            long writeStartOffset = FREE_OFFSET.get();
            while (iter.hasNext() && continueMerge) {
                lastWriter = iter.next();

                int writeLength = lastWriter.data.remaining() + 2 * Long.BYTES;
                lastWriter.offset = FREE_OFFSET.getAndAdd(writeLength);

                writerBuffer.putLong(lastWriter.data.remaining());
                writerBuffer.putLong(-1L); // next block
                writerBuffer.put(lastWriter.data);

                bufLength += writeLength;
                bufNum += 1;
                if (bufNum >= MAX_BUF_NUM) {
                    continueMerge = false;
                    writeStat.incExceedBufNumCount();
                }
                if (bufLength >= MAX_BUF_LENGTH) {
                    continueMerge = false;
                    writeStat.incExceedBufLengthCount();
                }
                if(!iter.hasNext()){
                    writeStat.incEmptyQueueCount();
                }

                if (lastWriter.meta.tail == -1) { // 保存 head
                    lastWriter.meta.head = lastWriter.offset;
                    lastWriter.meta.tail = lastWriter.offset;
                    lastWriter.meta.metaOffset = lastWriter.offset;

                    writerMetaList[bufNum*2-2] = lastWriter.offset;
                    writerMetaList[bufNum*2-1] = lastWriter.meta.metaOffset;
//                    buffer.flip();
//                    int size = fc.write(buffer, lastWriter.block.metaOffset);
                } else { // 更新上一个block的next指针
                    writerMetaList[bufNum*2-2] = lastWriter.offset;
                    writerMetaList[bufNum*2-1] = lastWriter.meta.tail + Long.BYTES;
//                    buffer.putLong(lastWriter.offset);
//                    buffer.flip();
//                    int size = fc.write(buffer, lastWriter.block.tail + Long.BYTES);
                    lastWriter.meta.tail = lastWriter.offset;
                }
            }
            writerBuffer.flip();
//            writerMetaBuffer.flip();
//            if(testStat.threadId.get() == null)testStat.updateThreadId();
//            testStat.stats[testStat.threadId.get()].addSample(bufLength);

            writeStat.addSample(bufLength);
            // 写期间 unlock 使得其他 writer 可以被加入 writerQueue
            {
                lock.unlock();
                ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
                long writePosition;
                for(int i = 0;i < bufNum;i++){
                    buffer.clear();
                    buffer.putLong(writerMetaList[i*2]);
                    writePosition = writerMetaList[i*2+1];
                    buffer.flip();
                    fc.write(buffer, writePosition);
                }
                fc.write(writerBuffer, writeStartOffset);
                fc.force(true);
                lock.lock();
            }

            while (true) {
                Writer ready = writerQueue.removeFirst();
                if (!ready.equals(w)) {
                    ready.done = true;
                    ready.cv.signal();
                }
                if (ready.equals(lastWriter)){
                    break;
                }
            }

            // Notify new head of write queue
            if (!writerQueue.isEmpty()) {
                writerQueue.getFirst().cv.signal();
            }
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
        return w.meta;
    }

    public long readHandle(long offset) throws IOException{
        ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES);
        int len = fc.read(tmp, offset + Long.BYTES);
        tmp.flip();
        return tmp.getLong();
    }
    public ByteBuffer readHandleData(long offset)throws IOException{
        ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES + Long.BYTES);
        int len1 = fc.read(tmp, offset);
        //logger.info(this.toString());
        tmp.flip();
        Long dataSize = tmp.getLong();
        Long nextOffset = tmp.getLong();

        ByteBuffer tmp1 = ByteBuffer.allocate(dataSize.intValue() + Long.BYTES);
        tmp1.putLong(nextOffset);
        int len2 = fc.read(tmp1, offset + Long.BYTES + Long.BYTES);
        tmp1.flip();
        return tmp1;
    }
    public int updateLink(long tail, long newTail)throws IOException{
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(newTail);
        buffer.flip();
        int size = fc.write(buffer, tail + Long.BYTES);
        //fc.force(true);
        return size;
    }
    // 保存 head
    public int updateMeta(long offset, long totalNum, long head, long tail)throws IOException{
        // ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 3);
        // buffer.putLong(totalNum);
        // buffer.putLong(head);
        // buffer.putLong(tail);
        // buffer.flip();
        // int size = fc.write(buffer, offset);
        // fc.force(true);
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(head);
        buffer.flip();
        int size = fc.write(buffer, offset);
        fc.force(true);
        return size;
    }
    public long createLink(){
        long res = FREE_OFFSET.getAndAdd(Long.BYTES * 1);
        return res;
    }

    void update(){
        try {
            ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES * 1);
            tmp.putLong(FREE_OFFSET.get());
            tmp.flip();
            fc.write(tmp, 0L);

        } catch (Exception e) {
            //TODO: handle exception
            e.printStackTrace();
            logger.error("err!!!!!!!!!!!!!!!!!");
        }
    }
}
