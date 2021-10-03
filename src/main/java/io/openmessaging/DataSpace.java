package io.openmessaging;

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

import org.apache.log4j.Logger;

public class DataSpace {
    private static final Logger logger = Logger.getLogger(DataSpace.class);

    AtomicLong FREE_OFFSET;
    FileChannel fc;

    public ThreadLocal<ByteBuffer> writerQueueLocalBuffer;
    final int writerQueueBufferCapacity = 1024 * 128;
    public Lock lock; // 写队列的锁
    public Condition queueCondition;
    public Deque<Writer> writerQueue;

    private class Writer{
        ByteBuffer data;
        boolean done;
        long offset;
        Condition cv;
        Writer(ByteBuffer d, Condition c){
            data = d;
            done = false;
            cv = c;
            offset = 0L;
        }
    }

    public DataSpace(FileChannel fc, long startSpace) {
        this.fc = fc;
        this.FREE_OFFSET = new AtomicLong(startSpace);

        writerQueueLocalBuffer = new ThreadLocal<>();
        lock = new ReentrantLock(false); // false 非公平锁
        queueCondition = lock.newCondition();
        writerQueue = new ArrayDeque<>();
    }   
    public DataSpace(FileChannel fc) throws IOException{
        this.fc = fc;
        // read metaData
        ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES);
        fc.read(tmp, 0L);
        tmp.flip();
        this.FREE_OFFSET = new AtomicLong(tmp.getLong());

        writerQueueLocalBuffer = new ThreadLocal<>();
        lock = new ReentrantLock(false); // false 非公平锁
        queueCondition = lock.newCondition();
        writerQueue = new ArrayDeque<>();
    }

    public long write(ByteBuffer data) throws IOException{
        int size = data.remaining() + Long.BYTES * 2;

        long offset = FREE_OFFSET.getAndAdd(size);

        long nextOffset = -1L;
        ByteBuffer byteData = ByteBuffer.allocate(size);
        byteData.putLong(size - (Long.BYTES + Long.BYTES));
        byteData.putLong(nextOffset);
        byteData.put(data);
        byteData.flip();
        int len = fc.write(byteData,offset);
        updata();
        fc.force(true);
        return offset;
    }

    /*
    * 写聚合
    * @return: data 在 dataSpace 存储位置的 offset
    * */
    public long writeAgg(ByteBuffer data) throws IOException {
        if (writerQueueLocalBuffer.get() == null) {
            writerQueueLocalBuffer.set(ByteBuffer.allocateDirect(writerQueueBufferCapacity)); // 分配堆外内存
        }
        ByteBuffer writerBuffer = writerQueueLocalBuffer.get();

        long offset = -1;
        lock.lock();
        try {
            Writer w = new Writer(data, queueCondition);
            writerQueue.addLast(w);
            while (!w.done && !w.equals(writerQueue.getFirst())) {
                w.cv.await();
            }
            if (w.done) {
                return w.offset;
            }

            // 设置参数
            int maxBufNum = 6;
            int maxBufLength = 36 * 1024; // 36KiB

            // 执行批量写操作
            long writeStartOffset = FREE_OFFSET.get();
            int bufLength = 0;
            int bufNum = 0;
            boolean continueMerge = true;
            Iterator<Writer> iter = writerQueue.iterator();
            Writer lastWriter = w;
            writerBuffer.clear();

            while (iter.hasNext() && continueMerge) {
                lastWriter = iter.next();

                int writeLength = lastWriter.data.remaining() + 2 * Long.BYTES;
                lastWriter.offset = FREE_OFFSET.getAndAdd(writeLength);
                writerBuffer.putLong(lastWriter.data.remaining());
                writerBuffer.putLong(-1L); // next block
                writerBuffer.put(lastWriter.data);

                bufLength += writeLength;
                bufNum += 1;
                if (bufNum >= maxBufNum) {
                    continueMerge = false;
                }
                if (bufLength >= maxBufLength) {
                    continueMerge = false;
                }
            }

            // do write work
            // 写期间 unlock 使得其他 writer 可以被加入 writerQueue
            {
                lock.unlock();
                writerBuffer.flip();
                fc.write(writerBuffer, writeStartOffset);
                fc.force(true);
                lock.lock();
            }

            while (true) {
                Writer ready = writerQueue.pop();
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
            offset = w.offset;
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
        return offset;
    }
    /*
    public ByteBuffer read(Long offset) throws IOException{
        
    }
    */
    public long readHandle(long offset) throws IOException{
        ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES);
        int len = fc.read(tmp, offset + Long.BYTES);
        tmp.flip();    
        return tmp.getLong();
    }
    public ByteBuffer readHandleData(long offset)throws IOException{
        ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES + Long.BYTES);
        int len1 = fc.read(tmp, offset);
        tmp.flip();
        Long dataSize = tmp.getLong();
        Long nextOffset = tmp.getLong();

        ByteBuffer tmp1 = ByteBuffer.allocate(dataSize.intValue() + Long.BYTES);
        tmp1.putLong(nextOffset);
        int len2 = fc.read(tmp1, offset + Long.BYTES + Long.BYTES);
        tmp1.flip();
        return tmp1;
    }

    void updata(){
        try {
            ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES * 1);
            tmp.putLong(FREE_OFFSET.get());
            tmp.flip();
            fc.write(tmp, 0L);

        } catch (Exception e) {
            //TODO: handle exception
            e.printStackTrace();
        }
    }
}
