package io.openmessaging;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.spi.LoggerFactory;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class DataSpace {
    private static final Logger logger = Logger.getLogger(DataSpace.class);

    AtomicLong FREE_OFFSET;
    FileChannel fc;

    public DataSpace(FileChannel fc, long startSpace) {
        this.fc = fc;
        this.FREE_OFFSET = new AtomicLong(startSpace);
        //update();
    }   
    public DataSpace(FileChannel fc) throws IOException{
        this.fc = fc;
        // read metaData
        // ByteBuffer tmp = ByteBuffer.allocate(Long.BYTES);
        // int size = fc.read(tmp, 0L);
        // //logger.info(size);
        // tmp.flip();
        this.FREE_OFFSET = new AtomicLong(0L);
        
    }
    public void force(){
        try {
            fc.force(true);
        } catch (Exception e) {
            //TODO: handle exception
            e.printStackTrace();
        }
        
    }
    synchronized public long write(ByteBuffer data) throws IOException{
        long size = data.remaining() + Long.BYTES * 2;
        long offset = FREE_OFFSET.getAndAdd(size);

        long nextOffset = -1L;
        ByteBuffer byteData = ByteBuffer.allocate((int)size);
        byteData.putLong(size - (Long.BYTES + Long.BYTES));
        byteData.putLong(nextOffset);
        byteData.put(data);
        byteData.flip();
        int len = fc.write(byteData,offset);
        //update();
        //fc.force(true);
        return offset;
    }
    public int read(ByteBuffer res, long offset) throws IOException{
        return fc.read(res, offset);
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
        fc.force(true);
        return size;
    }
    // public int updateMeta(long offset, long totalNum, long head, long tail)throws IOException{
    //     // ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 3);
    //     // buffer.putLong(totalNum);
    //     // buffer.putLong(head);
    //     // buffer.putLong(tail);
    //     // buffer.flip();
    //     // int size = fc.write(buffer, offset);
    //     // fc.force(true);
    //     ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    //     buffer.putLong(head);
    //     buffer.flip();
    //     int size = fc.write(buffer, offset);
    //     fc.force(true);
    //     return size;
    // }
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

    synchronized DataMeta addOne(ByteBuffer data, DataMeta src) throws IOException{
        DataMeta res = new DataMeta(src.head, src.metaOffset, src.tail, src.totalNum);

        long size = data.remaining() + Long.BYTES * 2;

        // 写入数据

        long offset = FREE_OFFSET.getAndAdd(size);
        long newTail = offset;

        long nextOffset = -1L;
        ByteBuffer byteData = ByteBuffer.allocate((int)size);
        byteData.putLong(size - (Long.BYTES + Long.BYTES));
        byteData.putLong(nextOffset);
        byteData.put(data);
        byteData.flip();
        int len = fc.write(byteData,offset);

        // 更新链表
        if(src.tail != -1L){
            updateLink(src.tail,  newTail);
        }else{
            res.tail = newTail;
            res.head = newTail;
            res.metaOffset = newTail;
        }
        
        res.totalNum++;

        return res;
    }
}
