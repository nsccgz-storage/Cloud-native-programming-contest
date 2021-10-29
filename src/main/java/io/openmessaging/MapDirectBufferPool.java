package io.openmessaging;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.*;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.lang.ThreadLocal;
import java.util.concurrent.Callable;

import com.intel.pmem.llpl.MemoryPool;


import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;
import java.lang.reflect.Field;


public class MapDirectBufferPool {
        public final Field byteBufferAddress;
        public final Field byteBufferCapacity;
        ByteBuffer[] dbs;
        int capacity;
        int cur;
        ByteBuffer baseByteBuffer;
    
    
        MapDirectBufferPool(){
            try {
                byteBufferAddress = Buffer.class.getDeclaredField("address");
                byteBufferAddress.setAccessible(true);
                byteBufferCapacity = Buffer.class.getDeclaredField("capacity");
                byteBufferCapacity.setAccessible(true);
            } catch (Exception e) {
            throw new RuntimeException(e);
            }


            capacity = 100;
            cur = 0;
            dbs = new ByteBuffer[capacity];
            // baseByteBuffer = ByteBuffer.allocateDirect(1);
            for (int i = 0; i < capacity; i++){
                // dbs[i] = baseByteBuffer.duplicate();
                dbs[i] = ByteBuffer.allocateDirect(0).order(ByteOrder.nativeOrder());
                try {
                    byteBufferCapacity.setInt(dbs[i], 17*1024);
                } catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
    
        ByteBuffer getNewMapDirectByteBuffer(long addr, int dataLength){
            ByteBuffer buf = dbs[cur];
            try {
                byteBufferAddress.setLong(buf, addr);
                buf.clear();
                buf.limit(dataLength);
                // System.out.println("ok!");
            } catch (Exception e){
                e.printStackTrace();
            }
            cur = (cur + 1) % capacity;
            return buf;
        }
    }