package io.openmessaging;

import java.nio.ByteBuffer;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import com.intel.pmem.llpl.MemoryPool;
import java.nio.MappedByteBuffer;
import java.nio.ByteBuffer;
import java.nio.Buffer;


public class PMDirectByteBufferPool {

	public static final Unsafe UNSAFE;
	public static Field byteBufferAddress;
	public static Field byteBufferCapacity;
	static {
	    try {
		Field field = Unsafe.class.getDeclaredField("theUnsafe");
		field.setAccessible(true);
		UNSAFE = (Unsafe) field.get(null);
		byteBufferAddress = Buffer.class.getDeclaredField("address");
		byteBufferAddress.setAccessible(true);
		byteBufferCapacity = Buffer.class.getDeclaredField("capacity");
		byteBufferCapacity.setAccessible(true);
	    } catch (Exception e) {
		throw new RuntimeException(e);
	    }
	}
	ByteBuffer[] dbs;
	int capacity;
	int cur;
	ByteBuffer baseByteBuffer;


	PMDirectByteBufferPool(){
		capacity = 100;
		cur = 0;
		dbs = new ByteBuffer[capacity];
		baseByteBuffer = ByteBuffer.allocateDirect(1);
		for (int i = 0; i < capacity; i++){
			// dbs[i] = baseByteBuffer.duplicate();
			dbs[i] = ByteBuffer.allocateDirect(0);
		}
	}

	ByteBuffer getNewPMDirectByteBuffer(long pmAddr, int capacity){
		ByteBuffer buf = dbs[cur];
		try {
			buf.clear();
			byteBufferAddress.setLong(buf, pmAddr);
			byteBufferCapacity.set(buf, capacity);
			buf.limit(capacity);
			// System.out.println("ok!");
		} catch (Exception e){
			e.printStackTrace();
		}
		cur = (cur + 1) % capacity;
		return buf;
	}
}