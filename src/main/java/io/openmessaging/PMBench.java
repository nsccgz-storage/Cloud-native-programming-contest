package io.openmessaging;

import java.nio.ByteBuffer;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;


import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import com.intel.pmem.llpl.MemoryPool;

public class PMBench {
	public static final Unsafe UNSAFE;
	static {
	    try {
		Field field = Unsafe.class.getDeclaredField("theUnsafe");
		field.setAccessible(true);
		UNSAFE = (Unsafe) field.get(null);
	    } catch (Exception e) {
		throw new RuntimeException(e);
	    }
	}
    	private static final Logger log = Logger.getLogger(PMBench.class);
	public static void main(String[] args) {
		benchWrite("/mnt/pmem/mq/data");
		// benchWriteUnsafe("/mnt/pmem/mq/data");
		// benchWriteUnsafeDirect("/mnt/pmem/mq/data");
		// benchWriteUnsafeDirectNT("/mnt/pmem/mq/data");
	}

	public static void runStandardBench(String pmDirPath){
		benchWrite(pmDirPath);
	}

	public static long getPMPoolAddr(MemoryPool pool){
	       	long pmPoolAddr = 0;
		try{
		        Field pmAddrField = pool.getClass().getDeclaredField("poolAddress");
		        pmAddrField.setAccessible(true);
	        	pmPoolAddr = (long)pmAddrField.get(pool);
		} catch (Exception e){
			e.printStackTrace();
		}
		return pmPoolAddr;
	}


	public static void benchWriteUnsafeDirectNT(String pmDataPath){
		// TODO
		try {

		long totalCapacity = 60L*1024L*1024L*1024L;
		MemoryPool pool = MemoryPool.createPool(pmDataPath, totalCapacity);
		long pmPoolAddr = getPMPoolAddr(pool);
		log.info("pmPoolAddr: " + pmPoolAddr);

		// get nativeCopyMemoryNT
		Class<?> MemoryPoolClass = Class.forName("com.intel.pmem.llpl.MemoryPoolImpl");
		Method nativeCopyMemoryNT = MemoryPoolClass.getDeclaredMethod("nativeCopyMemoryNT",long.class, long.class, long.class);
		nativeCopyMemoryNT.setAccessible(true);
		

		String type = "seqWrite";
		int thread = 1;
		long benchBlockSize = 4*1024L*1024*1024L;

		for (long i = 0; i < benchBlockSize; i++){
			pool.setByte(i, (byte)0);
		}

		long curPosition = 0L;
		// int ioSize = 4096;
		int ioSize = 4096*1024;
		long benchCount = benchBlockSize/ioSize;

		byte[] sampleData = new byte[ioSize];
		for (int i = 0; i < ioSize; i++){
			sampleData[i] = (byte)i;
		}
		ByteBuffer sampleDataBuffer = ByteBuffer.allocateDirect(ioSize);
		for (int i = 0; i < ioSize; i++){
			sampleDataBuffer.put((byte)i);
		}
		long sampleDataAddr = ((DirectBuffer)sampleDataBuffer).address();

		log.info("start!");
		long startTime = System.nanoTime();
		while (curPosition < benchBlockSize){
			nativeCopyMemoryNT.invoke(null, sampleDataAddr, pmPoolAddr, ioSize);
			// UNSAFE.copyMemory(null, sampleDataAddr, null, pmPoolAddr + curPosition, ioSize);
			// pool.copyFromByteArrayNT(sampleData, 0, curPosition, ioSize);
			curPosition += ioSize;
		}
		long endTime = System.nanoTime();
		long elapsedTime = endTime - startTime;
		double elapsedTimeS = (double)elapsedTime/(1000*1000*1000);

		double bandwidth = ((double)benchBlockSize/elapsedTimeS)/(1024*1024);
		double iops = (double)benchCount/elapsedTimeS;
		double latency = (double)elapsedTime/benchCount;


	        String output = String.format("%s,%d,%d,%.3f,%.3f,%.3f", type, thread, ioSize, bandwidth, iops, latency);
		log.info(output);

		} catch (Exception e){
			e.printStackTrace();
		}

	}


	public static void benchWriteUnsafeDirect(String pmDataPath){
		long totalCapacity = 60L*1024L*1024L*1024L;
		MemoryPool pool = MemoryPool.createPool(pmDataPath, totalCapacity);
		long pmPoolAddr = getPMPoolAddr(pool);
		log.info("pmPoolAddr: " + pmPoolAddr);

		String type = "seqWrite";
		int thread = 1;
		long benchBlockSize = 4*1024L*1024*1024L;

		for (long i = 0; i < benchBlockSize; i++){
			pool.setByte(i, (byte)0);
		}

		long curPosition = 0L;
		// int ioSize = 4096;
		int ioSize = 4096*1024;
		long benchCount = benchBlockSize/ioSize;

		byte[] sampleData = new byte[ioSize];
		for (int i = 0; i < ioSize; i++){
			sampleData[i] = (byte)i;
		}
		ByteBuffer sampleDataBuffer = ByteBuffer.allocateDirect(ioSize);
		for (int i = 0; i < ioSize; i++){
			sampleDataBuffer.put((byte)i);
		}
		long sampleDataAddr = ((DirectBuffer)sampleDataBuffer).address();

		log.info("start!");
		long startTime = System.nanoTime();
		while (curPosition < benchBlockSize){
			UNSAFE.copyMemory(null, sampleDataAddr, null, pmPoolAddr + curPosition, ioSize);
			// pool.copyFromByteArrayNT(sampleData, 0, curPosition, ioSize);
			curPosition += ioSize;
		}
		long endTime = System.nanoTime();
		long elapsedTime = endTime - startTime;
		double elapsedTimeS = (double)elapsedTime/(1000*1000*1000);

		double bandwidth = ((double)benchBlockSize/elapsedTimeS)/(1024*1024);
		double iops = (double)benchCount/elapsedTimeS;
		double latency = (double)elapsedTime/benchCount;


	        String output = String.format("%s,%d,%d,%.3f,%.3f,%.3f", type, thread, ioSize, bandwidth, iops, latency);
		log.info(output);

	}


	public static void benchWriteUnsafe(String pmDataPath){
		long totalCapacity = 60L*1024L*1024L*1024L;
		MemoryPool pool = MemoryPool.createPool(pmDataPath, totalCapacity);
		long pmPoolAddr = getPMPoolAddr(pool);
		log.info("pmPoolAddr: " + pmPoolAddr);

		String type = "seqWrite";
		int thread = 1;
		long benchBlockSize = 4*1024L*1024*1024L;

		for (long i = 0; i < benchBlockSize; i++){
			pool.setByte(i, (byte)0);
		}

		long curPosition = 0L;
		// int ioSize = 4096;
		int ioSize = 4096*1024;
		long benchCount = benchBlockSize/ioSize;

		byte[] sampleData = new byte[ioSize];
		for (int i = 0; i < ioSize; i++){
			sampleData[i] = (byte)i;
		}

		log.info("start!");
		long startTime = System.nanoTime();
		while (curPosition < benchBlockSize){
			UNSAFE.copyMemory(sampleData, 16, null, pmPoolAddr + curPosition, ioSize);
			// pool.copyFromByteArrayNT(sampleData, 0, curPosition, ioSize);
			curPosition += ioSize;
		}
		long endTime = System.nanoTime();
		long elapsedTime = endTime - startTime;
		double elapsedTimeS = (double)elapsedTime/(1000*1000*1000);

		double bandwidth = ((double)benchBlockSize/elapsedTimeS)/(1024*1024);
		double iops = (double)benchCount/elapsedTimeS;
		double latency = (double)elapsedTime/benchCount;


	        String output = String.format("%s,%d,%d,%.3f,%.3f,%.3f", type, thread, ioSize, bandwidth, iops, latency);
		log.info(output);

	}


	public static void benchWrite(String pmDataPath){
		long totalCapacity = 60L*1024L*1024L*1024L;
		MemoryPool pool = MemoryPool.createPool(pmDataPath, totalCapacity);

		String type = "seqWrite";
		int thread = 1;
		long benchBlockSize = 4*1024L*1024*1024L;

		for (long i = 0; i < benchBlockSize; i++){
			pool.setByte(i, (byte)0);
		}

		long curPosition = 0L;
		// int ioSize = 4096;
		int ioSize = 4096*1024;
		long benchCount = benchBlockSize/ioSize;

		byte[] sampleData = new byte[ioSize];
		for (int i = 0; i < ioSize; i++){
			sampleData[i] = (byte)i;
		}
		// ByteBuffer sampleDataBuffer = ByteBuffer.allocate(ioSize);
		// ByteBuffer sampleDataBuffer = ByteBuffer.allocateDirect(ioSize);

		log.info("start!");
		long startTime = System.nanoTime();
		while (curPosition < benchBlockSize){
			// pool.copyFromByteArrayNT(sampleDataBuffer.array(), 0, curPosition, ioSize);
			pool.copyFromByteArrayNT(sampleData, 0, curPosition, ioSize);
			curPosition += ioSize;
		}
		long endTime = System.nanoTime();
		long elapsedTime = endTime - startTime;
		double elapsedTimeS = (double)elapsedTime/(1000*1000*1000);

		double bandwidth = ((double)benchBlockSize/elapsedTimeS)/(1024*1024);
		double iops = (double)benchCount/elapsedTimeS;
		double latency = (double)elapsedTime/benchCount;


	        String output = String.format("%s,%d,%d,%.3f,%.3f,%.3f", type, thread, ioSize, bandwidth, iops, latency);
		log.info(output);

	}
	
}
