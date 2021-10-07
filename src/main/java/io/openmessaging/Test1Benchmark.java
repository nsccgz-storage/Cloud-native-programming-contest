package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

public class Test1Benchmark {
	public static void main(String[] args) {
		// testByteBufferAllocate();
		testConcurrentMap();
	}

	public static void testByteBufferAllocate(){
		int benchNum = 1000;
		int benchSize = 17408;

		ByteBuffer[] bufs = new ByteBuffer[benchNum];
		long startTime = System.nanoTime();
		for (int i = 0; i < benchNum; i++){
			bufs[i] = ByteBuffer.allocate(benchSize);
		}
		long endTime = System.nanoTime();
		double latency = (endTime - startTime)/benchNum;

		System.out.println("latency : "+latency + " ns");

	}

	public static void testConcurrentMap(){
		ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<>();
		long startTime = System.nanoTime();
		long endTime = System.nanoTime();
		int ret = 0;
		double latency = (endTime - startTime);

		startTime = System.nanoTime();
		for (int i = 0; i < 100; i++){
			map.put("topic"+i, i);
		}
		endTime = System.nanoTime();
		latency = (endTime - startTime)/100;
		System.out.println("put latency : " + latency + " ns");

		startTime = System.nanoTime();
		for (int i = 0; i < 100; i++){
			ret = map.get("topic"+i);
		}
		endTime = System.nanoTime();
		latency = (endTime - startTime)/100;
		System.out.println("get latency : " + latency + " ns");
		System.out.println(ret);
	}

	
}
