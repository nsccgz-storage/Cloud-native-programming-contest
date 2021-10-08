package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class Test1Benchmark {
	public static void main(String[] args) {
		// testByteBufferAllocate();
		testDeque();
		// testConcurrentMap();
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
		double latency = (endTime - startTime);
		int ret = 0;

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

	public static void testDeque(){
		Deque<Integer> dq = new ArrayDeque<>();
		// Deque<Integer> dq = new LinkedList<>();
		// ArrayBlockingQueue<Integer> dq = new ArrayBlockingQueue<>(10000);
		
		long startTime = System.nanoTime();
		long endTime = System.nanoTime();
		double latency = (endTime - startTime);
		int benchNum = 10000;

		startTime = System.nanoTime();
		for (int i = 0; i < benchNum; i++){
			dq.add(i);
			// dq.addLast(i);
		}
		endTime = System.nanoTime();
		latency  = (endTime-startTime)/benchNum;
		System.out.println("put latency : " + latency + " ns");

		int ret = 0;
		startTime = System.nanoTime();
		for (int i = 0; i < benchNum; i++){
			ret = dq.poll();
			// ret = dq.getFirst();
			// dq.removeFirst();
		}
		endTime = System.nanoTime();
		latency  = (endTime-startTime)/benchNum;
		System.out.println("get latency : " + latency + " ns");
		System.out.println(ret);



	}

	
}
