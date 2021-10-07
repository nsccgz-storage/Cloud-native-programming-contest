package io.openmessaging;

import io.openmessaging.MessageQueue;
import io.openmessaging.DefaultMessageQueueImpl;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.io.IOException;
import java.lang.Integer;
import java.util.Random;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.Vector;

import org.apache.log4j.spi.LoggerFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.util.concurrent.CyclicBarrier;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import java.util.concurrent.BrokenBarrierException;

public class Test1 {
	private static final Logger log = Logger.getLogger(Test1.class);
	public static Random rand = new Random();
	public static byte[] sampleData = new byte[17408];

	private static class Message {
		String topic;
		int queueId;
		long offset;
		long getOffset;
		ByteBuffer buf;
		ByteBuffer checkBuf;
		int oriPosition;

		Message(String msgTopic, int msgQueueId, long msgOffset) {
			topic = msgTopic;
			queueId = msgQueueId;
			offset = msgOffset;
			getOffset = -1;
			// get size between 100B to 17KiB (17408 B)
			int size = rand.nextInt(17308); // [0 - 17308]
			// size += 100; // [100-17408]
			buf = ByteBuffer.allocate(17408);
			buf.put(sampleData.clone());
			buf.position(size);
			oriPosition = size;
			// for check

			checkBuf = buf.duplicate();
			// checkBuf = ByteBuffer.allocate(17408);
			// checkBuf.put(sampleData.clone());
			// checkBuf.position(size);
		}
	}


	public static Vector<Message> generateTopic(int i) {
		String topicName = "topic" + i;
		Vector<Message> msgs = new Vector<>();
		for (long offset = 0; offset < 999; offset++) {
			for (int queueId = 0; queueId < 99; queueId++) {
				Message msg = new Message(topicName, queueId, offset);
				msgs.add(msg);
			}
		}
		return msgs;
	}

	public static Vector<Message> generateGetRange(int i) {
		String topicName = "topic" + i;
		Vector<Message> msgs = new Vector<>();
		for (long offset = 900; offset < 1300; offset++) {
			for (int queueId = 50; queueId < 150; queueId++) {
				Message msg = new Message(topicName, queueId, offset);
				msgs.add(msg);
			}
		}
		return msgs;
	}
	public static Vector<Message> generateQueueTestGetRangeMulti(int i) {
		String topicName = "topic" + i;
		int queueId = 23423;
		Vector<Message> msgs = new Vector<>();
		for (long offset = 0; offset < 999; offset++) {
			Message msg = new Message(topicName, queueId, offset);
			msgs.add(msg);
		}
		return msgs;
	}
	


	public static void threadRun(int threadId, MessageQueue mq, CyclicBarrier barrier) {
		try {
			{
				Vector<Message> msgs = generateTopic(threadId);
				if (threadId == 0){
					log.info("init messages ok");
				}
				barrier.await();
	
				if (threadId == 0){
					log.info("begin write!");
				}
			for (int i = 0; i < msgs.size(); i++) {
					Message msg = msgs.get(i);
					msg.getOffset = mq.append(msg.topic, msg.queueId, msg.buf);
					if (msg.getOffset != msg.offset) {
						log.error("offset error !");
						System.exit(0);
					}
					Map<Integer, ByteBuffer> result;
					result = mq.getRange(msg.topic, msg.queueId, msg.offset, 1);
					msg.buf.position(msg.oriPosition);
					if (result.get(0).compareTo(msg.checkBuf) != 0) {
						log.error(result.get(0));
						log.error(msg.checkBuf);
						log.error("data error !");
						barrier.await();
						System.exit(0);
					}
	
				}
				barrier.await();
	
				if (threadId == 0){
					log.info("begin read!");
				}
				Map<Integer, ByteBuffer> result;
				for (int i = 0; i < msgs.size(); i++) {
					Message msg = msgs.get(i);
	
					result = mq.getRange(msg.topic, msg.queueId, msg.offset, 1);
					msg.buf.position(msg.oriPosition);
					if (result.get(0).compareTo(msg.checkBuf) != 0) {
						log.error("data error !");
						System.exit(0);
					}
				}

				msgs = generateGetRange(threadId);

				if (threadId == 0){
					log.info("begin other read!");
				}
				for (int i = 0; i < msgs.size(); i++) {
					Message msg = msgs.get(i);
					result = mq.getRange(msg.topic, msg.queueId, msg.offset, 1);
					msg.buf.position(0);
				}
			}
			// {

			// 	Map<Integer, ByteBuffer> result;
			// 	barrier.await();
			// 	Vector<Message> getRangeMsgs = generateQueueTestGetRangeMulti(threadId);
			// 	barrier.await();
				
				
			// 	if (threadId == 0){
			// 		log.info("begin getRangeFetchMulti!");
			// 	}
			// 	for (int i = 0; i < getRangeMsgs.size(); i++){
			// 		log.debug("i : "+i);
			// 		Message msg = getRangeMsgs.get(i);
			// 		log.debug(msg.buf);
			// 		msg.getOffset = mq.append(msg.topic, msg.queueId, msg.buf);
			// 		if (msg.getOffset != msg.offset) {
			// 			log.error("offset error !");
			// 			System.exit(0);
			// 		}
			// 		result = mq.getRange(msg.topic, msg.queueId, 0, i+1);
			// 		for (int j = 0; j <= i; j++){
			// 			log.info(result.get(j));
			// 			log.info(getRangeMsgs.get(j).checkBuf);

			// 			if (result.get(j).compareTo(getRangeMsgs.get(j).checkBuf) != 0){
			// 				log.error("data error !");
			// 				byte[] tmp = getRangeMsgs.get(j).checkBuf.array();
			// 				log.info("***************real*************************");
			// 				for(int ii=0;  ii < tmp.length; ++ii){
			// 					System.out.print(tmp[ii] + " ");
			// 				}
			// 				log.info("***************end*************************");
			// 				System.exit(0);
			// 			}
					
			// 		}
			// 		for (int k = 0; k <= 40 && k <= i; k++){
			// 			log.debug("k : " + k);
			// 			log.debug("i-k : " + (i-k));
			// 			result = mq.getRange(msg.topic, msg.queueId, i-k, k+1);
			// 			for (int j = 0; j <= 40 && j<= k ; j++){
			// 				log.debug("j : "+j);
			// 				log.debug(result.get(j));
			// 				log.debug("i-k+j : "+ (i-k+j));
			// 				log.debug(getRangeMsgs.get(i-k+j).checkBuf);
			// 				if (result.get(j).compareTo(getRangeMsgs.get(i-k+j).checkBuf) != 0){
			// 					log.error("data error !");
			// 					System.exit(0);
			// 				}

			// 			}

			// 		}
	
				// }
			// 	barrier.await();

			// }

		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (BrokenBarrierException e) {
			e.printStackTrace();
		}

	}

	public static void testThreadPool(String dbPath) {
		MessageQueue mq = new Test1MessageQueueImpl(dbPath);
		// int numOfThreads = 1;
		int numOfThreads = 40;
		CyclicBarrier barrier = new CyclicBarrier(numOfThreads);
		ExecutorService executor = Executors.newFixedThreadPool(numOfThreads);
		long startTime = System.nanoTime();
		for (int i = 0; i < numOfThreads; i++) {
			final int thread_id = i;
			executor.execute(() -> {
				threadRun(thread_id, mq, barrier);

			});
		}
		executor.shutdown();
		try {
			// Wait a while for existing tasks to terminate
			while (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
				System.out.println("Pool did not terminate, waiting ...");
			}
		} catch (InterruptedException ie) {
			executor.shutdownNow();
			ie.printStackTrace();
		}
		long elapsedTime = System.nanoTime() - startTime;
		double elapsedTimeS = (double) elapsedTime / (1000 * 1000 * 1000);

		log.info("pass the test, successfully !!!");
		log.info("time: " + elapsedTimeS);
	}

	public static void init(){
		for (int i = 0; i < 17408; i++){
			sampleData[i] = (byte)i;
		}
	}
	public static void main(String[] args) {
		init();
		// log.setLevel(Level.DEBUG);
		log.setLevel(Level.INFO);
		if (args.length < 1){
			System.out.println("java SSDBench ${dbPath}");
			return ;
		}
		System.out.println("dbPath : " + args[0]);
		String dbPath = args[0] ;

		try {
			testThreadPool(dbPath);
		} catch (Exception e) {
			//TODO: handle exception
			e.printStackTrace();
		}
		
		// testThreadPool(dbPath);
		// Test1MessageQueue mq = new Test1MessageQueue("/mnt/nvme/mq");
		// int ioSize = 1000;
		// byte[] data = new byte[ioSize];
		// for (int i = 0; i < ioSize; i++){
		// data[i] = (byte)(i+96);
		// }
		// System.out.println(Arrays.toString(data));

		// for (int i = 0; i < 50; i++){
		// for (int j = 0; j < 10; j++){
		// mq.append("topic",i , ByteBuffer.wrap(data));
		// }
		// }
		// for (int i = 0; i < 50; i++){
		// for (int j = 0; j < 10; j++){
		// mq.getRange("topic", i, j, 1);
		// }
		// }
		// Map<Integer, ByteBuffer> ret = mq.getRange("topic", 3, 0, 3);
		// // System.out.println(ret);
		// // System.out.println(ret.get(0));
		// System.out.println(Arrays.toString(ret.get(0).array()));
		// System.out.println(Arrays.toString(ret.get(1).array()));
		// byte[] getdata = ret.get(0).array();
		// for (int i = 0; i < getdata.length; i++){
		// System.out.println(getdata[i]);
		// }

		// for (int i = 0; i < getdata.length; i++){
		// }

		// MessageQueue mq = new DefaultMessageQueueImpl();
		// int ioSize = 4096;
		// byte[] data = new byte[ioSize];
		// for (int i = 0; i < ioSize; i++){
		// data[i] = (byte)i;
		// }
		// mq.append("topic", 1324124, ByteBuffer.wrap(data));
		// mq.append("topic", 1324124, ByteBuffer.wrap(data));
		// mq.append("topic", 1324124, ByteBuffer.wrap(data));
		// mq.append("topic", 1324124, ByteBuffer.wrap(data));
		// Map<Integer, ByteBuffer> ret = mq.getRange("topic", 1324124, 0, 1);
	}
}
