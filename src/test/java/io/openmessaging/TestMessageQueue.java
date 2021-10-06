package io.openmessaging;

import org.junit.Test;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

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

import io.openmessaging.MessageQueue;
import io.openmessaging.Test1MessageQueue;
import io.openmessaging.DefaultMessageQueueImpl;
import io.openmessaging.Test1MessageQueueImpl;

import java.nio.ByteBuffer;

import java.util.concurrent.BrokenBarrierException;
import java.io.File;

public class TestMessageQueue {
	private static final Logger log = Logger.getLogger(Test1.class);
	public static Random rand = new Random();
	public static byte[] sampleData = new byte[17408];
	public static String dbPath = "/mnt/nvme/mq";
	// public static String dbPath = "/mnt/ssd/mq";
	// TODO: just for test
	// public static String dbPath = ".";

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
			checkBuf = ByteBuffer.allocate(17408);
			checkBuf.put(sampleData.clone());
			checkBuf.position(size);

		}
	}

	@Test
	public void testGetHelloWorld() {
		System.out.println("Helloworld");
	}

	@Test
	public void testSingleThreadAppendAndGetRange() {
		log.info("Start : testSingleThreadAppendAndGetRange");
		deleteDir(dbPath);
		mkdirDir(dbPath);
		MessageQueue mq = new Test1MessageQueueImpl(dbPath);
		String topicName = "testSingleThreadAppendAndGetRange";
		Vector<Message> msgs = new Vector<>();
		for (long offset = 0; offset < 10; offset++) {
			for (int queueId = 0; queueId < 99; queueId++) {
				Message msg = new Message(topicName, queueId, offset);
				msgs.add(msg);
			}
		}

		for (int i = 0; i < msgs.size(); i++) {
			Message msg = msgs.get(i);
			msg.getOffset = mq.append(msg.topic, msg.queueId, msg.buf);
			if (msg.getOffset != msg.offset) {
				log.error("offset error !");
			}
			Map<Integer, ByteBuffer> result;
			result = mq.getRange(msg.topic, msg.queueId, msg.offset, 1);
			if (result.get(0).compareTo(msg.checkBuf) != 0) {
				log.error("data error !");
				System.exit(0);
			}
		}
		Map<Integer, ByteBuffer> result;
		for (int i = 0; i < msgs.size(); i++) {
			Message msg = msgs.get(i);
			result = mq.getRange(msg.topic, msg.queueId, msg.offset, 1);
			if (result.get(0).compareTo(msg.checkBuf) != 0) {

				log.info("topic: " + msg.topic + " id: " + msg.queueId + " offset: " + msg.offset
						+ " buffer: " + result.get(0));

				byte[] tmp = msg.buf.array();
				log.info("***************real*************************");
				for (int ii = 0; ii < tmp.length; ++ii) {
					System.out.print(tmp[ii] + " ");
				}
				log.info("***************end*************************");
				log.error("data error !");
				System.exit(0);
			}
		}

	}

	@Test
	public void testMultiThreadAppendAndGetRange() {
		deleteDir(dbPath);
		mkdirDir(dbPath);
		MessageQueue mq = new Test1MessageQueueImpl(dbPath);
		log.info("Start : testMultiThreadAppendAndGetRange");
		int numOfThreads = 20;
		CyclicBarrier barrier = new CyclicBarrier(numOfThreads);
		ExecutorService executor = Executors.newFixedThreadPool(numOfThreads);
		long startTime = System.nanoTime();
		for (int i = 0; i < numOfThreads; i++) {
			final int thread_id = i;
			executor.execute(() -> {
				threadRunTestMultiThreadAppendAndGetRange(thread_id, mq, barrier);

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

	public static void threadRunTestMultiThreadAppendAndGetRange(int threadId, MessageQueue mq,
			CyclicBarrier barrier) {
		try {
			String topicName = "testMultiThreadAppendAndGetRange" + threadId;
			Vector<Message> msgs = new Vector<>();
			for (long offset = 0; offset < 999; offset++) {
				for (int queueId = 0; queueId < 9; queueId++) {
					Message msg = new Message(topicName, queueId, offset);
					msgs.add(msg);
				}
			}
			if (threadId == 0) {
				log.info("init messages ok");
			}
			barrier.await();

			if (threadId == 0) {
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
				if (result.get(0).compareTo(msg.buf) != 0) {
					log.error("data error !");
					barrier.await();
					System.exit(0);
				}

			}
			barrier.await();

			if (threadId == 0) {
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

		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (BrokenBarrierException e) {
			e.printStackTrace();
		}

	}

	public static boolean deleteDir(String path) {
		File file = new File(path);
		if (!file.exists()) {// 判断是否待删除目录是否存在
			System.err.println("The dir are not exists!");
			return false;
		}

		String[] content = file.list();// 取得当前目录下所有文件和文件夹
		for (String name : content) {
			File temp = new File(path, name);
			if (temp.isDirectory()) {// 判断是否是目录
				deleteDir(temp.getAbsolutePath());// 递归调用，删除目录里的内容
				temp.delete();// 删除空目录
			} else {
				if (!temp.delete()) {// 直接删除文件
					System.err.println("Failed to delete " + name);
				}
			}
		}
		return true;
	}

	public static boolean mkdirDir(String path) {
		File file = new File(path);
		if (file.exists()) {// 判断是否待删除目录是否存在
			System.err.println("The dir is exist!");
			return false;
		}
		file.mkdir();
		return true;

	}

	@Test
	public void testMultiThreadGetRangeFetchNum(){
		deleteDir(dbPath);
		mkdirDir(dbPath);
		MessageQueue mq = new Test1MessageQueueImpl(dbPath);
		log.info("Start : testMultiThreadAppendAndGetRange");
		int numOfThreads = 20;
		CyclicBarrier barrier = new CyclicBarrier(numOfThreads);
		ExecutorService executor = Executors.newFixedThreadPool(numOfThreads);
		long startTime = System.nanoTime();
		for (int i = 0; i < numOfThreads; i++) {
			final int thread_id = i;
			executor.execute(() -> {
				threadRunTestMultiThreadGetRangeFetchNum(thread_id, mq, barrier);

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

	public static void threadRunTestMultiThreadGetRangeFetchNum(int threadId, MessageQueue mq,
			CyclicBarrier barrier) {
		try {

			Map<Integer, ByteBuffer> result;
			barrier.await();
			String topicName = "topic" + threadId;
			int queueId = 23423;
			Vector<Message> getRangeMsgs = new Vector<>();
			for (long offset = 0; offset < 999; offset++) {
				Message msg = new Message(topicName, queueId, offset);
				getRangeMsgs.add(msg);
			}
			if (threadId == 0) {
				log.info("init messages ok");
			}
	
			barrier.await();
			
			
			if (threadId == 0){
				log.info("begin getRangeFetchMulti!");
			}
			for (int i = 0; i < getRangeMsgs.size(); i++){
				log.debug("i : "+i);
				Message msg = getRangeMsgs.get(i);
				log.debug(msg.buf);
				msg.getOffset = mq.append(msg.topic, msg.queueId, msg.buf);
				if (msg.getOffset != msg.offset) {
					log.error("offset error !");
					System.exit(0);
				}
				result = mq.getRange(msg.topic, msg.queueId, 0, i+1);
				for (int j = 0; j <= i; j++){
					// log.info(result.get(j));
					// log.info(getRangeMsgs.get(j).checkBuf);

					if (result.get(j).compareTo(getRangeMsgs.get(j).checkBuf) != 0){
						log.error("data error !");
						byte[] tmp = getRangeMsgs.get(j).checkBuf.array();
						log.info("***************real*************************");
						for(int ii=0;  ii < tmp.length; ++ii){
							System.out.print(tmp[ii] + " ");
						}
						log.info("***************end*************************");
						System.exit(0);
					}
				
				}
				for (int k = 0; k <= 40 && k <= i; k++){
					log.debug("k : " + k);
					log.debug("i-k : " + (i-k));
					result = mq.getRange(msg.topic, msg.queueId, i-k, k+1);
					for (int j = 0; j <= 40 && j<= k ; j++){
						log.debug("j : "+j);
						log.debug(result.get(j));
						log.debug("i-k+j : "+ (i-k+j));
						log.debug(getRangeMsgs.get(i-k+j).checkBuf);
						if (result.get(j).compareTo(getRangeMsgs.get(i-k+j).checkBuf) != 0){
							log.error("data error !");
							System.exit(0);
						}

					}

				}

			}
			barrier.await();


		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (BrokenBarrierException e) {
			e.printStackTrace();
		}

	}

}
