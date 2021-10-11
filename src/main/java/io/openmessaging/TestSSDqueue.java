package io.openmessaging;

import io.openmessaging.MessageQueue;
import io.openmessaging.DefaultMessageQueueImpl;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;

import java.util.concurrent.BrokenBarrierException;

public class TestSSDqueue {
	private static final Logger log = Logger.getLogger(TestSSDqueue.class);
	public static Random rand = new Random();

	private static class Message {
		String topic;
		int queueId;
		long offset;
		long getOffset;
		ByteBuffer buf;

		Message(String msgTopic, int msgQueueId, long msgOffset) {
			topic = msgTopic;
			queueId = msgQueueId;
			offset = msgOffset;
			getOffset = -1;
			// get size between 100B to 17KiB (17408 B)
			int size = rand.nextInt(17308); // [0 - 17308]
			size += 100; // [100-17408]
			byte[] data = new byte[size];
			for (int i = 0; i < size; i++) {
				data[i] = (byte) i;
			}
			buf = ByteBuffer.wrap(data);
		}
	}

	public static Vector<Message> generateOne() {
		String topicName = "tipic";
		Vector<Message> msgs = new Vector<>();
		for (long offset = 0; offset < 999; offset++) {
			for (int queueId = 0; queueId < 99; queueId++) {
				Message msg = new Message(topicName, queueId, offset);
				msgs.add(msg);
			}
		}
		return msgs;
	}

	public static void testOne() {
		Test1MessageQueue mq = new Test1MessageQueue("/mnt/nvme/mq");
		Vector<Message> msgs = generateOne();
		for (int i = 0; i < msgs.size(); i++) {
			Message msg = msgs.get(i);
			msg.getOffset = mq.append(msg.topic, msg.queueId, msg.buf);
			if (msg.getOffset != msg.offset) {
				log.error("offset error !");
			}
		}
		Map<Integer, ByteBuffer> result;
		for (int i = 0; i < msgs.size(); i++) {
			Message msg = msgs.get(i);
			result = mq.getRange(msg.topic, msg.queueId, msg.offset, 1);
			if (result.get(0).compareTo(msg.buf) != 0) {
				log.error("data error !");
			}
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

	public static void threadRun(int threadId, SSDqueue mq, CyclicBarrier barrier) {
		Vector<Message> msgs = generateTopic(threadId);
		log.info("init messages ok");
		try {
			barrier.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (BrokenBarrierException e) {
			e.printStackTrace();
		}
		log.info("begin append!");
		for (int i = 0; i < msgs.size(); i++) {
			Message msg = msgs.get(i);
			
			msg.getOffset = mq.append(msg.topic, msg.queueId, msg.buf);
			
			if (msg.getOffset != msg.offset) {
				log.error("offset error !");
				return;
			}
		}
		try {
			barrier.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (BrokenBarrierException e) {
			e.printStackTrace();
		}

		log.info("begin getRange!");

		Map<Integer, ByteBuffer> result;
		for (int i = 0; i < msgs.size(); i++) {
			Message msg = msgs.get(i);
			
			result = mq.getRange(msg.topic, msg.queueId, msg.offset, 1);
			msg.buf.position(0);
			if (result.get(0).compareTo(msg.buf) != 0) {
				log.error("data error !");
				return;
			}
		}
	}

	public static void testThreadPool() {

		String metaPath = "/mnt/nvme/mq/MetaData";
		String dataPath = "/mnt/nvme/mq/Data";
		// System.out.println(" 28 ");

	
			//FileChannel fileChannel = new RandomAccessFile(new File(dataPath), "rw").getChannel();
			//FileChannel metaFileChannel = new RandomAccessFile(new File(metaPath), "rw").getChannel();
			SSDqueue mq = new SSDqueue("/mnt/ssd/wyk", "/mnt/pmem/wyk");
			int numOfThreads = 16;
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
			log.info("time: " + elapsedTimeS);
			mq.freePmemThread.interrupt();
			mq.writePmemThread.interrupt();

		
	}

	public static void main(String[] args) {
		// testOne();
		testThreadPool();
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
