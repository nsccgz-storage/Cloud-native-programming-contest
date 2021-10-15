package io.openmessaging;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.ConnectException;
import java.nio.ByteBuffer;
import java.security.cert.TrustAnchor;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class MQBench {
	private static final Logger log = Logger.getLogger(MQBench.class);
	// public static byte[] sampleData = new byte[17408];

	private static class Message {
		int type; // 1 : append, 2: getRange
		String topic;
		int queueId;
		int dataLength;
		long offset;
		ByteBuffer buf;
		ByteBuffer buf2;
		ByteBuffer checkBuf;
		int fetchNum;
		Map<Integer, ByteBuffer> results;

		Message(String msgTopic, int msgQueueId, int l, byte[] sampleData) {
			// append
			type = 1;
			topic = msgTopic;
			queueId = msgQueueId;
			dataLength = l;
			buf = ByteBuffer.wrap(sampleData);
			buf.position(17408 - l);
			buf2 = buf.duplicate();
			checkBuf = buf.duplicate();
		}

		Message(String msgTopic, int msgQueueId, long msgOffset, int fNum) {
			// getRange
			type = 2;
			topic = msgTopic;
			queueId = msgQueueId;
			offset = msgOffset;
			fetchNum = fNum;
		}

		@Override
		public String toString() {
			StringBuffer output = new StringBuffer();
			if (type == 1) {
				output.append("append,");
			} else if (type == 2) {
				output.append("getRange,");
			}
			output.append(topic + ",");
			output.append(queueId + ",");
			if (type == 1) {
				output.append(dataLength + ",");
			} else if (type == 2) {
				output.append(offset + ",");
				output.append(fetchNum + ",");
			}
			return output.toString();
		}

	}

	// public static void init() {
	// 	for (int i = 0; i < 17408; i++) {
	// 		sampleData[i] = (byte) i;
	// 	}
	// }

	public static void correctBenchByTrace(String dbPath, String pmDirPath) {
		//MessageQueue mq = new LSMessageQueue(dbPath, pmDirPath);
		// MessageQueue mq = new SSDqueue(dbPath, pmDirPath);
		MessageQueue mq = new MyLSMessageQueue(dbPath, pmDirPath);
		int numOfThreads = 8;
		CyclicBarrier barrier = new CyclicBarrier(numOfThreads);
		ExecutorService executor = Executors.newFixedThreadPool(numOfThreads);
		long startTime = System.nanoTime();
		for (int i = 0; i < numOfThreads; i++) {
			final int threadId = i;
			executor.execute(() -> {
				threadRunCorrectBenchByTrace(threadId, mq, barrier);

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

	public static void threadRunCorrectBenchByTrace(int threadId, MessageQueue mq, CyclicBarrier barrier) {
		if (threadId == 0){
			log.info("start");
		}
		byte[] sampleData = new byte[17408];
		for (int i = 0; i < 17408; i++) {
			sampleData[i] = (byte) i;
		}

		try {
			String filename = "./workloads/workload.csv";
			BufferedReader reader = new BufferedReader(new FileReader(filename));
			String line = null;
			Vector<Message> step1Msgs = new Vector<>();
			Vector<Message> step2Msgs = new Vector<>();
			Vector<Message> msgs = step1Msgs;

			while ((line = reader.readLine()) != null) {
				// log.debug(line);
				String item[] = line.split(",");
				// log.debug(item[0]);

				String topic = item[1]+"0"+threadId;
				int queueId = Integer.parseInt(item[2]);
				if (item[0].compareTo("append") == 0) {
					int dataLength = Integer.parseInt(item[3]);
					Message msg = new Message(topic, queueId, dataLength, sampleData);
					// log.debug(msg);
					msgs.add(msg);
				} else if (item[0].compareTo("getRange") == 0) {
					if (msgs.equals(step1Msgs)) {
						msgs = step2Msgs;
					}
					long offset = Long.parseLong(item[3]);
					int fetchNum = Integer.parseInt(item[4]);
					Message msg = new Message(topic, queueId, offset, fetchNum);
					// log.debug(msg);
					msgs.add(msg);
				}
			}
			barrier.await();
			if (threadId == 0){
				log.info("init message ok");
				log.info("start step 1");
			}

			MessageQueue trueMQ = new MemoryMessageQueueImpl();
			// 不同的线程不会相互访问，所以每个线程起一个单独的mq用来检查也ok
			long trueOffset = 0;
			Map<Integer, ByteBuffer> trueResult;

			for (int i = 0; i < step1Msgs.size(); i++) {
				Message msg = step1Msgs.get(i);
				msg.offset = mq.append(msg.topic, msg.queueId, msg.buf);
				trueOffset = trueMQ.append(msg.topic, msg.queueId, msg.buf2);
				if (trueOffset != msg.offset){
					log.error("offset error");
					System.exit(-1);
				}
				// msg.results = mq.getRange(msg.topic, msg.queueId, trueOffset, 1);
				// trueResult = trueMQ.getRange(msg.topic, msg.queueId, trueOffset, 1);
				// if (msg.results.get(0).compareTo(trueResult.get(0)) != 0){
				// 	log.error(msg.results.get(0));
				// 	log.error(trueResult.get(0));
				// 	log.error("data error");
				// 	System.exit(-1);
				// }
			}

			barrier.await();

			if (threadId == 0){
				log.info("step 1 ok");
				log.info("start step 2");
			}

			for (int i = 0; i < step2Msgs.size(); i++) {
				Message msg = step2Msgs.get(i);
				if (msg.type == 1) {
					msg.offset = mq.append(msg.topic, msg.queueId, msg.buf);
					trueOffset = trueMQ.append(msg.topic, msg.queueId, msg.buf2);
					if (trueOffset != msg.offset){
						log.error("offset error");
						System.exit(-1);
					}
				} else if (msg.type == 2) {
					msg.results = mq.getRange(msg.topic, msg.queueId, msg.offset, msg.fetchNum);
					trueResult = trueMQ.getRange(msg.topic, msg.queueId, msg.offset, msg.fetchNum);
					for (int j = 0; j < msg.fetchNum; j++){
						if (trueResult.containsKey(j)){
							if (msg.results.get(j).compareTo(trueResult.get(j)) != 0){
								log.error("data error");
								log.error("j : "+ j);
								log.error(msg.results.get(j));
								log.error(trueResult.get(j));
								System.exit(-1);
							}
						}

					}
				}
			}

			if (threadId == 0){
				log.info("step 2 ok");
			}



			if (threadId == 0){
				log.info("pass !ok !!");
			}



		} catch (Exception e) {
			e.printStackTrace();
		}

	}


	public static void perfBenchByTrace(String dbPath, String pmDirPath) {
		//MessageQueue mq = new LSMessageQueue(dbPath, pmDirPath);
		// MessageQueue mq = new SSDqueue(dbPath, pmDirPath);
		MessageQueue mq = new MyLSMessageQueue(dbPath, pmDirPath);
		int numOfThreads = 40;
		CyclicBarrier barrier = new CyclicBarrier(numOfThreads);
		ExecutorService executor = Executors.newFixedThreadPool(numOfThreads);
		long startTime = System.nanoTime();
		for (int i = 0; i < numOfThreads; i++) {
			final int threadId = i;
			executor.execute(() -> {
				threadRunPerfBenchByTrace(threadId, mq, barrier);

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
		((MyLSMessageQueue)mq).shutdown();
	}

	public static void threadRunPerfBenchByTrace(int threadId, MessageQueue mq, CyclicBarrier barrier) {
		if (threadId == 0){
			log.info("start");
		}
		byte[] sampleData = new byte[17408];
		for (int i = 0; i < 17408; i++) {
			sampleData[i] = (byte) i;
		}

		try {
			String filename = "./workloads/workload.csv";
			BufferedReader reader = new BufferedReader(new FileReader(filename));
			String line = null;
			Vector<Message> step1Msgs = new Vector<>();
			Vector<Message> step2Msgs = new Vector<>();
			Vector<Message> msgs = step1Msgs;
			int num = 0;
			while ((line = reader.readLine()) != null) {
				// log.debug(line);
				//if(num++ % 7 != 0) continue;
				String item[] = line.split(",");
				// log.debug(item[0]);

				String topic = item[1]+"0"+threadId;
				int queueId = Integer.parseInt(item[2]);
				if (item[0].compareTo("append") == 0) {
					int dataLength = Integer.parseInt(item[3]);
					Message msg = new Message(topic, queueId, dataLength, sampleData);
					// log.debug(msg);
					msgs.add(msg);
				} else if (item[0].compareTo("getRange") == 0) {
					if (msgs.equals(step1Msgs)) {
						msgs = step2Msgs;
					}
					long offset = Long.parseLong(item[3]);
					int fetchNum = Integer.parseInt(item[4]);
					Message msg = new Message(topic, queueId, offset, fetchNum);
					// log.debug(msg);
					msgs.add(msg);
				}
			}
			barrier.await();
			long step1StartTime = System.nanoTime();
			if (threadId == 0){
				log.info("init message ok");
				log.info("start step 1");
			}

			// MessageQueue trueMQ = new MemoryMessageQueueImpl();

			for (int i = 0; i < step1Msgs.size(); i++) {
				Message msg = step1Msgs.get(i);
				msg.offset = mq.append(msg.topic, msg.queueId, msg.buf);
			}

			barrier.await();

			long step1EndTime = System.nanoTime();
			long step2StartTime = System.nanoTime();
			if (threadId == 0){
				log.info("step 1 ok");
				log.info("start step 2");
			}

			for (int i = 0; i < step2Msgs.size(); i++) {
				Message msg = step2Msgs.get(i);
				if (msg.type == 1) {
					msg.offset = mq.append(msg.topic, msg.queueId, msg.buf);
				} else if (msg.type == 2) {
					msg.results = mq.getRange(msg.topic, msg.queueId, msg.offset, msg.fetchNum);
				}
			}

			barrier.await();
			long step2EndTime = System.nanoTime();
			if (threadId == 0){
				log.info("step 2 ok");
				log.info("time of step 1: " + (step1EndTime-step1StartTime)/(1000*1000*1000));
				log.info("time of step 2: " + (step2EndTime-step2StartTime)/(1000*1000*1000));
			}

			if (threadId == 0){
				log.info("pass !ok !!");
			}





		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		// init();
		log.setLevel(Level.INFO);
		// log.setLevel(Level.DEBUG);
		String dbPath = "/mnt/nvme/wyk";
		// String dbPath = "/mnt/ssd/wyk";
		String pmDirPath = "/mnt/pmem/wyk";
		if(args.length >= 2){
			dbPath = args[0];
			pmDirPath = args[1];
		}

		log.info("test from MQBench");

		 correctBenchByTrace(dbPath, pmDirPath);

//		perfBenchByTrace(dbPath, pmDirPath);

		// log.setLevel(Level.INFO);
		// try {
		// 	String filename = "./workloads/workload.csv";
		// 	BufferedReader reader = new BufferedReader(new FileReader(filename));
		// 	String line = null;
		// 	Vector<Message> step1Msgs = new Vector<>();
		// 	Vector<Message> step2Msgs = new Vector<>();
		// 	Vector<Message> msgs = step1Msgs;
		// 	while ((line = reader.readLine()) != null) {
		// 		// log.debug(line);
		// 		String item[] = line.split(",");
		// 		// log.debug(item[0]);

		// 		String topic = item[1];
		// 		int queueId = Integer.parseInt(item[2]);
		// 		if (item[0].compareTo("append") == 0) {
		// 			int dataLength = Integer.parseInt(item[3]);
		// 			Message msg = new Message(topic, queueId, dataLength);
		// 			log.debug(msg);
		// 			msgs.add(msg);
		// 		} else if (item[0].compareTo("getRange") == 0) {
		// 			if (msgs.equals(step1Msgs)) {
		// 				msgs = step2Msgs;
		// 			}
		// 			long offset = Long.parseLong(item[3]);
		// 			int fetchNum = Integer.parseInt(item[4]);
		// 			Message msg = new Message(topic, queueId, offset, fetchNum);
		// 			log.debug(msg);
		// 			msgs.add(msg);
		// 		}
		// 	}

		// 	log.info("read message from file ok!");

		// 	MessageQueue trueMQ = new MemoryMessageQueueImpl();

		// 	for (int i = 0; i < step1Msgs.size(); i++) {
		// 		Message msg = step1Msgs.get(i);
		// 		msg.offset = trueMQ.append(msg.topic, msg.queueId, msg.buf);
		// 	}

		// 	for (int i = 0; i < step2Msgs.size(); i++) {
		// 		Message msg = step2Msgs.get(i);
		// 		if (msg.type == 1) {
		// 			msg.offset = trueMQ.append(msg.topic, msg.queueId, msg.buf);
		// 		} else if (msg.type == 2) {
		// 			msg.results = trueMQ.getRange(msg.topic, msg.queueId, msg.offset, msg.fetchNum);
		// 		}
		// 	}

		// } catch (Exception e) {
		// 	e.printStackTrace();
		// }
	}

}
