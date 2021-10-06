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




public  class TestMessageQueue {
	private static final Logger log = Logger.getLogger(Test1.class);
	public static Random rand = new Random();
	public static byte[] sampleData = new byte[17408];
	// public static String dbPath = "/mnt/nvme/mq";
	// public static String dbPath = "/mnt/ssd/mq";
	// TODO: just for test
	public static String dbPath = ".";

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
	public void testGetHelloWorld(){
		System.out.println("Helloworld");
	}

	@Test
	public void testSingleThreadAppendAndGetRange(){
		String topicName = "topic";
		Vector<Message> msgs = new Vector<>();
		for (long offset = 0; offset < 10; offset++) {
			for (int queueId = 0; queueId < 99; queueId++) {
				Message msg = new Message(topicName, queueId, offset);
				msgs.add(msg);
			}
		}
		MessageQueue mq = new Test1MessageQueueImpl(dbPath);

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
			if (result.get(0).compareTo(msg.checkBuf) != 0) {

				log.info("topic: " + msg.topic + " id: " + msg.queueId + " offset: " + msg.offset +" buffer: "  + result.get(0));
		
				byte[] tmp = msg.buf.array();
							log.info("***************real*************************");
							for(int ii=0;  ii < tmp.length; ++ii){
								System.out.print(tmp[ii] + " ");
							}
							log.info("***************end*************************");
				log.error("data error !");
				System.exit(0);
			}
		}

	}

	
}
