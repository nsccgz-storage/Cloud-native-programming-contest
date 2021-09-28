package io.openmessaging;

import io.openmessaging.MessageQueue;
import io.openmessaging.DefaultMessageQueueImpl;

import java.nio.ByteBuffer;
import  java.util.HashMap;
import java.util.Map;
import java.lang.Integer;

public class Test1 {
	public static void main(String []args) {
		MessageQueue mq = new DefaultMessageQueueImpl();
		int ioSize = 4096;
		byte[] data = new byte[ioSize];
		for (int i = 0; i < ioSize; i++){
			data[i] = (byte)i;
		}
		mq.append("topic", 1324124, ByteBuffer.wrap(data));
		mq.append("topic", 1324124, ByteBuffer.wrap(data));
		mq.append("topic", 1324124, ByteBuffer.wrap(data));
		mq.append("topic", 1324124, ByteBuffer.wrap(data));
		Map<Integer, ByteBuffer> ret = mq.getRange("topic", 1324124, 0, 1);
	}
}
