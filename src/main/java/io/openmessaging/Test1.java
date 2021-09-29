package io.openmessaging;

import io.openmessaging.MessageQueue;
import io.openmessaging.DefaultMessageQueueImpl;

import java.nio.ByteBuffer;
import  java.util.HashMap;
import java.util.Map;
import java.lang.Integer;
import java.util.Arrays;

public class Test1 {
	public static void main(String []args) {
		Test1MessageQueue mq = new Test1MessageQueue("/mnt/nvme/mq");
		int ioSize = 18;
		byte[] data = new byte[ioSize];
		for (int i = 0; i < ioSize; i++){
			data[i] = (byte)(i+96);
		}
		System.out.println(Arrays.toString(data));

		for (int i = 0; i < 50; i++){
			for (int j = 0; j < 10; i++){
				mq.append("topic",i , ByteBuffer.wrap(data));
			}
		}
		// Map<Integer, ByteBuffer> ret = mq.getRange("topic", 1324124, 0, 3);
		// System.out.println(ret);
		// System.out.println(ret.get(0));
		// System.out.println(Arrays.toString(ret.get(0).array()));
		// System.out.println(Arrays.toString(ret.get(1).array()));
		// byte[] getdata = ret.get(0).array();
		// for (int i = 0; i < getdata.length; i++){
		// 	System.out.println(getdata[i]);
		// }


		
		// for (int i = 0; i < getdata.length; i++){
		// }

		// MessageQueue mq = new DefaultMessageQueueImpl();
		// int ioSize = 4096;
		// byte[] data = new byte[ioSize];
		// for (int i = 0; i < ioSize; i++){
		// 	data[i] = (byte)i;
		// }
		// mq.append("topic", 1324124, ByteBuffer.wrap(data));
		// mq.append("topic", 1324124, ByteBuffer.wrap(data));
		// mq.append("topic", 1324124, ByteBuffer.wrap(data));
		// mq.append("topic", 1324124, ByteBuffer.wrap(data));
		// Map<Integer, ByteBuffer> ret = mq.getRange("topic", 1324124, 0, 1);
	}
}
