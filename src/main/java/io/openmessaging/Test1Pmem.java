package io.openmessaging;

import java.nio.ByteBuffer;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryBlock;

public class Test1Pmem {
    	private static final Logger log = Logger.getLogger(Test1MessageQueue.class);
	public static class PMHeap{
		String pmPath;
		Heap h;
		int numOfBlocks;
		MemoryBlock[] pmBlocks;
		long curPosition;
		PMHeap(String path){
			pmPath = path;
			h = Heap.createHeap(pmPath, 60L*1024L*1024L*1024L);
			numOfBlocks = 1;
			pmBlocks = new MemoryBlock[numOfBlocks];
			for (int i = 0; i < numOfBlocks; i++){
				pmBlocks[i] = h.allocateMemoryBlock(1024L*1024L*1024L, false);
				pmBlocks[i].setMemory((byte)0, 0L, 1024L*1024L*1024L);
			}
			curPosition = 0L;
		}

		public long addData(int blockId, ByteBuffer data){ 
			MemoryBlock block = pmBlocks[blockId];
			long retAddr = curPosition;
			long addr = curPosition;
			int dataLength = data.remaining();
			block.setInt(addr, dataLength);
			log.debug(addr);
			log.debug(dataLength);
			log.debug(data.array().length);
			addr += Integer.BYTES;
			block.copyFromArray(data.array(), 0, addr, dataLength);
			// for (int i = 0; i < dataLength; i++){
			// 	block.setByte(addr+i, data.array()[i]);
			// }

			return retAddr;
		}
		public ByteBuffer getData(int blockId, long addr){
			MemoryBlock block = pmBlocks[blockId];
			int dataLength = block.getInt(addr);
			log.debug(dataLength);
			ByteBuffer data = ByteBuffer.allocate(dataLength);
			addr += Integer.BYTES;
			// for (int i = 0; i < dataLength; i++){
			// 	data.put(block.getByte(addr+i));
			// }
			block.copyToArray(addr, data.array(), 0, dataLength);
			return data;
		}
	}
	public static void main(String[] args) {
        	log.setLevel(Level.DEBUG);
        	// log.setLevel(Level.INFO);
		// if (args.length < 1){
		// 	System.out.println("java SSDBench ${dbPath}");
		// 	return ;
		// }
		// System.out.println("dbPath : " + args[0]);
		String pmPath = "/mnt/pmem/mq/test";


		PMHeap h = new PMHeap(pmPath);
		ByteBuffer buf = ByteBuffer.allocate(20);
		for (int i = 0; i < 5; i++){
			buf.putInt(i);
		}
		log.debug(buf);
		buf.position(0);
		log.debug(buf);
		long addr = h.addData(0,buf);
		log.debug("addr : " + addr);
		ByteBuffer newBuf = h.getData(0, addr);
		log.debug(newBuf);
		newBuf.position(0);
		log.debug(newBuf);

		for (int i = 0; i < 5; i++){
			log.debug(newBuf.getInt());
		}


		// long heapSize = 100 * 1024 * 1024;
		// Heap heap = Heap.createHeap(pmPath, heapSize);	
		// MemoryBlock block1 = heap.allocateMemoryBlock(1024, false);
		// block1.setLong(0, 1234);
		// block1.flush(0, Long.BYTES); // flush data to persistent memory modules
		// String s = "Saturn";
		// block1.setInt(0, s.length());
		// block1.copyFromArray(s.getBytes(), 0, 4, s.length());
		// block1.flush(0, Integer.BYTES + s.length()); // flush both writes		
		// byte[] buf = new byte[100];
		// block1.copyToArray(4, buf, 0, s.length());
		// String s2 = new String(buf);
		// System.out.println(buf);
		// System.out.println(s2);
		// System.out.println(s2.length());

	}
	
}
