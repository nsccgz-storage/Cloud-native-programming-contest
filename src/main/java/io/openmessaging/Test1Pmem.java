package io.openmessaging;

import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryBlock;

public class Test1Pmem {
	public static void main(String[] args) {
		// if (args.length < 1){
		// 	System.out.println("java SSDBench ${dbPath}");
		// 	return ;
		// }
		// System.out.println("dbPath : " + args[0]);
		String pmPath = "/mnt/pmem/mq/test";
		long heapSize = 100 * 1024 * 1024;
		Heap heap = Heap.createHeap(pmPath, heapSize);	
		MemoryBlock block1 = heap.allocateMemoryBlock(1024, false);
		block1.setLong(0, 1234);
		block1.flush(0, Long.BYTES); // flush data to persistent memory modules
		String s = "Saturn";
		block1.setInt(0, s.length());
		block1.copyFromArray(s.getBytes(), 0, 4, s.length());
		block1.flush(0, Integer.BYTES + s.length()); // flush both writes		
		byte[] buf = new byte[100];
		block1.copyToArray(4, buf, 0, s.length());
		String s2 = new String(buf);
		System.out.println(buf);
		System.out.println(s2);
		System.out.println(s2.length());

	}
	
}
