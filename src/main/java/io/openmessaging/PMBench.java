package io.openmessaging;

import java.nio.ByteBuffer;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryBlock;


public class PMBench {

    	private static final Logger log = Logger.getLogger(PMBench.class);
	public static void main(String[] args) {
		benchWrite("/mnt/pmem/mq/data");
		
	}

	public static void runStandardBench(String pmDirPath){
		benchWrite(pmDirPath);
	}

	public static void benchWrite(String pmDirPath){
		long heapSize = 60L*1024L*1024L*1024L;
		String pmDataPath = pmDirPath+"/data";
		Heap h = Heap.createHeap(pmDataPath, heapSize);
		long benchBlockSize = 1*1024L*1024L*1024L;
		MemoryBlock block = h.allocateMemoryBlock(benchBlockSize);

		String type = "seqWrite";
		int thread = 1;

		long curPosition = 0L;
		int ioSize = 4096;
		long benchCount = benchBlockSize/ioSize;

		byte[] sampleData = new byte[ioSize];
		for (int i = 0; i < ioSize; i++){
			sampleData[i] = (byte)i;
		}
		long startTime = System.nanoTime();
		while (curPosition < benchBlockSize){
			block.copyFromArray(sampleData, 0, curPosition, ioSize);
			curPosition += ioSize;
		}
		long endTime = System.nanoTime();
		long elapsedTime = endTime - startTime;
		double elapsedTimeS = (double)elapsedTime/(1000*1000*1000);

		double bandwidth = ((double)benchBlockSize/elapsedTimeS)/(1024*1024);
		double iops = (double)benchCount/elapsedTimeS;
		double latency = (double)elapsedTimeS/benchCount;


	        String output = String.format("%s,%d,%d,%.3f,%.3f,%.3f", type, thread, ioSize, bandwidth, iops, latency);
		log.info(output);

	}
	
}
