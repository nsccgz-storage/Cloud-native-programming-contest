package io.openmessaging;

import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.io.RandomAccessFile;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class SSDBench {
	public static AtomicLong writePosition = new AtomicLong(0);
	// public static long writePosition = 0;
	public static void main(String []args) {
		try {
			if (args.length < 1){
				System.out.println("java SSDBench ${dbPath}");
				return ;
			}
			System.out.println("dbPath : " + args[0]);
			String dbPath = args[0] ;
			FileChannel fileChannel = new RandomAccessFile(new File(dbPath), "rw").getChannel();
			long totalBenchSize = 4L*1024L*1024L*1024L; // 4GiB
			// long totalBenchSize = 1L*1024L*1024L*1024L; // 1GiB
			// long totalBenchSize = 256L*1024L*1024L; //256MiB 
			// long totalBenchSize = 64L*1024L*1024L; // 64MiB

			System.out.println("type,thread,ioSize,bandwidth,iops");
			int[] ioSizes = {4*1024, 8*1024, 16*1024, 32*1024, 64*1024, 128*1024, 256*1024, 512*1024,1024*1024};
			for (int i = 0; i < ioSizes.length; i++){
				benchFileChannelWrite(fileChannel, totalBenchSize, ioSizes[i]); // ioSize = 64KiB
			}

			for (int t = 1; t <= 50; t+=1){
				for (int i = 0; i < ioSizes.length; i++){
					benchFileChannelWriteThreadPool(fileChannel, totalBenchSize, t, ioSizes[i]);
				}
			}


			for (int t = 1; t <= 50; t+=1){
				for (int i = 0; i < ioSizes.length; i++){
					benchFileChannelWriteThreadPoolRange(fileChannel, totalBenchSize, t, ioSizes[i]);
				}
			}



			// benchFileChannelWrite(fileChannel, totalBenchSize, 64*1024); // ioSize = 64KiB
			// benchFileChannelWriteThreadPool(fileChannel, totalBenchSize, 16, 64*1024);

			// benchFileChannelWriteThreadPoolAtomicPosition(fileChannel, totalBenchSize, 1, 64*1024);
			// benchFileChannelWriteThreadPoolAtomicPosition(fileChannel, totalBenchSize, 16, 64*1024);

			// benchFileChannelWriteThreadPoolRange(fileChannel, totalBenchSize, 1, 64*1024);
			// benchFileChannelWriteThreadPoolRange(fileChannel, totalBenchSize, 16, 64*1024);


		} catch(IOException ie) {
			ie.printStackTrace();
		}  
	}

	public static void benchFileChannelWrite(FileChannel fileChannel, long totalBenchSize ,int ioSize) throws IOException {
		int thread = 1;
		assert(totalBenchSize % ioSize == 0);
		long totalBenchCount = totalBenchSize/ioSize;
		byte[] data = new byte[ioSize];
		long curPosition = 0L;
		long maxPosition = totalBenchSize;
		long startTime = System.nanoTime();    
		while (curPosition < maxPosition){
			fileChannel.write(ByteBuffer.wrap(data), curPosition);
			// fileChannel.force(false);
			fileChannel.force(true);
			curPosition += ioSize;
		}
		long elapsedTime = System.nanoTime() - startTime;
		double elapsedTimeS = (double)elapsedTime/(1000*1000*1000);
		double totalBenchSizeMiB = totalBenchSize/(1024*1024);
		double bandwidth =  (totalBenchSizeMiB)/(elapsedTimeS);
		double iops = totalBenchCount/elapsedTimeS;
		System.out.println("sequentialWrite,"+thread+","+ioSize+","+bandwidth+","+iops);
	}


	public static void benchFileChannelWriteThreadPool(FileChannel fileChannel, long totalBenchSize, int thread ,int ioSize) throws IOException {
		assert(totalBenchSize % ioSize == 0);
		long totalBenchCount = totalBenchSize/ioSize;
		byte[] data = new byte[ioSize];
		long curPosition = 0L;
		long maxPosition = totalBenchSize;
		ExecutorService executor = Executors.newFixedThreadPool(thread);
		long startTime = System.nanoTime();    
		while (curPosition < maxPosition){
			final long curP = curPosition;
			executor.execute(()->{
				try {
					mySyncWrite(fileChannel, data, curP);
				} catch(IOException ie) {
					ie.printStackTrace();
				}  

			});
			curPosition += ioSize;
		}
		executor.shutdown();

		try {
		  // Wait a while for existing tasks to terminate
		  while(!executor.awaitTermination(60, TimeUnit.SECONDS)) {
			System.out.println("Pool did not terminate, waiting ...");
		  }
		} catch (InterruptedException ie) {
			executor.shutdownNow();
			ie.printStackTrace();
		}
		long elapsedTime = System.nanoTime() - startTime;
		double elapsedTimeS = (double)elapsedTime/(1000*1000*1000);
		double totalBenchSizeMiB = totalBenchSize/(1024*1024);
		double bandwidth =  (totalBenchSizeMiB)/(elapsedTimeS);
		double iops = totalBenchCount/elapsedTimeS;
		System.out.println("sequentialWriteThreadPool,"+thread+","+ioSize+","+bandwidth+","+iops);
	}
	public static synchronized void mySyncWrite(FileChannel fileChannel, byte[] data, long curPosition) throws IOException {
		fileChannel.write(ByteBuffer.wrap(data),curPosition);
		fileChannel.force(true);
	}


	public static void benchFileChannelWriteThreadPoolAtomicPosition(FileChannel fileChannel, long totalBenchSize, int thread ,int ioSize) throws IOException {
		assert(totalBenchSize % ioSize == 0);
		long totalBenchCount = totalBenchSize/ioSize;
		byte[] data = new byte[ioSize];
		long curPosition = 0L;
		long maxPosition = totalBenchSize;
		ExecutorService executor = Executors.newFixedThreadPool(thread);
		long startTime = System.nanoTime();    
		for (int i = 0; i < totalBenchCount; i++){
			executor.execute(()->{
				try {
					mySyncWriteAtomicPosition(fileChannel, data, ioSize);
				} catch(IOException ie) {
					ie.printStackTrace();
				}  

			});
		}
		executor.shutdown();

		try {
		  // Wait a while for existing tasks to terminate
		  while(!executor.awaitTermination(60, TimeUnit.SECONDS)) {
			System.out.println("Pool did not terminate, waiting ...");
		  }
		} catch (InterruptedException ie) {
			executor.shutdownNow();
			ie.printStackTrace();
		}
		long elapsedTime = System.nanoTime() - startTime;
		double elapsedTimeS = (double)elapsedTime/(1000*1000*1000);
		double totalBenchSizeMiB = totalBenchSize/(1024*1024);
		double bandwidth =  (totalBenchSizeMiB)/(elapsedTimeS);
		double iops = totalBenchCount/elapsedTimeS;
		System.out.println("sequentialWriteThreadPoolAtomicPosition,"+thread+","+ioSize+","+bandwidth+","+iops);
	}

	public static synchronized void mySyncWriteAtomicPosition(FileChannel fileChannel, byte[] data, int ioSize) throws IOException {
		fileChannel.write(ByteBuffer.wrap(data),writePosition.getAndAdd(ioSize));
		fileChannel.force(true);
	}


	public static void benchFileChannelWriteThreadPoolRange(FileChannel fileChannel, long totalBenchSize, int thread ,int ioSize) throws IOException {
		assert(totalBenchSize % ioSize == 0);
		long totalBenchCount = totalBenchSize/ioSize;
		long operationPerThread = totalBenchCount/thread;
		totalBenchSize = operationPerThread*thread*ioSize;
		totalBenchCount = operationPerThread*thread;
		long curPosition = 0L;
		long maxPosition = totalBenchSize;
		ExecutorService executor = Executors.newFixedThreadPool(thread);
		long startTime = System.nanoTime();    
		while (curPosition < maxPosition){
			final long finalCurPosition = curPosition;
			final long curMaxPosition = curPosition + operationPerThread*ioSize;

			executor.execute(()->{
				try {
					byte[] data = new byte[ioSize];
					myWriteRange(fileChannel, data, finalCurPosition, curMaxPosition, ioSize);
				} catch(IOException ie) {
					ie.printStackTrace();
				}  

			});
			curPosition = curMaxPosition;
		}
		executor.shutdown();

		try {
		  // Wait a while for existing tasks to terminate
		  while(!executor.awaitTermination(60, TimeUnit.SECONDS)) {
			System.out.println("Pool did not terminate, waiting ...");
		  }
		} catch (InterruptedException ie) {
			executor.shutdownNow();
			ie.printStackTrace();
		}
		long elapsedTime = System.nanoTime() - startTime;
		double elapsedTimeS = (double)elapsedTime/(1000*1000*1000);
		double totalBenchSizeMiB = (double)totalBenchSize/(1024*1024);
		double bandwidth =  (totalBenchSizeMiB)/(elapsedTimeS);
		double iops = totalBenchCount/elapsedTimeS;
		System.out.println("sequentialWriteThreadPoolRange,"+thread+","+ioSize+","+bandwidth+","+iops);
	}
	public static void myWriteRange(FileChannel fileChannel, byte[] data, long minPosition, long maxPosition, int ioSize) throws IOException {
		long curPosition = minPosition;
		while (curPosition < maxPosition){
			fileChannel.write(ByteBuffer.wrap(data),curPosition);
			fileChannel.force(true);
			curPosition += ioSize;
		}
	}


	public static void benchFileChannelRead(FileChannel fileChannel, long totalBenchSize ,int ioSize) throws IOException {
		int thread = 1;
		// System.out.println("Test Sequential Read Bandwidth and IOPS");
		assert(totalBenchSize % ioSize == 0);
		long totalBenchCount = totalBenchSize/ioSize;
		ByteBuffer buffer = ByteBuffer.allocate(4096);
		long curPosition = 0L;
		long maxPosition = totalBenchSize;
		long startTime = System.nanoTime();    
		while (curPosition < maxPosition){
			// // 指定 position 读取 4kb 的数据
			fileChannel.read(buffer,curPosition);
			curPosition += ioSize;
		}
		long elapsedTime = System.nanoTime() - startTime;
		double elapsedTimeS = (double)elapsedTime/(1000*1000*1000);
		double totalBenchSizeMiB = totalBenchSize/(1024*1024);
		double bandwidth =  (totalBenchSizeMiB)/(elapsedTimeS) ;
		double iops = totalBenchCount/elapsedTimeS;
		System.out.println("sequentialRead,"+thread+","+ioSize+","+bandwidth+","+iops);
	}



	
}