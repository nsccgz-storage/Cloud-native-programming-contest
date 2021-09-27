import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.io.RandomAccessFile;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SSDBench {
	public static void main(String []args) {
		try {
			if (args.length < 1){
				System.out.println("java SSDBench ${dbPath}");
				return ;
			}
			System.out.println("dbPath : " + args[0]);
			String dbPath = args[0] ;
			FileChannel fileChannel = new RandomAccessFile(new File(dbPath), "rw").getChannel();
			long totalBenchSize = 1L*1024L*1024L*1024L; // 1GiB
			// long totalBenchSize = 64L*1024L*1024L; // 64MiB

			System.out.println("type,thread,ioSize,bandwidth,iops");
			int[] ioSizes = {4*1024, 8*1024, 16*1024, 32*1024, 64*1024, 128*1024, 256*1024, 512*1024,1024*1024};
			// for (int i = 0; i < ioSizes.length; i++){
			// 	benchFileChannelWrite(fileChannel, totalBenchSize, ioSizes[i]); // ioSize = 64KiB
			// }

			benchFileChannelWriteThreadPool(fileChannel, totalBenchSize, 4, 64*1024);

			// benchFileChannelWrite(fileChannel, totalBenchSize, 1, 64*1024); // ioSize = 64KiB
			// benchFileChannelRead(fileChannel, totalBenchSize, 1, 64*1024); // ioSize = 64KiB
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
		  while(!executor.awaitTermination(2, TimeUnit.SECONDS)) {
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
		System.out.println("sequentialWrite,"+thread+","+ioSize+","+bandwidth+","+iops);
	}
	public static synchronized void mySyncWrite(FileChannel fileChannel, byte[] data, long curPosition) throws IOException {
		fileChannel.write(ByteBuffer.wrap(data),curPosition);
		// fileChannel.force(true);
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
