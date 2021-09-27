import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.io.RandomAccessFile;
import java.io.File;
import java.io.IOException;

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
			// long totalBenchSize = 1L*1024L*1024L*1024L; // 1GiB
			long totalBenchSize = 64L*1024L*1024L; // 64MiB
			benchFileChannelWrite(fileChannel, totalBenchSize, 1, 64*1024); // ioSize = 64KiB
			benchFileChannelRead(fileChannel, totalBenchSize, 1, 64*1024); // ioSize = 64KiB
		} catch(IOException ie) {
			ie.printStackTrace();
		}  
	}

	public static void benchFileChannelWrite(FileChannel fileChannel, long totalBenchSize ,int thread, int ioSize) throws IOException {
		// thread = 1
		System.out.println("Test Sequential Write Bandwidth and IOPS");
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
			// System.out.println(curPosition);
		}
		long elapsedTime = System.nanoTime() - startTime;
		double elapsedTimeS = (double)elapsedTime/(1000*1000*1000);
		double totalBenchSizeMiB = totalBenchSize/(1024*1024);
		// System.out.println(elapsedTime);
		// System.out.println(totalBenchSize);
		System.out.println("Bandwidth : " + (totalBenchSizeMiB)/(elapsedTimeS) + " MiB/s");
		System.out.println("IOPS : " + totalBenchCount/elapsedTimeS + " op/s");
	}

	public static void benchFileChannelRead(FileChannel fileChannel, long totalBenchSize ,int thread, int ioSize) throws IOException {
		// thread = 1
		System.out.println("Test Sequential Read Bandwidth and IOPS");
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
		// System.out.println(elapsedTime);
		// System.out.println(totalBenchSize);
		System.out.println("Bandwidth : " + (totalBenchSizeMiB)/(elapsedTimeS) + " MiB/s");
		System.out.println("IOPS : " + totalBenchCount/elapsedTimeS + " op/s");
	}



	
}
