import java.nio.channels.FileChannel   ;
import java.nio.ByteBuffer;
import java.io.RandomAccessFile;
import java.io.File;
import java.io.IOException;

public class SSDBench {
	public static void main(String []args) {
		try {
			int ret;
			System.out.println("Hello World");
			String dbPath = "/Users/wyf/0code/nsccgz-storage/Cloud-native-programming-contest/testDB/db.data";
			FileChannel fileChannel = new RandomAccessFile(new File(dbPath), "rw").getChannel();
			byte[] data = new byte[4096];
			long position = 1024L;
			// 指定 position 写入 4kb 的数据
			fileChannel.write(ByteBuffer.wrap(data), position);
			// 从当前文件指针的位置写入 4kb 的数据
			fileChannel.write(ByteBuffer.wrap(data));

			// 读
			ByteBuffer buffer = ByteBuffer.allocate(4096);
			position = 1024L;
			// 指定 position 读取 4kb 的数据
			ret = fileChannel.read(buffer,position);
			System.out.println(ret);
			// 从当前文件指针的位置读取 4kb 的数据
			ret = fileChannel.read(buffer);
			System.out.println(ret);
		} catch(IOException ie) {
			ie.printStackTrace();
		}  
	}
	
}
