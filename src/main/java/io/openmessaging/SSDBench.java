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

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import java.util.concurrent.CyclicBarrier;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.nio.MappedByteBuffer;

import java.util.concurrent.BrokenBarrierException;
import java.io.IOException;

import io.openmessaging.UnsafeUtil;
import sun.nio.ch.DirectBuffer;


public class SSDBench {
    private static final Logger log = Logger.getLogger(SSDBench.class);
    public static AtomicLong writePosition = new AtomicLong(0);
    public static Lock benchLock = new ReentrantLock();
    public static ByteBuffer sampleHeapData = ByteBuffer.allocate(1024*1024);

    // FileChannel/Mapped
    // io size
    // direct/indirect
    // numOfFiles ( threads )

    public static void main(String[] args) {
        // log.setLevel(Level.DEBUG);
        log.setLevel(Level.INFO);
        if (args.length < 1) {
            System.out.println("java SSDBench ${dbPath}");
            return;
        }
        String dbPath = args[0];
        runOneBench(dbPath);
        // runBench(dbPath);
        // runBench1(dbPath);
        // testBench(dbPath);
    }

    public static void runBench(String dbPath) {
        benchLock.lock();
        System.out.println("dbPathDir : " + dbPath);
        // long totalBenchSize = 4L*1024L*1024L*1024L; // 4GiB
        // long totalBenchSize = 256L*1024L*1024L; //256MiB
        // long totalBenchSize = 64L*1024L*1024L; // 64MiB

        log.info("type,thread,ioSize,bandwidth,iops,latency(us)");

        // log.info("small io size in 16MiB");

        // {
        // long totalBenchSize = 16L*1024L*1024L; // 16MiB
        // int[] ioSizes = {64, 128, 256, 512, 1*1024, 2*1024, 4*1024, 8*1024};

        // for (int i = 0; i < ioSizes.length; i++) {
        // benchFileChannelWrite(dbPath, totalBenchSize, ioSizes[i], false);
        // benchFileChannelWrite(dbPath, totalBenchSize, ioSizes[i], true);
        // }
        // for (int i = 0; i < ioSizes.length; i++) {
        // benchMappedlWrite(dbPath, totalBenchSize, ioSizes[i], false);
        // benchMappedlWrite(dbPath, totalBenchSize, ioSizes[i], true);
        // }
        // // int[] numOfFiles = { 1, 2 };
        // int[] numOfFiles = {1,2,4,6,8,10,12,14,16};
        // // int[] numOfFiles = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16};

        // for (int i = 0; i < numOfFiles.length; i++) {
        // for (int j = 0; j < ioSizes.length; j++) {
        // benchFileChannelWriteMultiFile(dbPath, totalBenchSize, numOfFiles[i],
        // ioSizes[j], false);
        // benchFileChannelWriteMultiFile(dbPath, totalBenchSize, numOfFiles[i],
        // ioSizes[j], true);
        // }
        // }
        // for (int i = 0; i < numOfFiles.length; i++) {
        // for (int j = 0; j < ioSizes.length; j++) {
        // benchFileChannelWriteMappedMultiFile(dbPath, totalBenchSize, numOfFiles[i],
        // ioSizes[j],
        // false);
        // benchFileChannelWriteMappedMultiFile(dbPath, totalBenchSize, numOfFiles[i],
        // ioSizes[j],
        // true);
        // }
        // }
        // }

        // log.info("large io size in 1GiB");
        // {
        // long totalBenchSize = 1L * 1024L * 1024L * 1024L; // 1GiB
        // int[] ioSizes = { 4 * 1024, 8 * 1024, 16 * 1024, 32 * 1024, 64 * 1024, 128 *
        // 1024, 256 * 1024,
        // 512 * 1024, 1024 * 1024 };
        // for (int i = 0; i < ioSizes.length; i++) {
        // benchFileChannelWrite(dbPath, totalBenchSize, ioSizes[i], false);
        // benchFileChannelWrite(dbPath, totalBenchSize, ioSizes[i], true);
        // }
        // for (int i = 0; i < ioSizes.length; i++) {
        // benchMappedlWrite(dbPath, totalBenchSize, ioSizes[i], false);
        // benchMappedlWrite(dbPath, totalBenchSize, ioSizes[i], true);
        // }
        // // int[] numOfFiles = { 1, 2 };
        // // int[] numOfFiles = {1,2,3,4,5};
        // int[] numOfFiles = {1,2,4,6,8,10,12,14,16};
        // // int[] numOfFiles = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16};

        // for (int i = 0; i < numOfFiles.length; i++) {
        // for (int j = 0; j < ioSizes.length; j++) {
        // benchFileChannelWriteMultiFile(dbPath, totalBenchSize, numOfFiles[i],
        // ioSizes[j],
        // false);
        // benchFileChannelWriteMultiFile(dbPath, totalBenchSize, numOfFiles[i],
        // ioSizes[j], true);
        // }
        // }
        // for (int i = 0; i < numOfFiles.length; i++) {
        // for (int j = 0; j < ioSizes.length; j++) {
        // benchFileChannelWriteMappedMultiFile(dbPath, totalBenchSize, numOfFiles[i],
        // ioSizes[j],
        // false);
        // benchFileChannelWriteMappedMultiFile(dbPath, totalBenchSize, numOfFiles[i],
        // ioSizes[j],
        // true);
        // }
        // }
        // }

        log.info("test");
        {
            long totalBenchSize = 256L * 1024L * 1024L; // 1GiB
            int[] ioSizes = { 4 * 1024, 8 * 1024, 16 * 1024, 32 * 1024, 48 * 1024, 64 * 1024, 80 * 1024, 128 * 1024,
                    256 * 1024, 512 * 1024, 1024 * 1024 };
            // int[] ioSizes = { 64 * 1024};
            // for (int i = 0; i < ioSizes.length; i++) {
            // benchFileChannelWrite(dbPath, totalBenchSize, ioSizes[i], false);
            // benchFileChannelWrite(dbPath, totalBenchSize, ioSizes[i], true);
            // }
            // for (int i = 0; i < ioSizes.length; i++) {
            // benchMappedlWrite(dbPath, totalBenchSize, ioSizes[i], false);
            // benchMappedlWrite(dbPath, totalBenchSize, ioSizes[i], true);
            // }
            // int[] numOfFiles = { 1, 2 };
            // int[] numOfFiles = { 3 };
            // int[] numOfFiles = {1,2,3,4,5};
            int[] numOfFiles = { 1, 2, 3, 4, 5, 6, 8, 10, 12, 14, 16 };
            // int[] numOfFiles = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16};

            // for (int i = 0; i < numOfFiles.length; i++) {
            //     for (int j = 0; j < ioSizes.length; j++) {
            //         benchFileChannelWriteMultiFile(dbPath, totalBenchSize, numOfFiles[i], ioSizes[j], false);
            //         benchFileChannelWriteMultiFile(dbPath, totalBenchSize, numOfFiles[i], ioSizes[j], true);
            //         benchFileChannelWriteMappedMultiFile(dbPath, totalBenchSize, numOfFiles[i], ioSizes[j], false);
            //         benchFileChannelWriteMappedMultiFile(dbPath, totalBenchSize, numOfFiles[i], ioSizes[j], true);

            //     }
            // }
            // for (int i = 0; i < numOfFiles.length; i++) {
            // for (int j = 0; j < ioSizes.length; j++) {
            // }
            // }
        }
        benchLock.unlock();
    }
    public static void runStandardBench(String dbPath) {

        benchLock.lock();
        System.out.println("dbPathDir : " + dbPath);

        log.info("type,thread,ioSize,bandwidth,iops,latency(us)");

        log.info("test");
        {
            long totalBenchSize = 512L * 1024L * 1024L; // 1GiB
            int[] ioSizes = {48 * 1024, 52*1024, 56 * 1024, 60*1024, 64 * 1024};
            int[] numOfFiles = { 4 };

            for (int i = 0; i < numOfFiles.length; i++) {
                for (int j = 0; j < ioSizes.length; j++) {
                    benchFileChannelWriteMultiFile(dbPath, totalBenchSize, numOfFiles[i], ioSizes[j], false);
                    benchFileChannelWriteMultiFile(dbPath, totalBenchSize, numOfFiles[i], ioSizes[j], true);
                    // benchFileChannelWriteMappedMultiFileUnsafe(dbPath, totalBenchSize, numOfFiles[i], ioSizes[j]);
                    // benchFileChannelWriteMappedMultiFile(dbPath, totalBenchSize, numOfFiles[i],ioSizes[j], false);
                    // benchFileChannelWriteMappedMultiFile(dbPath, totalBenchSize, numOfFiles[i],ioSizes[j],true);

                }
            }
        }
        benchLock.unlock();
    }
    public static void runOneBench(String dbPath) {

        benchLock.lock();
        System.out.println("dbPathDir : " + dbPath);

        log.info("type,thread,ioSize,bandwidth,iops,latency(us)");

        log.info("test");
        {
            long totalBenchSize = 16*1024 * 1024L * 1024L; // 1GiB
            // long totalBenchSize = (1024+512) * 1024L * 1024L; // 1GiB
            // int[] ioSizes = {48 * 1024, 52*1024, 56 * 1024, 60*1024, 64 * 1024};
            int[] ioSizes = {64 * 1024};
            int[] numOfFiles = { 4 };

            for (int i = 0; i < numOfFiles.length; i++) {
                for (int j = 0; j < ioSizes.length; j++) {
                    // benchFileChannelWriteMultiFile(dbPath, totalBenchSize, numOfFiles[i], ioSizes[j], false);
                    benchFileChannelWriteMultiFile(dbPath, totalBenchSize, numOfFiles[i], ioSizes[j], true);
                    benchFileChannelWriteMultiFileOpt(dbPath, totalBenchSize, numOfFiles[i], ioSizes[j], true);
                    // benchFileChannelWriteMappedMultiFileUnsafe(dbPath, totalBenchSize, numOfFiles[i], ioSizes[j]);
                    // benchFileChannelWriteMappedMultiFile(dbPath, totalBenchSize, numOfFiles[i],ioSizes[j], false);
                    // benchFileChannelWriteMappedMultiFile(dbPath, totalBenchSize, numOfFiles[i],ioSizes[j],true);
                    // benchFileChannelWriteMappedMultiFileOpt(dbPath, totalBenchSize, numOfFiles[i],ioSizes[j],true);

                }
            }
        }
        benchLock.unlock();
    }



    public static void runBench1(String dbPath) {

        benchLock.lock();
        System.out.println("dbPathDir : " + dbPath);

        log.info("type,thread,ioSize,bandwidth,iops,latency(us)");

        log.info("test");
        {
            long totalBenchSize = 512L * 1024L * 1024L; // 1GiB
            int[] ioSizes = { 32 * 1024, 48 * 1024, 64 * 1024, 80 * 1024, 128 * 10244 };
            int[] numOfFiles = { 3, 4 };

            for (int i = 0; i < numOfFiles.length; i++) {
                for (int j = 0; j < ioSizes.length; j++) {
                    benchFileChannelWriteMultiFile(dbPath, totalBenchSize, numOfFiles[i], ioSizes[j], false);
                    benchFileChannelWriteMultiFile(dbPath, totalBenchSize, numOfFiles[i], ioSizes[j], true);
                    benchFileChannelWriteMappedMultiFileUnsafe(dbPath, totalBenchSize, numOfFiles[i], ioSizes[j]);
                    benchFileChannelWriteMappedMultiFile(dbPath, totalBenchSize, numOfFiles[i],ioSizes[j], false);
                    benchFileChannelWriteMappedMultiFile(dbPath, totalBenchSize, numOfFiles[i],ioSizes[j],true);

                }
            }
        }
        benchLock.unlock();
    }


    public static void testBench(String dbPath) {

        benchLock.lock();
        System.out.println("dbPathDir : " + dbPath);

        log.info("type,thread,ioSize,bandwidth,iops,latency(us)");

        log.info("test");
        {
            long totalBenchSize = 512L * 1024L * 1024L; // 1GiB
            int[] ioSizes = { 32 * 1024, 48 * 1024, 64 * 1024, 80 * 1024, 128 * 10244 };
            int[] numOfFiles = { 3,4 };

            for (int i = 0; i < numOfFiles.length; i++) {
                for (int j = 0; j < ioSizes.length; j++) {
                    // benchFileChannelWrite(dbPath, totalBenchSize, ioSizes[j], false);
                    // benchFileChannelWrite(dbPath, totalBenchSize, ioSizes[j], true);
                    // benchMappedlWriteUnsafe(dbPath, totalBenchSize, ioSizes[j]);
                    // benchMappedlWrite(dbPath, totalBenchSize, ioSizes[j], true);
                    benchFileChannelWriteMappedMultiFileUnsafe(dbPath, totalBenchSize, numOfFiles[i], ioSizes[j]);
                }
            }
        }
        benchLock.unlock();
    }

    public static void benchFileChannelWrite(String dbPath, long totalBenchSize, int ioSize, boolean isDirect) {
        try {
            dbPath = dbPath + "/ssdbench";
            log.debug("dbPath : " + dbPath);
            File db = new File(dbPath);
            FileChannel fileChannel = new RandomAccessFile(db, "rw").getChannel();

            int thread = 1;
            assert (totalBenchSize % ioSize == 0);
            long totalBenchCount = totalBenchSize / ioSize;
            totalBenchSize = totalBenchCount * ioSize;
            ByteBuffer buf;
            String type = "seqWrite";
            if (isDirect) {
                buf = ByteBuffer.allocateDirect(ioSize);
                type += "Direct";
            } else {
                buf = ByteBuffer.allocate(ioSize);
            }
            long curPosition = 0L;
            long maxPosition = totalBenchSize;
            long startTime = System.nanoTime();
            while (curPosition < maxPosition) {
                buf.position(0);
                fileChannel.write(buf, curPosition);
                fileChannel.force(true);
                curPosition += ioSize;
            }
            long elapsedTime = System.nanoTime() - startTime;
            double elapsedTimeS = (double) elapsedTime / (1000 * 1000 * 1000);
            double latency = (double) (elapsedTime / 1000) / totalBenchCount;
            double totalBenchSizeMiB = totalBenchSize / (1024 * 1024);
            double bandwidth = (totalBenchSizeMiB) / (elapsedTimeS);
            double iops = totalBenchCount / elapsedTimeS;
            String output = String.format("%s,%d,%d,%.3f,%.3f,%.3f", type, thread, ioSize, bandwidth, iops, latency);
            log.info(output);
            fileChannel.close();
            db.delete();
        } catch (IOException ie) {
            ie.printStackTrace();
        }

    }

    public static void benchMappedlWrite(String dbPath, long totalBenchSize, int ioSize, boolean isDirect) {
        try {
            dbPath = dbPath + "/ssdbench";
            log.debug("dbPath : " + dbPath);
            File db = new File(dbPath);
            FileChannel fileChannel = new RandomAccessFile(db, "rw").getChannel();

            int thread = 1;
            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, totalBenchSize);

            assert (totalBenchSize % ioSize == 0);
            long totalBenchCount = totalBenchSize / ioSize;
            totalBenchSize = totalBenchCount*ioSize;

            ByteBuffer buf;
            String type = "seqWriteMapped";
            if (isDirect) {
                buf = ByteBuffer.allocateDirect(ioSize);
                type += "Direct";
            } else {
                buf = ByteBuffer.allocate(ioSize);
            }

            long curPosition = 0L;
            long maxPosition = totalBenchSize;
            long startTime = System.nanoTime();
            while (curPosition < maxPosition) {
                buf.position(0);
                mappedByteBuffer.put(buf);
                mappedByteBuffer.force();
                curPosition += ioSize;
            }
            long elapsedTime = System.nanoTime() - startTime;
            double elapsedTimeS = (double) elapsedTime / (1000 * 1000 * 1000);
            double latency = (double) (elapsedTime / 1000) / totalBenchCount;
            double totalBenchSizeMiB = totalBenchSize / (1024 * 1024);
            double bandwidth = (totalBenchSizeMiB) / (elapsedTimeS);
            double iops = totalBenchCount / elapsedTimeS;
            String output = String.format("%s,%d,%d,%.3f,%.3f,%.3f", type, thread, ioSize, bandwidth, iops, latency);
            log.info(output);
            fileChannel.close();
            db.delete();
        } catch (IOException ie) {
            ie.printStackTrace();
        }

    }

    public static class ThreadStat {
        long startTime;
        long endTime;

        ThreadStat() {
            startTime = 0L;
            endTime = 0L;
        }
    }
    public static void benchFileChannelWriteMultiFile(String dbPath, long totalBenchSize, int thread, int ioSize,
            boolean isDirect) {
        CyclicBarrier barrier = new CyclicBarrier(thread);
        long totalBenchCount = totalBenchSize / ioSize;
        final long curTotalBenchSize = totalBenchSize - totalBenchSize % ioSize;
        totalBenchSize = curTotalBenchSize;

        ThreadStat[] stats = new ThreadStat[thread];
        for (int i = 0; i < stats.length; i++) {
            stats[i] = new ThreadStat();
        }
        String type = "seqWriteMultiFile";
        if (isDirect) {
            type += "Direct";
        }
        ExecutorService executor = Executors.newFixedThreadPool(thread);
        log.info("start");
        long startTime = System.nanoTime();
        for (int i = 0; i < thread; i++) {
            final int threadId = i;
            executor.execute(() -> {
                threadRunSeqWrite(stats[threadId], barrier, dbPath, threadId, curTotalBenchSize, ioSize, isDirect);
            });

        }
        executor.shutdown();

        try {
            // Wait a while for existing tasks to terminate
            while (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                System.out.println("Pool did not terminate, waiting ...");
            }
        } catch (InterruptedException ie) {
            executor.shutdownNow();
            ie.printStackTrace();
        }
        long endTime = System.nanoTime();
        log.info("end");
        long elapsedTime = stats[0].endTime - stats[0].startTime;
        double latency = (double) (elapsedTime / 1000) / totalBenchCount;
        double elapsedTimeS = (double) elapsedTime / (1000 * 1000 * 1000);
        double totalBenchSizeMiB = (double) totalBenchSize * thread / (1024 * 1024);
        double bandwidth = (totalBenchSizeMiB) / (elapsedTimeS);
        double iops = totalBenchCount * thread / elapsedTimeS;
        String output = String.format("%s,%d,%d,%.3f,%.3f,%.3f", type, thread, ioSize, bandwidth, iops, latency);
        log.info(output);
    }

    public static void threadRunSeqWrite(ThreadStat stat, CyclicBarrier barrier, String dbPath, int threadId,
            long totalBenchSize, int ioSize, boolean isDirect) {
        try {
            dbPath = dbPath + "/ssdbench" + threadId;
            File db = new File(dbPath);
            FileChannel fileChannel = new RandomAccessFile(db, "rw").getChannel();
            // fileChannel.truncate(16*1024*1024*1024);
            fileChannel.position(totalBenchSize);
            log.debug("dbPath : " + dbPath);

            assert (totalBenchSize % ioSize == 0);
            ByteBuffer buf;
            if (isDirect) {
                buf = ByteBuffer.allocateDirect(ioSize);
            } else {
                buf = ByteBuffer.allocate(ioSize);
            }
            log.debug("begin bench !!");
            long curPosition = 0L;
            long maxPosition = totalBenchSize;
            barrier.await();
            long startTime = System.nanoTime();
            while (curPosition < maxPosition) {
                buf.position(0);
                fileChannel.write(buf, curPosition);
                fileChannel.force(true);
                curPosition += ioSize;
            }
            long endTime = System.nanoTime();
            barrier.await();
            stat.startTime = startTime;
            stat.endTime = endTime;
            fileChannel.close();
            db.delete();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        } catch (IOException ie) {
            ie.printStackTrace();
        }
    }

    public static void benchFileChannelWriteMultiFileOpt(String dbPath, long totalBenchSize, int thread, int ioSize,
            boolean isDirect) {
        CyclicBarrier barrier = new CyclicBarrier(thread);
        long totalBenchCount = totalBenchSize / ioSize;
        final long curTotalBenchSize = totalBenchSize - totalBenchSize % ioSize;
        totalBenchSize = curTotalBenchSize;

        ThreadStat[] stats = new ThreadStat[thread];
        for (int i = 0; i < stats.length; i++) {
            stats[i] = new ThreadStat();
        }
        String type = "seqWriteMultiFileOpt";
        if (isDirect) {
            type += "Direct";
        }
        ExecutorService executor = Executors.newFixedThreadPool(thread);
        log.info("start");
        long startTime = System.nanoTime();
        for (int i = 0; i < thread; i++) {
            final int threadId = i;
            executor.execute(() -> {
                threadRunSeqWriteOpt(stats[threadId], barrier, dbPath, threadId, curTotalBenchSize, ioSize, isDirect);
            });

        }
        executor.shutdown();

        try {
            // Wait a while for existing tasks to terminate
            while (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                System.out.println("Pool did not terminate, waiting ...");
            }
        } catch (InterruptedException ie) {
            executor.shutdownNow();
            ie.printStackTrace();
        }
        long endTime = System.nanoTime();
        log.info("end");
        long elapsedTime = stats[0].endTime - stats[0].startTime;
        double latency = (double) (elapsedTime / 1000) / totalBenchCount;
        double elapsedTimeS = (double) elapsedTime / (1000 * 1000 * 1000);
        double totalBenchSizeMiB = (double) totalBenchSize * thread / (1024 * 1024);
        double bandwidth = (totalBenchSizeMiB) / (elapsedTimeS);
        double iops = totalBenchCount * thread / elapsedTimeS;
        String output = String.format("%s,%d,%d,%.3f,%.3f,%.3f", type, thread, ioSize, bandwidth, iops, latency);
        log.info(output);
    }

    public static void threadRunSeqWriteOpt(ThreadStat stat, CyclicBarrier barrier, String dbPath, int threadId,
            long totalBenchSize, int ioSize, boolean isDirect) {
        try {
            dbPath = dbPath + "/ssdbench" + threadId;
            File db = new File(dbPath);
            FileChannel fileChannel = new RandomAccessFile(db, "rw").getChannel();
            // fileChannel.truncate(16*1024*1024*1024);
            fileChannel.position(totalBenchSize);
            log.debug("dbPath : " + dbPath);

            assert (totalBenchSize % ioSize == 0);
            ByteBuffer buf;
            if (isDirect) {
                buf = ByteBuffer.allocateDirect(ioSize);
            } else {
                buf = ByteBuffer.allocate(ioSize);
            }
            fileChannel.position(totalBenchSize);
            fileChannel.write(buf);
            fileChannel.position(0);
            log.debug("begin bench !!");
            long curPosition = 0L;
            long maxPosition = totalBenchSize;
            barrier.await();
            long startTime = System.nanoTime();
            while (curPosition < maxPosition) {
                buf.position(0);
                fileChannel.write(buf, curPosition);
                fileChannel.force(true);
                curPosition += ioSize;
            }
            long endTime = System.nanoTime();
            barrier.await();
            stat.startTime = startTime;
            stat.endTime = endTime;
            fileChannel.close();
            db.delete();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        } catch (IOException ie) {
            ie.printStackTrace();
        }
    }
    public static void benchFileChannelWriteMappedMultiFileOpt(String dbPath, long totalBenchSize, int thread, int ioSize,
            boolean isDirect) {
        CyclicBarrier barrier = new CyclicBarrier(thread);
        long totalBenchCount = totalBenchSize / ioSize;
        final long curTotalBenchSize = totalBenchSize - totalBenchSize % ioSize;
        totalBenchSize = curTotalBenchSize;

        ThreadStat[] stats = new ThreadStat[thread];
        for (int i = 0; i < stats.length; i++) {
            stats[i] = new ThreadStat();
        }
        String type = "seqWriteMultiFileMapped";
        if (isDirect) {
            type += "Direct";
        }
        ExecutorService executor = Executors.newFixedThreadPool(thread);
        log.info("start");
        long startTime = System.nanoTime();
        for (int i = 0; i < thread; i++) {
            final int threadId = i;
            executor.execute(() -> {
                threadRunSeqWriteMapped(stats[threadId], barrier, dbPath, threadId, curTotalBenchSize, ioSize,
                        isDirect);
            });

        }
        executor.shutdown();

        try {
            // Wait a while for existing tasks to terminate
            while (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                System.out.println("Pool did not terminate, waiting ...");
            }
        } catch (InterruptedException ie) {
            executor.shutdownNow();
            ie.printStackTrace();
        }
        long endTime = System.nanoTime();
        log.info("end");
        long elapsedTime = stats[0].endTime - stats[0].startTime;
        double latency = (double) (elapsedTime / 1000) / totalBenchCount;
        double elapsedTimeS = (double) elapsedTime / (1000 * 1000 * 1000);
        double totalBenchSizeMiB = (double) totalBenchSize * thread / (1024 * 1024);
        double bandwidth = (totalBenchSizeMiB) / (elapsedTimeS);
        double iops = totalBenchCount * thread / elapsedTimeS;
        String output = String.format("%s,%d,%d,%.3f,%.3f,%.3f", type, thread, ioSize, bandwidth, iops, latency);
        log.info(output);
    }


    public static void threadRunSeqWriteMappedOpt(ThreadStat stat, CyclicBarrier barrier, String dbPath, int threadId,
            long totalBenchSize, int ioSize, boolean isDirect) {
        try {
            dbPath = dbPath + "/ssdbench" + threadId;
            File db = new File(dbPath);
            FileChannel fileChannel = new RandomAccessFile(db, "rw").getChannel();
            log.debug("dbPath : " + dbPath);
            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, totalBenchSize);
            for (int i = 0; i < totalBenchSize; i++){
                mappedByteBuffer.put((byte)0);
            }
            mappedByteBuffer.force();
            mappedByteBuffer.clear();

            assert (totalBenchSize % ioSize == 0);
            ByteBuffer buf;
            if (isDirect) {
                buf = ByteBuffer.allocateDirect(ioSize);
            } else {
                buf = ByteBuffer.allocate(ioSize);
            }
            log.debug("begin bench !!");
            long curPosition = 0L;
            long maxPosition = totalBenchSize;
            barrier.await();
            long startTime = System.nanoTime();
            while (curPosition < maxPosition) {
                buf.position(0);
                mappedByteBuffer.put(buf);
                mappedByteBuffer.force();
                curPosition += ioSize;
            }
            barrier.await();
            long endTime = System.nanoTime();
            stat.startTime = startTime;
            stat.endTime = endTime;
            fileChannel.close();
            db.delete();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        } catch (IOException ie) {
            ie.printStackTrace();
        }
    }

    public static void benchFileChannelWriteMappedMultiFile(String dbPath, long totalBenchSize, int thread, int ioSize,
            boolean isDirect) {
        CyclicBarrier barrier = new CyclicBarrier(thread);
        long totalBenchCount = totalBenchSize / ioSize;
        final long curTotalBenchSize = totalBenchSize - totalBenchSize % ioSize;
        totalBenchSize = curTotalBenchSize;

        ThreadStat[] stats = new ThreadStat[thread];
        for (int i = 0; i < stats.length; i++) {
            stats[i] = new ThreadStat();
        }
        String type = "seqWriteMultiFileMapped";
        if (isDirect) {
            type += "Direct";
        }
        ExecutorService executor = Executors.newFixedThreadPool(thread);
        log.info("start");
        long startTime = System.nanoTime();
        for (int i = 0; i < thread; i++) {
            final int threadId = i;
            executor.execute(() -> {
                threadRunSeqWriteMapped(stats[threadId], barrier, dbPath, threadId, curTotalBenchSize, ioSize,
                        isDirect);
            });

        }
        executor.shutdown();

        try {
            // Wait a while for existing tasks to terminate
            while (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                System.out.println("Pool did not terminate, waiting ...");
            }
        } catch (InterruptedException ie) {
            executor.shutdownNow();
            ie.printStackTrace();
        }
        long endTime = System.nanoTime();
        log.info("end");
        long elapsedTime = stats[0].endTime - stats[0].startTime;
        double latency = (double) (elapsedTime / 1000) / totalBenchCount;
        double elapsedTimeS = (double) elapsedTime / (1000 * 1000 * 1000);
        double totalBenchSizeMiB = (double) totalBenchSize * thread / (1024 * 1024);
        double bandwidth = (totalBenchSizeMiB) / (elapsedTimeS);
        double iops = totalBenchCount * thread / elapsedTimeS;
        String output = String.format("%s,%d,%d,%.3f,%.3f,%.3f", type, thread, ioSize, bandwidth, iops, latency);
        log.info(output);
    }

    public static void threadRunSeqWriteMapped(ThreadStat stat, CyclicBarrier barrier, String dbPath, int threadId,
            long totalBenchSize, int ioSize, boolean isDirect) {
        try {
            dbPath = dbPath + "/ssdbench" + threadId;
            File db = new File(dbPath);
            FileChannel fileChannel = new RandomAccessFile(db, "rw").getChannel();
            log.debug("dbPath : " + dbPath);
            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, totalBenchSize);

            assert (totalBenchSize % ioSize == 0);
            ByteBuffer buf;
            if (isDirect) {
                buf = ByteBuffer.allocateDirect(ioSize);
            } else {
                buf = ByteBuffer.allocate(ioSize);
            }
            log.debug("begin bench !!");
            long curPosition = 0L;
            long maxPosition = totalBenchSize;
            barrier.await();
            long startTime = System.nanoTime();
            while (curPosition < maxPosition) {
                buf.position(0);
                mappedByteBuffer.put(buf);
                mappedByteBuffer.force();
                curPosition += ioSize;
            }
            barrier.await();
            long endTime = System.nanoTime();
            stat.startTime = startTime;
            stat.endTime = endTime;
            fileChannel.close();
            db.delete();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        } catch (IOException ie) {
            ie.printStackTrace();
        }
    }

    @SuppressWarnings("restriction")
    public static void benchMappedlWriteUnsafeMMapper(String dbPath, long totalBenchSize, int ioSize) {
        try {
            dbPath = dbPath + "/ssdbench";
            log.debug("dbPath : " + dbPath);
            File db = new File(dbPath);

            int thread = 1;
            MMapper mmapper = new MMapper(dbPath, 1024L*1024L*1024L);

            assert (totalBenchSize % ioSize == 0);
            long totalBenchCount = totalBenchSize / ioSize;

            byte[] buf = new byte[ioSize];
            for (int i = 0; i < ioSize; i++){
                buf[i] = (byte)i;
            }

            String type = "seqWriteMappedUnsafeMMapper";

            long curPosition = 0;
            long maxPosition = totalBenchSize;
            long startTime = System.nanoTime();
            while (curPosition < maxPosition) {
                mmapper.setBytes(curPosition, buf);
                curPosition += ioSize;
            }
            long elapsedTime = System.nanoTime() - startTime;
            double elapsedTimeS = (double) elapsedTime / (1000 * 1000 * 1000);
            double latency = (double)(elapsedTime/1000)/(totalBenchCount);
            double totalBenchSizeMiB = totalBenchSize / (1024 * 1024);
            double bandwidth = (totalBenchSizeMiB) / (elapsedTimeS);
            double iops = totalBenchCount / elapsedTimeS;
            String output = String.format("%s,%d,%d,%.3f,%.3f,%.3f", type, thread, ioSize, bandwidth, iops,latency);
            log.info(output);
            db.delete();
        } catch (IOException ie) {
            ie.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    @SuppressWarnings("restriction")
    public static void benchMappedlWriteUnsafe(String dbPath, long totalBenchSize, int ioSize) {
        try {
            dbPath = dbPath + "/ssdbench";
            log.debug("dbPath : " + dbPath);
            File db = new File(dbPath);
            FileChannel fileChannel = new RandomAccessFile(db, "rw").getChannel();

            int thread = 1;
            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, totalBenchSize);
            long mappedBufferAddr = ((DirectBuffer)mappedByteBuffer).address();

            assert (totalBenchSize % ioSize == 0);
            long totalBenchCount = totalBenchSize / ioSize;
            totalBenchSize = totalBenchCount * ioSize;

            ByteBuffer buf;
            String type = "seqWriteMappedUnsafe";
            buf = ByteBuffer.allocateDirect(ioSize);

            long bufAddr = ((DirectBuffer)buf).address();

            long curPosition = 0L;
            long maxPosition = totalBenchSize;
            long startTime = System.nanoTime();
            while (curPosition < maxPosition) {
                UnsafeUtil.UNSAFE.copyMemory(bufAddr, mappedBufferAddr+curPosition, ioSize);
                mappedByteBuffer.force();
                curPosition += ioSize;
            }
            long elapsedTime = System.nanoTime() - startTime;
            double elapsedTimeS = (double) elapsedTime / (1000 * 1000 * 1000);
            double latency = (double)(elapsedTime/1000)/(totalBenchCount);
            double totalBenchSizeMiB = totalBenchSize / (1024 * 1024);
            double bandwidth = (totalBenchSizeMiB) / (elapsedTimeS);
            double iops = totalBenchCount / elapsedTimeS;
            String output = String.format("%s,%d,%d,%.3f,%.3f,%.3f", type, thread, ioSize, bandwidth, iops,latency);
            log.info(output);
            fileChannel.close();
            db.delete();
        } catch (IOException ie) {
            ie.printStackTrace();
        }

    }


    public static void benchFileChannelWriteMappedMultiFileUnsafe(String dbPath, long totalBenchSize, int thread, int ioSize) {
        CyclicBarrier barrier = new CyclicBarrier(thread);
        long totalBenchCount = totalBenchSize / ioSize;
        final long curTotalBenchSize = totalBenchCount * ioSize ;
        totalBenchSize = curTotalBenchSize;

        ThreadStat[] stats = new ThreadStat[thread];
        for (int i = 0; i < stats.length; i++) {
            stats[i] = new ThreadStat();
        }
        String type = "seqWriteMultiFileMappedUnsafe";
        ExecutorService executor = Executors.newFixedThreadPool(thread);
        long startTime = System.nanoTime();
        for (int i = 0; i < thread; i++) {
            final int threadId = i;
            executor.execute(() -> {
                threadRunSeqWriteMappedUnsafe(stats[threadId], barrier, dbPath, threadId, curTotalBenchSize, ioSize);
            });

        }
        executor.shutdown();

        try {
            // Wait a while for existing tasks to terminate
            while (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                System.out.println("Pool did not terminate, waiting ...");
            }
        } catch (InterruptedException ie) {
            executor.shutdownNow();
            ie.printStackTrace();
        }
        long endTime = System.nanoTime();
        long elapsedTime = stats[0].endTime - stats[0].startTime;
        double latency = (double) (elapsedTime / 1000) / totalBenchCount;
        double elapsedTimeS = (double) elapsedTime / (1000 * 1000 * 1000);
        double totalBenchSizeMiB = (double) totalBenchSize * thread / (1024 * 1024);
        double bandwidth = (totalBenchSizeMiB) / (elapsedTimeS);
        double iops = totalBenchCount * thread / elapsedTimeS;
        String output = String.format("%s,%d,%d,%.3f,%.3f,%.3f", type, thread, ioSize, bandwidth, iops, latency);
        log.info(output);
    }

    public static void threadRunSeqWriteMappedUnsafe(ThreadStat stat, CyclicBarrier barrier, String dbPath, int threadId,
            long totalBenchSize, int ioSize) {
        try {
            dbPath = dbPath + "/ssdbench" + threadId;
            File db = new File(dbPath);
            FileChannel fileChannel = new RandomAccessFile(db, "rw").getChannel();
            log.debug("dbPath : " + dbPath);
            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, totalBenchSize);

            assert (totalBenchSize % ioSize == 0);

            long mappedBufferAddr = ((DirectBuffer)mappedByteBuffer).address();
            ByteBuffer buf;
            buf = ByteBuffer.allocateDirect(ioSize);

            long bufAddr = ((DirectBuffer)buf).address();

            log.debug("begin bench !!");
            long curPosition = 0L;
            long maxPosition = totalBenchSize;
            barrier.await();
            long startTime = System.nanoTime();
            while (curPosition < maxPosition) {
                UnsafeUtil.UNSAFE.copyMemory(bufAddr, mappedBufferAddr+curPosition, ioSize);
                // UnsafeUtil.UNSAFE.copyMemory(sampleHeapData.array(), 16, null , mappedBufferAddr+curPosition, ioSize);
                mappedByteBuffer.force();
                curPosition += ioSize;
            }
            barrier.await();
            long endTime = System.nanoTime();
            stat.startTime = startTime;
            stat.endTime = endTime;
            fileChannel.close();
            db.delete();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        } catch (IOException ie) {
            ie.printStackTrace();
        }
    }

    // public static void benchFileChannelWriteThreadPool(FileChannel fileChannel,
    // long totalBenchSize, int thread ,int ioSize) throws IOException {
    // assert(totalBenchSize % ioSize == 0);
    // long totalBenchCount = totalBenchSize/ioSize;
    // byte[] data = new byte[ioSize];
    // long curPosition = 0L;
    // long maxPosition = totalBenchSize;
    // ExecutorService executor = Executors.newFixedThreadPool(thread);
    // long startTime = System.nanoTime();
    // while (curPosition < maxPosition){
    // final long curP = curPosition;
    // executor.execute(()->{
    // try {
    // mySyncWrite(fileChannel, data, curP);
    // } catch(IOException ie) {
    // ie.printStackTrace();
    // }

    // });
    // curPosition += ioSize;
    // }
    // executor.shutdown();

    // try {
    // // Wait a while for existing tasks to terminate
    // while(!executor.awaitTermination(60, TimeUnit.SECONDS)) {
    // System.out.println("Pool did not terminate, waiting ...");
    // }
    // } catch (InterruptedException ie) {
    // executor.shutdownNow();
    // ie.printStackTrace();
    // }
    // long elapsedTime = System.nanoTime() - startTime;
    // double elapsedTimeS = (double)elapsedTime/(1000*1000*1000);
    // double totalBenchSizeMiB = totalBenchSize/(1024*1024);
    // double bandwidth = (totalBenchSizeMiB)/(elapsedTimeS);
    // double iops = totalBenchCount/elapsedTimeS;
    // System.out.println("sequentialWriteThreadPool,"+thread+","+ioSize+","+bandwidth+","+iops);
    // }
    // public static synchronized void mySyncWrite(FileChannel fileChannel, byte[]
    // data, long curPosition) throws IOException {
    // fileChannel.write(ByteBuffer.wrap(data),curPosition);
    // fileChannel.force(true);
    // }

    // public static void benchFileChannelWriteThreadPoolAtomicPosition(FileChannel
    // fileChannel, long totalBenchSize, int thread ,int ioSize) throws IOException
    // {
    // assert(totalBenchSize % ioSize == 0);
    // long totalBenchCount = totalBenchSize/ioSize;
    // byte[] data = new byte[ioSize];
    // long curPosition = 0L;
    // long maxPosition = totalBenchSize;
    // ExecutorService executor = Executors.newFixedThreadPool(thread);
    // long startTime = System.nanoTime();
    // for (int i = 0; i < totalBenchCount; i++){
    // executor.execute(()->{
    // try {
    // mySyncWriteAtomicPosition(fileChannel, data, ioSize);
    // } catch(IOException ie) {
    // ie.printStackTrace();
    // }

    // });
    // }
    // executor.shutdown();

    // try {
    // // Wait a while for existing tasks to terminate
    // while(!executor.awaitTermination(60, TimeUnit.SECONDS)) {
    // System.out.println("Pool did not terminate, waiting ...");
    // }
    // } catch (InterruptedException ie) {
    // executor.shutdownNow();
    // ie.printStackTrace();
    // }
    // long elapsedTime = System.nanoTime() - startTime;
    // double elapsedTimeS = (double)elapsedTime/(1000*1000*1000);
    // double totalBenchSizeMiB = totalBenchSize/(1024*1024);
    // double bandwidth = (totalBenchSizeMiB)/(elapsedTimeS);
    // double iops = totalBenchCount/elapsedTimeS;
    // System.out.println("sequentialWriteThreadPoolAtomicPosition,"+thread+","+ioSize+","+bandwidth+","+iops);
    // }

    // public static synchronized void mySyncWriteAtomicPosition(FileChannel
    // fileChannel, byte[] data, int ioSize) throws IOException {
    // fileChannel.write(ByteBuffer.wrap(data),writePosition.getAndAdd(ioSize));
    // fileChannel.force(true);
    // }

    // public static void benchFileChannelWriteThreadPoolRange(FileChannel
    // fileChannel, long totalBenchSize, int thread ,int ioSize) throws IOException
    // {
    // benchLock.lock();
    // assert(totalBenchSize % ioSize == 0);
    // long totalBenchCount = totalBenchSize/ioSize;
    // long operationPerThread = totalBenchCount/thread;
    // totalBenchSize = operationPerThread*thread*ioSize;
    // totalBenchCount = operationPerThread*thread;
    // long curPosition = 0L;
    // long maxPosition = totalBenchSize;
    // ExecutorService executor = Executors.newFixedThreadPool(thread);
    // long startTime = System.nanoTime();
    // while (curPosition < maxPosition){
    // final long finalCurPosition = curPosition;
    // final long curMaxPosition = curPosition + operationPerThread*ioSize;

    // executor.execute(()->{
    // try {
    // byte[] data = new byte[ioSize];
    // myWriteRange(fileChannel, data, finalCurPosition, curMaxPosition, ioSize);
    // } catch(IOException ie) {
    // ie.printStackTrace();
    // }

    // });
    // curPosition = curMaxPosition;
    // }
    // executor.shutdown();

    // try {
    // // Wait a while for existing tasks to terminate
    // while(!executor.awaitTermination(60, TimeUnit.SECONDS)) {
    // System.out.println("Pool did not terminate, waiting ...");
    // }
    // } catch (InterruptedException ie) {
    // executor.shutdownNow();
    // ie.printStackTrace();
    // }
    // long elapsedTime = System.nanoTime() - startTime;
    // double elapsedTimeS = (double)elapsedTime/(1000*1000*1000);
    // double totalBenchSizeMiB = (double)totalBenchSize/(1024*1024);
    // double bandwidth = (totalBenchSizeMiB)/(elapsedTimeS);
    // double iops = totalBenchCount/elapsedTimeS;
    // System.out.println("sequentialWriteThreadPoolRange,"+thread+","+ioSize+","+bandwidth+","+iops);
    // benchLock.unlock();
    // }
    // public static void myWriteRange(FileChannel fileChannel, byte[] data, long
    // minPosition, long maxPosition, int ioSize) throws IOException {
    // long curPosition = minPosition;
    // while (curPosition < maxPosition){
    // fileChannel.write(ByteBuffer.wrap(data),curPosition);
    // fileChannel.force(true);
    // curPosition += ioSize;
    // }
    // }

    // public static void benchFileChannelRead(FileChannel fileChannel, long
    // totalBenchSize ,int ioSize) throws IOException {
    // int thread = 1;
    // // System.out.println("Test Sequential Read Bandwidth and IOPS");
    // assert(totalBenchSize % ioSize == 0);
    // long totalBenchCount = totalBenchSize/ioSize;
    // ByteBuffer buffer = ByteBuffer.allocate(4096);
    // long curPosition = 0L;
    // long maxPosition = totalBenchSize;
    // long startTime = System.nanoTime();
    // while (curPosition < maxPosition){
    // // // 指定 position 读取 4kb 的数据
    // fileChannel.read(buffer,curPosition);
    // curPosition += ioSize;
    // }
    // long elapsedTime = System.nanoTime() - startTime;
    // double elapsedTimeS = (double)elapsedTime/(1000*1000*1000);
    // double totalBenchSizeMiB = totalBenchSize/(1024*1024);
    // double bandwidth = (totalBenchSizeMiB)/(elapsedTimeS) ;
    // double iops = totalBenchCount/elapsedTimeS;
    // System.out.println("sequentialRead,"+thread+","+ioSize+","+bandwidth+","+iops);
    // }

}
