package io.openmessaging;

import com.intel.pmem.llpl.CompactMemoryBlock;
import com.intel.pmem.llpl.Heap;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PmemManager {
    final long MAX_PMEM_SIZE = 60*1024L*1024L*1024L; // 60GB
    final long BLOCK_SIZE = 128 * 1024L * 1024L; // 暂定每个队列分配128KB的大小
    int topicNum = 100;
    int queueNumPerTopic = 500;

    Heap heap;
    ArrayList<ArrayList<MyBlock>> blockList;

    ExecutorService executorService = Executors.newFixedThreadPool(4);
    AsynchronousFileChannel asyncFc;

    private class MyBlock{
        long head, tail;
        CompactMemoryBlock block;

        // head == tail empty
        // tail + 1 == head full
        public MyBlock(CompactMemoryBlock b){
            head = 0;
            tail = 0;
            block = b;
        }

        public long getRemainSize(){
            return (head - tail - 1 + BLOCK_SIZE)%BLOCK_SIZE;
        }

        public int put(byte[] data){
            if(data.length > getRemainSize())return -1; // block 空间已满

            int position = (int) tail;
            if(BLOCK_SIZE - tail >= data.length) {
                block.copyFromArray(data, 0, tail, data.length);
                tail += data.length;
            }else{
                int tmp = (int)(BLOCK_SIZE - tail);
                block.copyFromArray(data, 0, tail, tmp);
                block.copyFromArray(data, tmp, 0, data.length-tmp);
                tail = data.length - tmp;
            }
            return position;
        }

        /*
        * 目前只检查 offset，不检查 length
        * */
        public byte[] get(long offset, int length){
            if((head<=offset&&offset<tail)||!(tail<=offset&&offset<head)) { // check bound
                byte[] bytes = new byte[length];
                if (offset + length <= BLOCK_SIZE) {
                    block.copyToArray(offset, bytes, 0, length);
                } else {
                    int tmp = (int)(BLOCK_SIZE - offset);
                    block.copyToArray(offset, bytes, 0, tmp);
                    block.copyToArray(0, bytes, tmp, length - tmp);
                }
                if (head == offset) head = (head + offset) % BLOCK_SIZE;
                return bytes;
            }else{
                return null;
            }
        }
    }

    public PmemManager(String pmemPath){
        heap = Heap.createHeap(pmemPath, MAX_PMEM_SIZE);
        blockList = new ArrayList<>(topicNum);
        for(int i = 0;i < topicNum;i++){
            ArrayList<MyBlock> blocks = new ArrayList<>(queueNumPerTopic);
            for(int j = 0;j < queueNumPerTopic;j++){
                blocks.add(new MyBlock(heap.allocateCompactMemoryBlock(BLOCK_SIZE)));
            }
            blockList.add(blocks);
        }
    }

    /*
    * 异步写
    * */
    public int asyncWrite(){


    }

    /*
    * TODO: topicId: 将每个topic映射为 int 类型的 id， 可能 topicId 从 1 开始
    *  @return : 写入操作是否成功，返回 false 说明空间已满
    * */
    public int write(int topicId, int queueId, byte[] data){
        int dataSize = data.length;
        MyBlock myBlock = blockList.get(topicId - 1).get(queueId);
        return myBlock.put(data);
    }

    /*
    * 外部接口 通过计算fetchNum个Message的总长度；
    * @param offset:
    * */
    public byte[] read(int topicId, int queueId, long offset, long length){
        MyBlock myBlock = blockList.get(topicId - 1).get(queueId);
        return myBlock.get(offset, length);
    }
}
