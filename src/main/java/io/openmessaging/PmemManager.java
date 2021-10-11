package io.openmessaging;

import com.intel.pmem.llpl.MemoryPool;

import java.util.ArrayList;
import java.util.HashMap;

public class PmemManager {
    final long MAX_PMEM_SIZE = 60*1024L*1024L*1024L; // 60GB
    final long CHUNK_SIZE = 1024L*1024L*1024L; // 1GiB 每个线程1个chunk?
    final long PAGE_SIZE = 256*1024L; // 64KiB

    static int[] depthArray;
    HashMap<Integer, Integer> size2Depth;
    int maxDepth = (int) (Math.log(CHUNK_SIZE/PAGE_SIZE)/Math.log(2));; // TODO:Depth从0开始计数，max_depth = log2(CHUNK_SIZE/PAGE_SIZE)

    MemoryPool pool;
    Chunk[] chunkList;

    public PmemManager(String pmemPath){
        pool = MemoryPool.createPool(pmemPath, MAX_PMEM_SIZE);
        int chunkNum = (int) (MAX_PMEM_SIZE/CHUNK_SIZE);
        chunkList = new Chunk[chunkNum];
        for(int i = 0;i < chunkNum;i++){
            chunkList[i] = new Chunk(i);
        }
        size2Depth = new HashMap<>();
        depthArray = new int[maxDepth+1];
        int tmp = (int)CHUNK_SIZE; // TODO：检查是否超出int表示范围
        for(int i = 0;i <= maxDepth;i++){
            depthArray[i] = tmp;
            size2Depth.put(tmp, i);
            tmp /= 2;
        }
    }

    private class Chunk{
        ArrayList<Integer> memoryMap; // Vector是线程安全的，而ArrayList不是线程安全的 // 或许可用short来储存
        long handle;

        // 考虑初始化带来的开销
        public Chunk(int id){
            memoryMap = new ArrayList<>((int) (Math.pow(2, maxDepth+1)-1)); // 2^(maxDepth+1)-1
            for(int i = 0;i <= maxDepth;i++){
                for(int j = 0;j < i+1;j++){
                    memoryMap.add(i+1);
                }
            }
            handle = id * CHUNK_SIZE;
        }

        /**
         * @param size 需要保证 size 不会小于 page_size
         * @return -1 分配失败, 地址 = res * PAGE_SIZE
         **/
        public int allocate(int size){
//            size = normalizeCapacity(size);
//            if(size > depthArray.get(0)){} // TODO：分配的空间过大与过小的情形

            int depth;
            for(depth = 0;depth <= maxDepth;depth++){
                if(size == depthArray[depth])break;
            }

            int index = 0, curDepth = 0;
            while(curDepth <= depth){
                if(memoryMap.get(index) > depth)return -1;

                if(memoryMap.get(index) == depth && curDepth == depth){
                    // TODO: allocate this block
                    int res = index;
                    memoryMap.set(index, maxDepth+1);
                    // update parent node
                    while(index > 0){
                        index = (index - 1)/2;
                        memoryMap.set(index, Math.min(memoryMap.get(index*2+1), memoryMap.get(index*2+2))); // 左右子节点的最小值
                    }
                    return res; // 具体地址表示为 (res - (1 << depth) + 1)*normalize_size
                }
                else if(memoryMap.get(index*2+1) <= depth){
                    index = index*2+1;
                }
                else{
                    index = index*2+2;
                }
                curDepth++;
            }
            return -1;
        }

        public void free(int index){
//            size = normalizeCapacity(size);
//            memoryMap.set(index, );
            int depth = 0, tmp = index+1;
            while(tmp > 1){
                tmp >>= 1;
                depth++;
            }
            memoryMap.set(index, depth);
            // update parent node
            while(index > 0){
                index = (index - 1)/2;
                memoryMap.set(index, Math.min(memoryMap.get(index*2+1), memoryMap.get(index*2+2))); // 左右子节点的最小值
            }
        }

        public long getAddress(int index, int size){
            return (index + 1 - (1L << size2Depth.get(size)))*size + handle; // FIXME: check size is normalized
        }
//        public int doubleCapacity(int index, int originSize){

//        }
    }

//    public void write(byte[] bytes, long handle, int dataSize){
//        pool.copyFromByteArray(bytes, 0, handle, dataSize);
////        pool.flush(handle, dataSize); // just write to page cache
//    }
//
//    public byte[] read(long handle, int dataSize){
//        byte[] bytes = new byte[dataSize];
//        pool.copyToByteArray(handle, bytes, 0, dataSize);
//        return bytes;
//    }

    // 将一个数向上取值2为最接近的2的整数幂
    public int normalizeCapacity(int num){
//         if (num <= 0) return 1; // assert(num > 0) ??
        if ((num & (num - 1)) == 0) return num;
        num |= num >> 1;
        num |= num >> 2;
        num |= num >> 4;
        num |= num >> 8;
        num |= num >> 16;
        return num + 1;
    }


    // 每个队列对应一个block
    public class MyBlock{
        long head, tail;
        long handle; // pool中的地址
        int index;
        int size;
        Chunk chunk;

        // head == tail empty
        // tail + 1 == head full
        // 假定传进来的size就是normalized的
        public MyBlock(int size, Chunk c){
            head = 0;
            tail = 0;
            this.size = size;
            chunk = c;
            index = chunk.allocate(size); // 具体地址表示为 (res - (1 << depth) + 1)*normalize_size
            handle = chunk.getAddress(index, size);
        }

        public int doubleCapacity(){
            if(index == 0)return -1;
            // 检查相邻的结点是否可以分配
            int neighbor = index % 2 == 0 ? index-1 : index+1;
            int depth = size2Depth.get(size);

//            if(memoryMap.get(neighbor)==depth){
//                if(index % 2 == 0){ // 偶数块需要迁移数据
//                    long srcOffset = (index - (1L << depth) + 1) *originSize;
//                    long dstOffset = srcOffset - originSize;
//                    pool.copyFromPool(srcOffset+handle, dstOffset+handle, originSize);
//                    return index-1;
//                }
//                else{
//                    return index;
//                }
//            } else{
                int newIndex = chunk.allocate(size*2);
                if(newIndex == -1)return -1;
                long srcOffset = chunk.getAddress(index, size);
                long dstOffset = chunk.getAddress(newIndex, size*2);
                if(head <= tail){
                    pool.copyFromPool(srcOffset+head, srcOffset, tail-head);
                    tail = tail - head;
                    head = 0;
                }else{
                    long tmp = size - head;
                    pool.copyFromPool(srcOffset+head, srcOffset, tmp);
                    pool.copyFromPool(srcOffset+tail, srcOffset+tmp, tail);
                    tail = tail + tmp;
                    head = 0;
                }
                chunk.free(index);
//                return newIndex;
//            }
            index = newIndex;
            size *= 2;
            handle = chunk.getAddress(index, size);
            return index;
        }

        public void freeSpace(){
            chunk.free(index);
        }

        public long getRemainSize(){
            return (head - tail - 1 + size)%size;
        }

        // TODO: 关注 handle是否为绝对地址
        public int put(byte[] data){
            if(data.length > getRemainSize())return -1; // block 空间已满

            int position = (int) tail;
            if(size - tail >= data.length) {
                copyFromArray(data, 0, tail, data.length);
                tail += data.length;
            }else{
                int tmp = (int)(size - tail);
                copyFromArray(data, 0, tail, tmp);
                copyFromArray(data, tmp, 0, data.length-tmp);
                tail = data.length - tmp;
            }
            return position;
        }
//           // TODO: 关注 handle是否为绝对地址
        public byte[] get(long offset, int length){
//            if((head<=offset&&offset<tail)||!(tail<=offset&&offset<head)) { // check bound
                byte[] bytes = new byte[length];
                if (offset + length <= size) {
                    copyToArray(offset, bytes, 0, length);
                } else {
                    int tmp = (int)(size - offset);
                    copyToArray(offset, bytes, 0, tmp);
                    copyToArray(0, bytes, tmp, length - tmp);
                }
                if (head == offset) head = (head + length) % size;
                return bytes;
//            }else{
//                return null;
//            }
        }

        private void copyFromArray(byte[] srcArray, int srcIndex, long dstOffset, int length){
            pool.copyFromByteArray(srcArray, srcIndex, dstOffset+handle, length);
        }

        private void copyToArray(long srcOffset, byte[] dstArray, int dstOffset, int length){
            pool.copyToByteArray(srcOffset+handle, dstArray, dstOffset, length);
        }
    }
}

