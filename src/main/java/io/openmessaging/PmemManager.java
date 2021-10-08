package io.openmessaging;

import com.intel.pmem.llpl.Heap;

public class PmemManager {
    final long MAX_PMEM_SIZE = 60*1024L*1024L*1024L; // 60GB
    Heap heap;

    public PmemManager(String pmemPath){
        heap = Heap.createHeap(pmemPath, MAX_PMEM_SIZE);

    }

    public void put(){

    }
}
