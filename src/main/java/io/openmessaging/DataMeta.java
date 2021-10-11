package io.openmessaging;

public class DataMeta{
    public long metaOffset;
    public long head;
    public long tail;
    public long totalNum;
    public long uselessIndex;
    

    public DataMeta(long metaOffset, long head, long tail, long totalNum){
        this.metaOffset = metaOffset;
        this.head = head;
        this.tail = tail;
        this.totalNum = totalNum;
        this.uselessIndex = 0L;
    }
    public DataMeta(){
        this.metaOffset = 0;
        this.head = 0;
        this.tail = 0;
        this.totalNum = 0;
        this.uselessIndex = 0L;
    }
    
}
