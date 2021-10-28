package io.openmessaging;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.Vector;

public class Main {
    public static void main(String args[]) throws IOException{
        // DefaultMessageQueueImpl mq = new DefaultMessageQueueImpl();
        // String t = "1234567890123456789012345678901234567890";
        // System.out.println( t.hashCode());
        // System.out.println(mq.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        // System.out.println(mq.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        // System.out.println(mq.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        // System.out.println(mq.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        // System.out.println(mq.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        // System.out.println(mq.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        // System.out.println(mq.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        // System.out.println(mq.append("dsfsf", 123, ByteBuffer.wrap(t.getBytes())));
        // System.out.println(mq.append("32424", 256, ByteBuffer.wrap(t.getBytes())));

        // Map<Integer, ByteBuffer> res = mq.getRange("12345", 123, 0L, 100);

        
        // String dirPath = "/home/ubuntu/test";
        // SSDqueue ssdQueue = new SSDqueue(dirPath);
        // String t = "1234567890123456789012345678901234567890";
        // System.out.println( t.hashCode());
        // System.out.println(ssdQueue.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        // System.out.println(ssdQueue.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        // System.out.println(ssdQueue.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        // System.out.println(ssdQueue.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        // System.out.println(ssdQueue.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        // System.out.println(ssdQueue.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        // System.out.println(ssdQueue.append("12345", 123, ByteBuffer.wrap(t.getBytes())));
        // System.out.println(ssdQueue.append("dsfsf", 123, ByteBuffer.wrap(t.getBytes())));
        // System.out.println(ssdQueue.append("32424", 256, ByteBuffer.wrap(t.getBytes())));

        // for(long idx=0; idx < 10; idx++){
        //     Map<Integer, ByteBuffer> res = ssdQueue.getRange("12345", 123, idx, 100);
        //     System.out.println("---------****-------------------------");
        //     for(Map.Entry<Integer, ByteBuffer> entry: res.entrySet()){
        //         System.out.println("" + entry.getKey() + " : " + new String(entry.getValue().array()));
        //     }
        // }


        MyDRAMbuffer dramBuffer = new MyDRAMbuffer();
        
        Vector<String> testData = new Vector<>();
        Vector<Integer> testAddr = new Vector<>();
        Vector<Integer> testSize = new Vector<>();  

        String t = "1234567890123456789012345678901234567890";
        
        byte[] data = new byte[5 * 1024];
        for(int i=0; i<data.length; i++){
            data[i] =  (byte)(i % 128);
        }
        
        ByteBuffer tmp0 = ByteBuffer.wrap(data); 
        for(int i=0; i<10; i++){
            
            ByteBuffer tmp = ByteBuffer.wrap(data); 
            int addr = dramBuffer.put(tmp);

            if(addr == -1){
                System.out.println("full!");
                break;
            }

            testAddr.add(addr);
            
        }        
        int i = 0;
        for(i=0; i<10; i++){
            int addr = testAddr.get(i);
            ByteBuffer tmp2 = dramBuffer.read(addr, data.length);
            
            ByteBuffer tmp = ByteBuffer.wrap(data); 
            addr = dramBuffer.put(tmp);

            testAddr.add(addr);

            // String t2 = new String(tmp2.array());
            // System.out.println(t2);
            if(!tmp2.equals(tmp0)){
                System.out.println("error!");
            }else{
                System.out.println("ok!");
            }
        }

        System.out.println("---------------------------------");;

        for(;i < 20; i++){
            // ByteBuffer tmp2 = ByteBuffer.allocate(t.length());
            int addr = testAddr.get(i);
            ByteBuffer tmp2 = dramBuffer.read(addr, data.length);
            // tmp2.flip();
            //String t2 = new String(tmp2.array());
            //System.out.println(t2);
            System.out.println(dramBuffer.toString());
            if(tmp2.equals(tmp0)){
                System.out.println("ok!");
            }
        }
    }

   
}

