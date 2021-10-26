package io.openmessaging;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;
import java.lang.reflect.Field;


public class UnsafeUtil {
	public static final Unsafe UNSAFE;
	static {
	    try {
		Field field = Unsafe.class.getDeclaredField("theUnsafe");
		field.setAccessible(true);
		UNSAFE = (Unsafe) field.get(null);
	    } catch (Exception e) {
		throw new RuntimeException(e);
	    }
	}
	public static void main(String[] args) {
		long addr = UNSAFE.allocateMemory(10);
		System.out.println(addr);
		UNSAFE.storeFence();
	}
}
      