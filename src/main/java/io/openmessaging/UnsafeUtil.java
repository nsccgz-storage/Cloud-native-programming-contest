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
	}
}

// public final class Unsafe {
// 	// 单例对象
// 	private static final Unsafe theUnsafe;
      
// 	private Unsafe() {
// 	}
// 	@CallerSensitive
// 	public static Unsafe getUnsafe() {
// 	  Class var0 = Reflection.getCallerClass();
// 	  // 仅在引导类加载器`BootstrapClassLoader`加载时才合法
// 	  if(!VM.isSystemDomainLoader(var0.getClassLoader())) {    
// 	    throw new SecurityException("Unsafe");
// 	  } else {
// 	    return theUnsafe;
// 	  }
// 	}
//       }
      