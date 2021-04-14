
package deerBase;

import java.util.Arrays;

/**
 * Debug is a utility class that wraps println statements and allows
 * more or less command line output to be turned on.
 * <p>
 * Change the value of the DEBUG_LEVEL constant using a system property:
 * deerBase.Debug. For example, on the command line, use -DdeerBase.Debug=x,
 * or simply -DdeerBase.Debug to enable it at level 0.
 * The log(level, message, ...) method will print to standard output if the
 * level number is less than or equal to the currently set DEBUG_LEVEL.
 */

public class Debug {
  private static final int DEBUG_LEVEL; // now: 2 for all (deerbase and test), 1 for deerbase, 0 for nothing
  static {
      String debug = System.getProperty("deerBase.Debug");
      if (debug == null) {
          // No system property = disabled
          DEBUG_LEVEL = 2; // null to close all
      } else if (debug == "") {
          // Empty property = level 0
          DEBUG_LEVEL = 0;
      } else {
          DEBUG_LEVEL = Integer.parseInt(debug);
      }
  }

  private static final int DEFAULT_STACK_TRACE_DEPTH = 10;
  private static final int DEFAULT_LEVEL = 1; // <= debug, always print
  public static final int DEER_BASE = 1;
  public static final int TEST = 2;
  public static final int CLOSE = 10;

  /** Log message if the log level >= level. Uses printf. */
  public static void log(int level, String message, Object... args) {
    if (isEnabled(level)) {
      System.out.printf(message, args);
      System.out.println();
    }
  }

  /** @return true if level is being logged. */
  public static boolean isEnabled(int level) {
    return level <= DEBUG_LEVEL;
  }

  /** @return true if the default level is being logged. */
  public static boolean isEnabled() {
    return isEnabled(DEFAULT_LEVEL);
  }

  /** Logs message at the default log level. */
  public static void log(String message, Object... args) {
    log(DEFAULT_LEVEL, message, args);
  }
  
  public static String stackTrace(int from, int depth) {
	  // from 2 to depth+2, to remove getStackTrace(Thread.java), stackTrace(Debug.java)
	  // Math.min()-1 to remove jdk.internal.reflect.NativeMethodAccessorImpl.invoke0
	  StackTraceElement[] stackTraces = Thread.currentThread().getStackTrace();
	  return '\n' + Arrays.toString( 
			  Arrays.copyOfRange(
					  stackTraces, 
					  from, Math.min(depth+from, stackTraces.length)-1
					  ));
  }
  
  public static String stackTrace(int depth) {
	  // from 2 to depth+2, to remove getStackTrace(Thread.java), stackTrace(Debug.java)
	  // Math.min()-1 to remove jdk.internal.reflect.NativeMethodAccessorImpl.invoke0
	  StackTraceElement[] stackTraces = Thread.currentThread().getStackTrace();
	  return '\n' + Arrays.toString(
			  Arrays.copyOfRange(
					  stackTraces,
					  2, Math.min(depth+2, stackTraces.length)-1
					  ));
  }
  
  public static String stackTrace() {
	  // Math.min()-1 to remove jdk.internal.reflect.NativeMethodAccessorImpl.invoke0
	  StackTraceElement[] stackTraces = Thread.currentThread().getStackTrace();
	  return '\n' + Arrays.toString(
			  Arrays.copyOfRange(
					  stackTraces,
					  2, Math.min(DEFAULT_STACK_TRACE_DEPTH+2, stackTraces.length)-1
					  ));
  }
}
