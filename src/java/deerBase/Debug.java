
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
  private static final int DEBUG_LEVEL; // now: 1 for all (deerbase and test), 0 for deerbase
  static {
      String debug = System.getProperty("deerBase.Debug");
      if (debug == null) {
          // No system property = disabled
          DEBUG_LEVEL = 1; // should be null
      } else if (debug == "") {
          // Empty property = level 0
          DEBUG_LEVEL = 0;
      } else {
          DEBUG_LEVEL = Integer.parseInt(debug);
      }
  }

  private static final int DEFAULT_STACK_TRACE_DEPTH = 5;
  private static final int DEFAULT_LEVEL = 0;
  public static final int DEER_BASE = 0;
  public static final int TEST = 1;

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
  
  public static String stackTrace(int depth) {
	  // from 2 to depth+2, to remove getStackTrace(Thread.java), stackTrace(Debug.java)
	  return '\n' + Arrays.toString(
			  Arrays.copyOfRange(
					  Thread.currentThread().getStackTrace(), 2, depth+2
					  ));
  }
  
  public static String stackTrace() {
	  return '\n' + Arrays.toString(
			  Arrays.copyOfRange(
					  Thread.currentThread().getStackTrace(), 2, DEFAULT_STACK_TRACE_DEPTH+2
					  ));
  }
}
