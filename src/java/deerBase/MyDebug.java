package deerBase;

public class MyDebug {
	private static final int debugLevel = 2; 
	// private static MyDebug debug = new MyDebug(1);
	
	public static final int TEST = 2; // 2 used in test package
	public static final int BEERBASE = 1; // 1 used in deerbase package
	
//	public MyDebug(int level) {
//		// TODO Auto-generated constructor stub
//		debugLevel = level;
//	}
	
//	public static void print(int level, String message, Object... args) {
//		System.out.printf(message, args);
//		System.out.printf(" test mydebug: debugLevel=%d, curLevel=%d", debugLevel, level);
//		System.out.println();
//		if (debugLevel >= level) {
//			System.out.printf(message, args);
//		    System.out.println();
//		}
//	}
	public static void print(int level, String message, Object... args) {
		if (debugLevel >= level) {
			System.out.printf(message, args);
		    System.out.println();
		}
	}

	// set 0: nothing
	// set 1: remove debugs in test
	// set 2: print all
	public static void setDebugLevel(int level) {
		//debugLevel = level;
	}
}
