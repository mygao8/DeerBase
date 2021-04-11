package deerBase;

public class MyDebug {
	static MyDebug debug = new MyDebug(1);
	static int debugLevel = 2; // 2 for all, 1 for log without test, 0 for nothing
	
	public MyDebug(int debugLevel) {
		// TODO Auto-generated constructor stub
		this.debugLevel = debugLevel;
	}
	
	public static void print(int level, String message, Object... args) {
		if (debugLevel >= level) {
			System.out.printf(message, args);
		    System.out.println();
		}
	}
}
