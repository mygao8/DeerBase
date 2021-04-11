package deerBase;

/**
 * Class representing requested LockModes.
 * Private constructor with two static objects R and X that
 * represent the two modes of LockMode.
 */
public class LockMode {
  int lockMode;

  private LockMode(int lockMode) {
    this.lockMode = lockMode;
  }

  public String toString() {
    if (lockMode == 0)
      return "R";
    if (lockMode == 1)
      return "X";
    return "UNKNOWN";
  }

  public static final LockMode R = new LockMode(0);
  public static final LockMode X = new LockMode(1);

  @Override
  public boolean equals(Object o) {
	if (o == this) return true;
		
	if (o instanceof LockMode) {		
		LockMode another = (LockMode) o;
		return this.toString().equals(another.toString());
	}
	
	return false;
  }
	
  @Override
  public int hashCode() {
	return lockMode;
  }
  
}