package deerBase;


// page-level S, X lock
public class Lock {
	private PageId pId;
	private LockMode mode; // 0 is shared lock, 1 is exclusive lock
	private TransactionId tId;
	//private LinkedList<Request> waitingList;
	
	public Lock(LockMode mode, PageId pId, TransactionId tId) {
		this.mode = mode;
		this.pId = pId;
		this.tId = tId;
	}
	
	public Lock(Permissions perm, PageId pId, TransactionId tId) {
		this.mode = perm == Permissions.READ_ONLY ? 
				LockMode.R : LockMode.X;
		this.pId = pId;
		this.tId = tId;
	}
	
	public PageId getPageId() {
		return this.pId;
	}

	public LockMode getMode() {
		return this.mode;
	}
	
	public boolean isR() {
		return this.mode == LockMode.R;
	}
	
	public boolean isX() {
		return this.mode == LockMode.X;
	}
	
	public TransactionId getTransactionId() {
		return this.tId;
	}
	
	@Override
	public String toString() {
		return mode+"Lock" + " pid:" + pId + " tid:" + tId;
	}
	
	@Override
	public boolean equals(Object o) {
		if (o == this) return true;
		
		if (o instanceof Lock) {		
			Lock anotherLock = (Lock) o;
			return this.mode.equals(anotherLock.mode)
					&& this.tId.equals(anotherLock.tId)
					&& this.pId.equals(anotherLock.pId);
		}
		
		return false;
	}
	
	@Override
	public int hashCode() {
		String lockModeHashCodeString = this.mode == LockMode.R ? "0" : "1";
		String hashCodeString = String.valueOf(this.pId.hashCode())
				+ String.valueOf(this.tId.hashCode())
				+ lockModeHashCodeString;
		return Integer.parseInt(hashCodeString);
	}
}
