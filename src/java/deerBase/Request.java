package deerBase;

import java.time.*;

public class Request {
	private PageId pId;
	private LockMode mode; // 0 is shared lock, 1 is exclusive lock
	private Instant requestTime;
	private TransactionId tId;
	private boolean granted;
	
	public Request(PageId pId, LockMode mode, TransactionId tId, boolean granted) {
		this.pId = pId;
		this.mode = mode;
		this.tId = tId;
		this.requestTime = Instant.now();
		this.granted = granted;
	}
	
	public PageId getPageId() {
		return this.pId;
	}
	
	public TransactionId geTransactionId() {
		return this.tId;
	}
	
	public LockMode getLockMode() {
		return this.mode;
	}
	
	public boolean isGranted() {
		return this.granted;
	}
	
	public void grant(boolean isGranted) {
		this.granted = isGranted;
	}
}
