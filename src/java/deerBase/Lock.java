package deerBase;

import java.util.LinkedList;

import com.google.common.base.MoreObjects.ToStringHelper;

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
	
	public String toString() {
		return mode+"Lock" + " pid:" + pId + " tid:" + tId;
	}
}
