package deerBase;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

public class oldLockManager {
    // keep track of all locks holden by specific tid
	public ConcurrentMap<TransactionId, List<Lock>> tid2Lock;
	// store which transactions hold this shared lock
    public ConcurrentMap<PageId, List<TransactionId>> sMap;
	// store which transactions hold this exclusive lock
    public ConcurrentMap<PageId, List<TransactionId>> xMap;
    
    public ConcurrentMap<PageId, List<Lock>> lockMap;
    
    public ReentrantLock lock;
    
    public oldLockManager() {
		tid2Lock = new ConcurrentHashMap<>();
		sMap = new ConcurrentHashMap<>();
		xMap = new ConcurrentHashMap<>();
		lock = new ReentrantLock();
	}
    
    public boolean acquireLock(TransactionId tid, Permissions perm, PageId pid) {
    	if (perm == Permissions.READ_ONLY) {
    		return this.tryAcquireSLock(tid, pid);
    	}
    	else {
    		return this.tryAcquireXLock(tid, pid);
    	}
    }
    
    public boolean tryAcquireSLock(TransactionId tid, PageId pid) {
    	if (this.holdsSLock(tid, pid) || this.holdsXLock(tid, pid)) return true;
    	
    	// tid has no lock
    	if (xMap.containsKey(pid)) {
    		
    	}
    	
    	// wrong return
    	return false;
    }
    
    public boolean tryAcquireXLock(TransactionId tid, PageId pid) {
		return false;
    	
    }
    
    public Lock upgrade(TransactionId tid, PageId pid) {
		return null;
    	
    }
    
    public boolean holdsSLock(TransactionId tid, PageId pid) {
		return false;
    	
    }
    
    public boolean holdsXLock(TransactionId tid, PageId pid) {
		return false;
    	
    }
}
