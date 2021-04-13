package deerBase;



import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class LockManager {
	// keep track of all locks holden by specific tid
	private ConcurrentMap<TransactionId, List<Lock>> txnLockMap;
	// store which transactions hold this shared lock
    private ConcurrentMap<PageId, List<Lock>> pageLockMap;
    
    int debug = 0; // close debug at the setup phase (@Before) of tests
    boolean foreverCloseDebug = false;
    
    public LockManager() {
    	txnLockMap = new ConcurrentHashMap<>();
		pageLockMap = new ConcurrentHashMap<>();
    }
    
    private synchronized ConcurrentMap<TransactionId, List<Lock>> getTid2LockMap() {
    	return this.txnLockMap;
    }
    
    private synchronized ConcurrentMap<PageId, List<Lock>> getLockMap() {
    	return this.pageLockMap;
    }
    
    private synchronized List<Lock> getLocksOnPage(PageId pid) {
    	return pageLockMap.get(pid);
    }
    
    private synchronized List<Lock> getLocksOnTransation(TransactionId tid) {
    	return txnLockMap.get(tid);
    }
    
    // used for flush pages belong to tid when txn commits
    public synchronized List<PageId> getPageIdsOnTransactionId (TransactionId tid) {    	
    	List<Lock> locksOnTxn = getLocksOnTransation(tid);
    	if (locksOnTxn == null) 
    		return new LinkedList<PageId>();
    	
    	List<PageId> res = locksOnTxn.stream()
    			.map(Lock::getPageId)
    			.distinct()
    			.collect(Collectors.toList());
    	
    	return res;
    }
    
    // the unique API to acquire lock for other classes
    // also the unique function where put lock into txnLockMap
    public synchronized boolean tryAcquireLock(TransactionId tid, PageId pid, Permissions perm) {
//    	if (tid.getId() == 0) {
//    		// for test tid
//    		return true;
//    	}
    	debug(tid, pid, "tryAcquire" + perm);
    	boolean acquired = false;
    	if (Permissions.READ_ONLY.equals(perm)) {
    		acquired = tryAcquireSLock(tid, pid);
    	}
    	else if (Permissions.READ_WRITE.equals(perm)) {
    		acquired = tryAcquireXLock(tid, pid);
    	}
    	else {
    		System.out.println("unsupported perm");
    		return false;
    	}
    	
    	if (acquired) {
    		List<Lock> locksOnTxn = txnLockMap.get(tid);
    		Lock acquiredLock = new Lock(perm, pid, tid);
    		
    		debug(tid, pid, "try to put lock into txnLockMap");
    		if (locksOnTxn == null) {
    			txnLockMap.put(tid, new LinkedList<Lock>() {{
        			add(acquiredLock);
        		}});
    		} 
    		else {
    			if (locksOnTxn.isEmpty()) {
    				debug("=========Empty List in Map Value=========");
    			}
    			if (locksOnTxn.contains(acquiredLock)) {
    				debug(tid, pid, "already in txnLockMap");
    			}
    			else {
    				locksOnTxn.add(acquiredLock);
    				debug(tid, pid, "successfully put lock into txnLockMap");
    			}
    			
    		}
    	}
    	
    	return acquired;
    }
    
    private synchronized <K> void putLockInMap(ConcurrentMap<K, List<Lock>> map, 
    		K key, Lock acquiredLock) {
			
    	List<Lock> locksList = map.get(key);
    	if (locksList == null) {
    		map.put(key, new LinkedList<Lock>() {{
    			add(acquiredLock);
    		}});
    		
//    		debug(acquiredLock.getTransactionId(), acquiredLock.getPageId(), 
//    				"tryAcquire" + acquiredLock.getMode() 
//    				+ "in putLockInMap: no lock in cur page, true");
    		return;
    	} 
    	
    	synchronized (locksList) {
    		locksList.add(acquiredLock);
    	}
    }
    
    private synchronized boolean tryAcquireSLock(TransactionId tid, PageId pid) {
    	// debug(tid, pid, "tryAcquireS");
    	List<Lock> locksOnPage = pageLockMap.get(pid);
    	if (locksOnPage == null) {
    		pageLockMap.put(pid, new LinkedList<Lock>() {{
    			add(new Lock(LockMode.R, pid, tid));
    		}});
    		debug(tid, pid, "tryAcquireS, no lock in cur page, true");
    		
    		return true;
    	}
    	
    	debug(tid, pid, "tryAcquireS before lock locksOnPage");
    	synchronized (locksOnPage) {
    		//assertFalse(lockList.isEmpty());
    		LockMode curMode = locksOnPage.get(0).getMode();
    		debug(tid, pid, "tryAcquireS: lock" + curMode + " on locksOnPage");
    		if (curMode == LockMode.R) {    		
    			// already has SLock on this page, do nothing
    			for (Lock lock : locksOnPage) {
    				if (lock.getTransactionId().equals(tid)) {
    					debug(tid, pid, "tryAcquireS: is already hold S? true, do nothing");
    					return true;
    				}
    			}
    			
    			// has no SLock, add SLock
    			debug(tid, pid, "tryAcquireS: is already hold S? false, add SLock on page");
    			locksOnPage.add(new Lock(LockMode.R, pid, tid));	
    			return true;
    		} else {
    			if (locksOnPage.size() > 1) {
    				debug("=========="+ locksOnPage.size() +"locks on Page=========");
    			}
    			
    			// if own XLock, equivalent to acquire S successfully
    			// else, others XLock, cannot get S
    			debug(tid, pid, 
    					"tryAcquireS: X is holden by  " + locksOnPage.get(0).getTransactionId() 
    					+ ". My own X? " + locksOnPage.get(0).getTransactionId().equals(tid));
    			return locksOnPage.get(0).getTransactionId().equals(tid);
    		}
		}
    	
    }
    
    private synchronized boolean tryAcquireXLock(TransactionId tid, PageId pid) {  	
    	// debug(tid, pid, "tryAcquireX");
    	List<Lock> locksOnPage = pageLockMap.get(pid);
    	if (locksOnPage == null) {
    		pageLockMap.put(pid, new LinkedList<Lock>() {{
    			add(new Lock(LockMode.X, pid, tid));
    		}});
    		debug(tid, pid, "tryAcquireX: no lock in cur page, true");
    		return true;
    	}
    	
    	debug(tid, pid, "tryAcquireX: before lock locksOnPage");
    	synchronized (locksOnPage) {
    		Lock firstLock = locksOnPage.get(0);
    		debug(tid, pid, "tryAcquireX: lock" + firstLock.getMode() + " on locksOnPage");
    		if (firstLock.isX()) {
    			debug(tid, pid, 
    					"tryAcquireX: X is holden by  " + firstLock.getTransactionId() 
    					+ ". My own X? " + firstLock.getTransactionId().equals(tid));
    			return firstLock.getTransactionId().equals(tid);
    		} else {
    			if (!firstLock.getTransactionId().equals(tid)) { // others' RLock
    				debug(tid, pid, 
        					"tryAcquireX: first of "+locksOnPage.size()+" S is holden by  " + firstLock.getTransactionId() 
        					+ ". Not my S, fail.");
    				return false;
    			}
    			
    			// own RLock, upgrade
    			debug(tid, pid, "tryAcquireX: upgrade, should be own RLock: " 
    					+ firstLock.getTransactionId().equals(tid));
    			return upgrade(tid, pid);
    		}
    	}
    }
    
    // overload
    public synchronized boolean releaseLock(Lock lock) {
    	return releaseLock(lock.getTransactionId(), lock.getPageId());
    }
    
    public synchronized boolean releaseLock(TransactionId tid, PageId pid) {   
    	debug(tid, pid, "release lock");
    	return removeLockFromPageLockMap(tid, pid) 
    			&& removeLockFromTxnLockMap(tid, pid);
    }
    
    private boolean removeLockFromTxnLockMap(Lock lock) {
    	return removeLockFromTxnLockMap(lock.getTransactionId(), lock.getPageId());
    }

    // return false if failed to remove lock from list or remove key from map
	private boolean removeLockFromTxnLockMap(TransactionId tid, PageId pid) {
		
		List<Lock> locksOnTxn = txnLockMap.get(tid);
    	if (locksOnTxn == null) {
    		debug(tid, pid, "already released in txnLockMap");
    		return false;
    	}
    	else {
    		boolean res;
	    	synchronized (locksOnTxn) {
	    		Predicate<Lock> samePid = (lock) -> {
	    			if (lock.getPageId().equals(pid)) {
	    				debug(tid, pid, "release single lock in txnLockMap");
	    				return true;
	    			}
	    			return false;
	    		};
	    		
	    		res = locksOnTxn.removeIf(samePid);
	    		
	    		if (locksOnTxn.isEmpty()) {
	    			debug(tid, pid, "after release, no locks on txn" + tid.getId());
	    			res = res && txnLockMap.remove(tid, locksOnTxn);
	    		}
			}
			return res;
    	}
	}

	private boolean removeLockFromPageLockMap(Lock lock) {
		return removeLockFromPageLockMap(lock.getTransactionId(), lock.getPageId());
	}
	
	// return false if failed to remove lock from list or remove key from map
	private boolean removeLockFromPageLockMap(TransactionId tid, PageId pid) {
		boolean res;
		
		List<Lock> locksOnPage = pageLockMap.get(pid);
    	if (locksOnPage == null) {
    		debug(tid, pid, "already released in pageLockMap");
    		return false;
    	}
    	else {
    	  	synchronized (locksOnPage) {
	    		Predicate<Lock> sameTid = (lock) -> {
	    			if (lock.getTransactionId().equals(tid)) {
	    				debug(tid, pid, "release single lock in pageLockMap");
	    				// pid.notifyAll();
	    				return true;
	    			}
	    			return false;
	    		};
	    		res = locksOnPage.removeIf(sameTid);
	    		
	    		if (locksOnPage.isEmpty()) {
	    			debug(tid, pid, "after release, no locks on this page " + pid.pageNumber());
	    			res = res && pageLockMap.remove(pid, locksOnPage);
	    		}
			}
    	}
		return res;
	}
    
    // release all locks belong to tid
    public synchronized boolean releaseLocksOnTxn(TransactionId tid) {
    	debug("try to release all locks of txn " + tid);
    	List<Lock> locksOnTxn = txnLockMap.get(tid);
    	if (locksOnTxn == null) {
    		debug("all locks of txn " + tid + " already released");
    		return false;
    	}
    	
    	synchronized (locksOnTxn) {
    		locksOnTxn.forEach(lock -> removeLockFromPageLockMap(lock));
    		
    		debug("remove locks of txn " + tid + " from txnLockMap");
			return txnLockMap.remove(tid, locksOnTxn);
		}
    }
    
 // release all locks belong to pid
    public synchronized boolean releaseLocksOnPage(PageId pid) {
    	debug("release all locks of page " + pid);
    	List<Lock> locksOnPage = pageLockMap.get(pid);
    	if (locksOnPage == null) {
    		debug("all locks of page " + pid + " already released");
    		return false;
    	}
    	
    	synchronized (locksOnPage) {
    		locksOnPage.forEach(lock -> removeLockFromTxnLockMap(lock));
			
    		debug("remove locks of page " + pid + " from pageLockMap");
			return pageLockMap.remove(pid, locksOnPage);
		}
    }
    
    // upgrade the lock pointed by <tid, pid> from S to X
    // if no such lock, or cannot upgrade (more than one S), return false
    // if already X, return true
    private synchronized boolean upgrade(TransactionId tid, PageId pid) {
    	debug(tid, pid, "try to upgrade");
    	List<Lock> locksOnPage = pageLockMap.get(pid);
    	if (locksOnPage == null) {
    		debug("upgrade: no locks on page" + pid.pageNumber());
    		return false;
    	}
    	
    	synchronized (locksOnPage) {
    		int numRLock = 0;
    		for (Lock lock : locksOnPage) {
    			if (lock.isX()) {
    				debug(tid, pid, "upgrade: X locks of txn"+ lock.getTransactionId() +" is on page"
    						+ "is my own X? " + lock.getTransactionId().equals(tid));
    				return lock.getTransactionId().equals(tid);
    			}
    			else { // RLock
    				if (++numRLock > 1) {
    					debug(tid, pid, "upgrade: has more than one Rlock");
    					debug(locksOnPageString(pid));
        				return false;
        			}
    				if (!lock.getTransactionId().equals(tid)) { // has other RLock
    					debug(tid, pid, "upgrde: has other Rlock:" + lock);
    					debug(locksOnPageString(pid));
    					return false;
    				}
    			}
    		}
    		
    		// can reach here: 
    		// only 1 Rlock of tid on this page
    		// OR locksOnPage is empty but not null (NEVER)
    		if (numRLock != 1) {
    			debug(tid, pid, "NEVER!!!!upgrade: shoule be only 1 lock of me on page, but has " +numRLock + "locks");
    			return false;
    		}
    		
    		if (locksOnPage.get(0).getTransactionId().equals(tid)) {
    			debug(tid, pid, "upgrade my own RLock");
    			debug(tid, pid, "===upgrade process start===");
        		releaseLock(tid, pid);
        		boolean res = tryAcquireLock(tid, pid, Permissions.READ_WRITE);
        		debug(tid, pid, "===upgrade process end===");
        		return res;
    		}
    		
    		debug(tid, pid, "==========shoule never reach here===========");
    		return false;
    	}
    }
    
    
    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
    	debug(tid, pid, "holdslock");
    	List<Lock> lockList = pageLockMap.get(pid);
    	if (lockList == null) {
    		return false;
    	}
    	
    	synchronized (lockList) {
    		for (Lock lock : lockList) {
    			if (lock.getTransactionId().equals(tid)) {
    				return true;
    			}
    		}
    		
    		return false;
    	}
    }
    
    public synchronized String locksOnPageString(PageId pid) {
    	List<Lock> locksOnPage = pageLockMap.get(pid);
    	if (locksOnPage == null) {
    		return "printLocksOnPage: no locks";
    	}
    	
    	StringBuffer sb = new StringBuffer('\n');
    	sb.append("On page" + pid.pageNumber() + ": ");
    	locksOnPage.stream().forEach(
    			lock -> {
    				sb.append("Txn" + lock.getTransactionId().getId());
    				sb.append(" " + lock.getMode() + ", ");
    			}
    	);
    	return sb.toString();
    }
    
    public synchronized String locksOnTxnString(TransactionId tid) {
    	List<Lock> locksOnTxn = txnLockMap.get(tid);
    	if (locksOnTxn == null) {
    		return "printLocksOnTxn: no locks";
    	}
    	
    	StringBuffer sb = new StringBuffer('\n');
    	sb.append("On txn" + tid.getId() + ": ");
    	locksOnTxn.stream().forEach(
    			lock -> {
    				sb.append("Page" + lock.getPageId().pageNumber());
    				sb.append(" " + lock.getMode() + ", ");
    			}
    	);
    	return sb.toString();
    }
    
    public void debug(TransactionId tid, PageId pid, String s) {
    	if (debug == 1 && !foreverCloseDebug)
    		System.out.println("Tid:"+tid+" Pid:"+pid+" "+s);
    }
    
    public void debug(Lock lock, String s) {
    	if (debug == 1 && !foreverCloseDebug)
    		System.out.println(lock + " " + s);
    }
    
    public void debug(String s) {
    	if (debug == 1 && !foreverCloseDebug)
    		System.out.println(s);
    }
    
    public void openDebug() {
    	debug = 1;
    }
    
    public void closeDebug() {
    	debug = 0;
    }
}
