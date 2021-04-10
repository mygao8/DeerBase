package deerBase;


import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

public class LockManager {
	// keep track of all locks holden by specific tid
	private ConcurrentMap<TransactionId, List<Lock>> tid2Lock;
	// store which transactions hold this shared lock
    private ConcurrentMap<PageId, List<Lock>> pid2Lock;
    
    public LockManager() {
    	tid2Lock = new ConcurrentHashMap<>();
		pid2Lock = new ConcurrentHashMap<>();
    }
    
    public synchronized ConcurrentMap<TransactionId, List<Lock>> getTid2LockMap() {
    	return this.tid2Lock;
    }
    
    public synchronized ConcurrentMap<PageId, List<Lock>> getLockMap() {
    	return this.pid2Lock;
    }
    
    public synchronized List<Lock> getLocksOnPage(PageId pid) {
    	return pid2Lock.get(pid);
    }
    
    public synchronized List<Lock> getLocksInTransation(TransactionId tid) {
    	return tid2Lock.get(tid);
    }
    
    public synchronized boolean tryAcquireLock(TransactionId tid, PageId pid, Permissions perm) {
    	if (perm == Permissions.READ_ONLY) {
    		return tryAcquireSLock(tid, pid);
    	}
    	if (perm == Permissions.READ_WRITE) {
    		return tryAcquireXLock(tid, pid);
    	}
    	else {
    		System.out.println("unsupported perm");
    		return false;
    	}
    }
    
    public synchronized boolean tryAcquireSLock(TransactionId tid, PageId pid) {
    	debug(tid, pid, "tryAcquireS");
    	List<Lock> lockList = pid2Lock.get(pid);
    	if (lockList == null) {
    		pid2Lock.put(pid, new LinkedList<Lock>() {{
    			add(new Lock(LockMode.R, pid, tid));
    		}});
    		return true;
    	}
    	
    	synchronized (lockList) {
    		//assertFalse(lockList.isEmpty());
    		
    		LockMode curMode = lockList.get(0).getMode();
    		if (curMode == LockMode.R) {
    			lockList.add(new Lock(LockMode.R, pid, tid));
    			return true;
    		} else {
    			return false;
    		}
		}
    }
    
    public synchronized boolean tryAcquireXLock(TransactionId tid, PageId pid) {
    	debug(tid, pid, "tryAcquireX");
    	List<Lock> lockList = pid2Lock.get(pid);
    	if (lockList == null) {
    		pid2Lock.put(pid, new LinkedList<Lock>() {{
    			add(new Lock(LockMode.X, pid, tid));
    		}});
    		return true;
    	}
    	
    	synchronized (lockList) {
    		Lock firstLock = lockList.get(0);
    		if (firstLock.isX()) {
    			return firstLock.getPageId().equals(pid);
    		} else {
    			// upgrade
    			return upgrade(tid, pid);
    		}
    	}
    }
    
    public synchronized boolean releaseLock(Lock lock) {
    	return releaseLock(lock.getTransactionId(), lock.getPageId());
    }
    
    public synchronized boolean releaseLock(TransactionId tid, PageId pid) {
    	debug(tid, pid, "release");
    	List<Lock> lockList = pid2Lock.get(pid);
    	if (lockList == null) {
    		return false;
    	}
    	
    	synchronized (lockList) {
    		Predicate<Lock> sameTid = (lock) -> {
    			if (lock.getTransactionId().equals(tid)) {
    				pid.notifyAll();
    				return true;
    			}
    			return false;
    		};
    		return lockList.removeIf(sameTid);
		}
    }
    
    // if no such lock, or cannot upgrade (more than one S), return false
    // if already X, return true
    public synchronized boolean upgrade(TransactionId tid, PageId pid) {
    	debug(tid, pid, "upgrade");
    	List<Lock> lockList = pid2Lock.get(pid);
    	if (lockList == null) {
    		return false;
    	}
    	
    	synchronized (lockList) {
    		int numRLock = 0;
    		for (Lock lock : lockList) {
    			if (lock.isX()) {
    				return lock.getTransactionId().equals(tid);
    			}
    			if (++numRLock > 1) {
    				return false;
    			}
    		}
    		releaseLock(tid, pid);
    		return tryAcquireXLock(tid, pid);
    	}
    }
    
    
    public boolean holdsLock(TransactionId tid, PageId pid) {
    	debug(tid, pid, "holdslock");
    	List<Lock> lockList = pid2Lock.get(pid);
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
    
    public void debug(TransactionId tid, PageId pid, String s) {
    	System.out.println("Tid:"+tid+" Pid:"+pid+" "+s);
    }
    
    public void debug(Lock lock, String s) {
    	System.out.println(lock + " " + s);
    }
}
