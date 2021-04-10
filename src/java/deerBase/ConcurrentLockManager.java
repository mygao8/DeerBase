package deerBase;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

public class ConcurrentLockManager {
	// keep track of all locks holden by specific tid
	public ConcurrentMap<TransactionId, List<Request>> tid2Lock;
	// store which transactions hold this shared lock
    public ConcurrentMap<PageId, List<TransactionId>> sMap;
	// store which transactions hold this exclusive lock
    public ConcurrentMap<PageId, TransactionId> xMap;
    
    public ConcurrentMap<PageId, RequestQueue> waitingMap;
    
    public ReentrantLock managerLock;
    
    public final long timeout;
    
    public ConcurrentLockManager() {
		tid2Lock = new ConcurrentHashMap<>();
		sMap = new ConcurrentHashMap<>();
		xMap = new ConcurrentHashMap<>();
		waitingMap = new ConcurrentHashMap<>();
		managerLock = new ReentrantLock();
		timeout = 500;
	}
    
    public boolean acquireLock(TransactionId tid, Permissions perm, PageId pid) {
    	if (perm == Permissions.READ_ONLY) {
    		return this.tryAcquireSLock(tid, pid);
    	}
    	else {
    		return this.tryAcquireXLock(tid, pid);
    	}
    }
    
    // if has s/x lock, return
    // if others have x granted request, wait
    // else, acquire
    public boolean tryAcquireSLock(TransactionId tid, PageId pid) {
    	// if (this.holdsSLock(tid, pid) || this.holdsXLock(tid, pid)) return true;
    	
		TransactionId xTid = xMap.get(pid);
		if (xTid != null) {
			synchronized (xTid) {
				long expiredTime = System.currentTimeMillis() + timeout;
				
				while (xTid != null) {
					try {
						xTid.wait(timeout);
						if (System.currentTimeMillis() >= expiredTime) {
							return false;
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				
				List<TransactionId> tIdList = sMap.get(pid);
				tIdList.add(tid);
			}
		}
		
		
		
		
		
    	// tid has no lock
    	if (xMap.containsKey(pid)) {
    		
    		
    		
    		Request lastXRequest = null;
//    		for (Request request : requestQ.getRequests()) {
//    			if (request.geTransactionId() == tid) {
//    				requestQ.unlock();
//    				return true;
//    			} else if (request.getLockMode() == LockMode.X) {
//    				lastXRequest = request;
//    			}
//    		}
//    		
//    		if (lastXRequest == null) {
//    			// no x request
//    		}
    		try {
				wait();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		managerLock.unlock();
    		return false;
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
    	if (sMap.containsKey(pid)) {
    		for (TransactionId onSLockTid : sMap.get(pid)) {
    			if (onSLockTid.equals(tid)) {
    				return true;
    			}
    		}
    	}
    	return false;
    }
    
    public boolean holdsXLock(TransactionId tid, PageId pid) {
    	return xMap.containsKey(pid) && xMap.get(pid).equals(tid);
    }
}
