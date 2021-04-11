package deerBase;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.TimeUnit;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int DEFAULT_PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 5120;
    
    /** Default ratio of loading a full table into buffer pool, which equals
    to table.numPages() / BufferPool.numPages(). */
    public static final double DEFUALT_LOAD_TABLE_RATIO = (float) 0.3;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    private static double loadTableRatio = DEFUALT_LOAD_TABLE_RATIO;
    
    private int numPages;
    private int numUsedPages;
    private LRUCache pageMap;
    private LRUCache fixedMap;
    
    //private Database.getLockManager() Database.getLockManager();
    
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
    	this.numPages = numPages;
    	this.numUsedPages = 0;
    	this.pageMap = new LRUCache(numPages);
    	this.fixedMap = new LRUCache(numPages);
    }

    public BufferPool(int numPages, float loadTableRatio) {
    	this(numPages);
    	this.loadTableRatio = loadTableRatio;
    }
    
    public static int getPageSize() {
		return pageSize;
	}
    
    // Only used for testing
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // Only used for testing
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }
    
    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
    	// TODO: tid and perm
    	Database.getLockManager().debug(tid, pid, "try getPage 0 times");
    	//synchronized (pid) {
    	
		boolean acquired = Database.getLockManager().tryAcquireLock(tid, pid, perm);
		int counter = 1;
		while (!acquired) {
			try {
				TimeUnit.MILLISECONDS.sleep(10);
				
				if (++counter > 200) {
					break;
				}
				
				acquired = Database.getLockManager().tryAcquireLock(tid, pid, perm);
				if (!acquired) {
					Database.getLockManager().debug(tid, pid, "try getPage "+ counter + "times");
				}
				
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		//}
    	
    	if (counter > 200) {
    		throw new TransactionAbortedException();
    	}
    	
    	Database.getLockManager().debug(tid, pid, "successfully getPage with try "+ counter);
    	
    	
    	if (pageMap.containsKey(pid)) {
    		return pageMap.get(pid);
    	}
    	
    	// pid is not in buffer pool
    	// get the heapFile corresponding to pid
    	DbFile dbFile = Database.getCatalog().getDbFile(pid.getTableId());
    	String tableName = Database.getCatalog().getTableName(dbFile.getTableId());
    	
//    	if (dbFile instanceof HeapFile) {
//    		//System.out.println("dbFile is HeapFile");
//    		//System.out.println((double) dbFile.getNumPages() + " / " 
//    		//		+ (double) this.numPages + " <= " + loadTableRatio);
//    	}
    	
    	
//    	if (tableName.equals("paperauths")) {
//    		//System.out.println("authors");
//    		if (fixedMap.containsKey(pid)) {
//    			return fixedMap.get(pid);
//    		}
//    		
//    		Page resPage = dbFile.readPage(pid);
//    		fixedMap.put(pid, resPage);
//    		return resPage;
//    	}
    	
    	// load full table
//    	if (dbFile instanceof HeapFile && 
//    			(double) dbFile.getNumPages() / (double) this.numPages <= loadTableRatio) {
//    		//System.out.println("load full table for " + tableName);
//    		for (int pageNo = 0; pageNo < dbFile.getNumPages(); pageNo++) {
//    			PageId heapPid = new HeapPageId(dbFile.getTableId(), pageNo);
//    			if (!pageMap.containsKey(heapPid)) {
//    				Page page = dbFile.readPage(heapPid);
//    				pageMap.put(heapPid, page);
//    				numUsedPages++;
//    			}
//    		}
//    	}
    	
    	Page resPage = dbFile.readPage(pid);
    	//System.out.println("load page for " + tableName + " page #" + pid.pageNumber());

    	
    	pageMap.put(pid, resPage);
    	numUsedPages++;
        return resPage;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for proj1
    	
    	Database.getLockManager().releaseLocksOnPage(pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
    	
    	Database.getLockManager().releaseLocksOnTxn(tid);   	
    	
    }
    
    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for proj1
        
    	return Database.getLockManager().holdsLock(tid, pid);
    }
    
    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for proj1
    	
    	transactionComplete(tid);
    }
    
    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to. 
     * May block if the lock cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     * @throws IOException 
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, TransactionAbortedException, IOException {
    	DbFile table = Database.getCatalog().getDbFile(tableId);
    	ArrayList<Page> ditryPages = table.insertTuple(tid, t);
    	for (Page page : ditryPages) {
    		page.markDirty(true, tid);
			// update to the new version of dirty pages, put in BufferPool
	    	pageMap.put(page.getId(), page);
	    	numUsedPages++;
    	}
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     * @throws IOException 
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException, IOException {
    	int tableId = t.getRecordId().getPageId().getTableId();
    	DbFile table = Database.getCatalog().getDbFile(tableId);
    	
    	ArrayList<Page> ditryPages = table.deleteTuple(tid, t);
    	for (Page page : ditryPages) {
    		page.markDirty(true, tid);
			// update to the new version of dirty pages, put in BufferPool
	    	pageMap.put(page.getId(), page);
	    	numUsedPages++;
    	}
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break deerBase if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
    	Iterator<PageId> pidItr = pageMap.keyIterator();
    	while (pidItr.hasNext()) {
			flushPage(pidItr.next());
		}
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
    	pageMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
    	DbFile tableFile = Database.getCatalog().getDbFile(pid.getTableId());
    	Page flushedPage = pageMap.get(pid);
    	tableFile.writePage(flushedPage);
    	flushedPage.markDirty(false, null);
    }
    

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for proj1
    }
}
