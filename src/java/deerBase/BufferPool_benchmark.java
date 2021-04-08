package deerBase;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool_benchmark {
    /** Bytes per page, including header. */
    public static final int DEFAULT_PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 5120; //20M

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    private int maxPages;
    private int numUsedPages;
    private LRUCache pageMap;
    
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool_benchmark(int numPages) {
    	this.maxPages = numPages;
    	this.numUsedPages = 0;
    	this.pageMap = new LRUCache(numPages);
    }

    public static int getPageSize() {
		return pageSize;
	}
    
    // Only used for testing
    public static void setPageSize(int pageSize) {
    	BufferPool_benchmark.pageSize = pageSize;
    }
    
    // Only used for testing
    public static void resetPageSize() {
    	BufferPool_benchmark.pageSize = DEFAULT_PAGE_SIZE;
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
    	if (pageMap.containsKey(pid)) {
    		return pageMap.get(pid);
    	}
    	    	
    	// pid is not in buffer pool
    	// get the heapFile corresponding to pid
    	DbFile tableFile = Database.getCatalog().getDbFile(pid.getTableId());
    	Page resPage = tableFile.readPage(pid);
    	pageMap.put(pid, resPage);
    	numUsedPages++;
        return resPage;
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
    	
    }
}
