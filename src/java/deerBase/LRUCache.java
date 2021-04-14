package deerBase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

// ref: 
public class LRUCache {
	private final static int LRUDebugLevel = Debug.CLOSE;
	
    class ListNode {
    	PageId pId;
        Page page;
        ListNode prev;
        ListNode next;
        public ListNode() {}
        public ListNode(PageId key, Page value) {
        	this.pId = key; 
        	this.page = value;
        }
        
        public String toString() {
        	if (page == null && pId == null) return String.valueOf(hashCode()).substring(0, 4);
        	if (page == null || pId == null) return "!!!wrong node: one of pId and page is null!!!";
        	
        	return pId.toString();
        }
    }

    private ConcurrentHashMap<PageId, ListNode> cache = new ConcurrentHashMap<>();
    private volatile int size;
    private final int capacity;
    private final ListNode dummyHead, dummyTail;
    private final int evictionPolicy = LRUCache.STEAL;
    private static final int NOSTEAL = 0;
    private static final int STEAL = 1;

    public LRUCache(int capacity) {
        this.size = 0;
        this.capacity = capacity;
        // fake head and tail
        dummyHead = new ListNode();
        dummyTail = new ListNode();
        dummyHead.next = dummyTail;
        dummyTail.prev = dummyHead;
    }

    public boolean containsKey (PageId key) {
    	return cache.containsKey(key);
    }
    
    /** if key exists, locate with hashMap, then move to head
     * 
     * @param key
     * @return
     */
    public synchronized Page get(PageId pId) {
    	Debug.log(LRUDebugLevel, "get page%d. in cache?%b 	%s", pId.pageNumber(), cache.containsKey(pId), Debug.stackTrace());
        ListNode node = cache.get(pId);
        if (node == null) {
            return null;
        }
        moveToHead(node);
        return node.page;
    }
    
    
    /** if the element associated with key exists, update the value
     * 
     * @param key
     * @param value
     * @throws DbException 
     */
    public synchronized void put(PageId pId, Page page) throws DbException {
    	if (pId == null || page == null) {
    		Debug.log(LRUDebugLevel, "put null pid/page in cache %s", Debug.stackTrace());
    		return;
    	}
    	Debug.log(LRUDebugLevel, "put page%d in cache 	%s", pId.pageNumber(), Debug.stackTrace());
        ListNode node = cache.get(pId);
        if (node == null) {
            // if key does not exist, create new node
            ListNode newNode = new ListNode(pId, page);
            
            if (size == capacity) {
            	// if full, try to delete tail from linked list
                ListNode tail = getTail();
                
                // (if the removed page is dirty, flush to disk) when Steal policy
                // NO STEAL dirty pages cannot be flushed until the txn completed
                if (evictionPolicy == LRUCache.NOSTEAL) {
	                int numDitryPages = 0;
	                while (tail.page.isDirty() && numDitryPages < capacity) {
	                	numDitryPages++;
	                	moveToHead(tail);
	                	tail = getTail();
	                }
	                Debug.log(LRUDebugLevel, "full cache, evict %d: dirty=%d / cap=%d", 
	                		tail.pId.pageNumber(), numDitryPages, capacity);
	                // full of dirty pages, will be stuck here
	                if (numDitryPages >= capacity) {
	                	throw new DbException("all pages in buffer are dirty");
	                }
                } 
                else { // STEAL Policy
                	// can flush page to disk before txn commit
                	if (tail.page.isDirty()) {
                		try {
							flushPage(tail.page.getId());
						} catch (IOException e) {
							e.printStackTrace();
						}
                	}
                }
                
                // delete the last accessed node from cache
            	//System.out.println("size=" + size + " capacity=" + capacity);
                //System.out.println("remove page for " + tableName + " page #" + tail.key.pageNumber());
                remove(tail.pId);
                --size;
            }
            
            cache.put(pId, newNode);
            addToHead(newNode);
            ++size;
//            if (size > capacity) {
//            	// if full, delete tail from linked list
//                ListNode tail = getTail();
//                // if the removed page is dirty, flush to disk
//                // but in No Steal, dirty pages cannot be flushed until the txn completed
//                int numDitryPages = 0;
//                while (tail.page.isDirty() && numDitryPages < capacity) {
//                	numDitryPages++;
//                	moveToHead(tail);
//                	tail = getTail();
//                }
//            	
//                // full of dirty pages, stuck here
//                if (numDitryPages >= capacity) {
//                	throw new DbException("all pages in buffer are dirty");
//                }
//                
//                // delete the last accessed node from cache
//            	//System.out.println("size=" + size + " capacity=" + capacity);
//                //System.out.println("remove page for " + tableName + " page #" + tail.key.pageNumber());
//                cache.remove(tail.pId);
//                --size;
//            }
        }
        else {
            // if key exists, update the value and move to head
            node.page = page;
            moveToHead(node);
        }
        Debug.log(LRUDebugLevel, "after put page%d now in cache: %s	%s",
        		pId.pageNumber(), cacheToString(), Debug.stackTrace());
    }

    /** Remove the element associated with key pid, without write to disk or check dirty
     * 
     * @param pid
     */
    public synchronized ListNode remove(PageId pid) {
    	ListNode removedNode = cache.remove(pid);
    	if (removedNode == null) {
    		return null;
    	}
    	removeNode(removedNode);
    	return removedNode;
    }
    
    private String cacheToString() {
    	StringBuilder sb = new StringBuilder('\n');
    	sb.append("[dummy:"+dummyHead+"] ");
    	ListNode cur = dummyHead;
    	while (cur != null) {
    		sb.append(cur);
    		cur = cur.next;
    		if (cur != null) {
    			sb.append("->");
    		}
    	}
    	sb.append(" [dummy:"+dummyTail+"] ");
    	return sb.toString();
    }
    
    public void printCache() {
    	Debug.log(LRUDebugLevel, cacheToString());
    }
    
    private synchronized void addToHead(ListNode node) {
        node.prev = dummyHead;
        node.next = dummyHead.next;
        dummyHead.next.prev = node;
        dummyHead.next = node;
    }

    private synchronized void removeNode(ListNode node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }

    private synchronized void moveToHead(ListNode node) {
        removeNode(node);
        addToHead(node);
    }

    private synchronized ListNode removeTail() {
        ListNode res = dummyTail.prev;
        removeNode(res);
        return res;
    }
    
    private ListNode getTail() {
        return dummyTail.prev;
    }
    
    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
    	if (pid == null || !cache.containsKey(pid)) {
    		String containsOrNot = pid == null ? "contains" : "does not contain";
    		throw new IllegalArgumentException("cache" + containsOrNot + " (PageId:"+pid+")");
    	}
    	
    	Debug.log("flush Page in LRU %s %s\n", pid.toString(), Debug.stackTrace());
    	
    	DbFile tableFile = Database.getCatalog().getDbFile(pid.getTableId());
    	Page flushedPage = cache.get(pid).page;
    	
    	/**
    	 * Ref: https://courses.cs.washington.edu/courses/cse444/15sp/labs/lab5/lab5.html
    	 * UW CSE444 Lab5 1.Started
         * Add UW's supplement codes for log and recovery
         */
    	// append an update record to the log, with
    	// a before-image and after-image.
    	TransactionId dirtier = flushedPage.getDirtier();
    	if (dirtier != null){ // what if ditier is not tid ???
	    	Database.getLogFile().logWrite(dirtier, flushedPage.getBeforeImage(), flushedPage);
	    	Database.getLogFile().force();
    	}
    	/**
         * UW's supplement codes for log and recovery end
         */
    	
    	
    	// flushedPage may be null when pid is not in pageMap (i.e. LRU cache)
    	tableFile.writePage(flushedPage);
    	flushedPage.markDirty(false, flushedPage.getDirtier());
    }
    
    public Iterator<PageId> keyIterator() {
    	return new keyIterator();
    }
    
    private class keyIterator implements Iterator<PageId>{
    	private ListNode curNode = dummyHead.next;
    	
		@Override
		public boolean hasNext() {
			return curNode.pId != null;
		}

		@Override
		public PageId next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			PageId resPageId = curNode.pId;
			curNode = curNode.next;
			return resPageId;
		}
    }
}