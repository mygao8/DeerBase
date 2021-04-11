package deerBase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

// ref: 
public class LRUCache {
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
    }

    private Map<PageId, ListNode> cache = new HashMap<PageId, ListNode>();
    private int size;
    private int capacity;
    private ListNode head, tail;

    public LRUCache(int capacity) {
        this.size = 0;
        this.capacity = capacity;
        // fake head and tail
        head = new ListNode();
        tail = new ListNode();
        head.next = tail;
        tail.prev = head;
    }

    public boolean containsKey (PageId key) {
    	return cache.containsKey(key);
    }
    
    /** if key exists, locate with hashMap, then move to head
     * 
     * @param key
     * @return
     */
    public Page get(PageId key) {
        ListNode node = cache.get(key);
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
    public void put(PageId pId, Page page) throws DbException {
        ListNode node = cache.get(pId);
        if (node == null) {
            // if key does not exist, create new node
            ListNode newNode = new ListNode(pId, page);
            cache.put(pId, newNode);
            addToHead(newNode);
            ++size;
            if (size > capacity) {
            	// if full, delete tail from linked list
                ListNode tail = getTail();
                // if the removed page is dirty, flush to disk
                // but in No Steal, dirty pages cannot be flushed until the txn completed
                int numDitryPages = 0;
                while (tail.page.isDirty() && numDitryPages < capacity) {
                	numDitryPages++;
                	moveToHead(tail);
                	tail = getTail();
                }
            	
                // full of dirty pages, stuck here
                if (numDitryPages >= capacity) {
                	throw new DbException("all pages in buffer are dirty");
                }
                
                // delete the last accessed node from cache
            	//System.out.println("size=" + size + " capacity=" + capacity);
                //System.out.println("remove page for " + tableName + " page #" + tail.key.pageNumber());
                cache.remove(tail.pId);
                --size;
            }
        }
        else {
            // if key exists, update the value and move to head
            node.page = page;
            moveToHead(node);
        }
    }

    /** Remove the element associated with key pid, without write to disk or check dirty
     * 
     * @param pid
     */
    public ListNode remove(PageId pid) {
    	ListNode removedNode = cache.remove(pid);
    	if (removedNode == null) {
    		return null;
    	}
    	removeNode(removedNode);
    	return removedNode;
    }
    
    private void addToHead(ListNode node) {
        node.prev = head;
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
    }

    private void removeNode(ListNode node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }

    private void moveToHead(ListNode node) {
        removeNode(node);
        addToHead(node);
    }

    private ListNode removeTail() {
        ListNode res = tail.prev;
        removeNode(res);
        return res;
    }
    
    private ListNode getTail() {
        return res = tail.prev;
    }
    
    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
    	DbFile tableFile = Database.getCatalog().getDbFile(pid.getTableId());
    	String tableName = Database.getCatalog().getTableName(tableFile.getTableId());
    	//System.out.println("flush page for " + tableName + " page #" + pid.pageNumber());
    	Page flushedPage = cache.get(pid).page;
    	tableFile.writePage(flushedPage);
    	flushedPage.markDirty(false, null);
    }
    
    public Iterator<PageId> keyIterator() {
    	return new keyIterator();
    }
    
    private class keyIterator implements Iterator<PageId>{
    	private ListNode curNode = head.next;
    	
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