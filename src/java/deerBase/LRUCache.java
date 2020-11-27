package deerBase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public class LRUCache {
    class DLinkedNode {
    	PageId key;
        Page value;
        DLinkedNode prev;
        DLinkedNode next;
        public DLinkedNode() {}
        public DLinkedNode(PageId _key, Page _value) {key = _key; value = _value;}
    }

    private Map<PageId, DLinkedNode> cache = new HashMap<PageId, DLinkedNode>();
    private int size;
    private int capacity;
    private DLinkedNode head, tail;

    public LRUCache(int capacity) {
        this.size = 0;
        this.capacity = capacity;
        // fake head and tail
        head = new DLinkedNode();
        tail = new DLinkedNode();
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
        DLinkedNode node = cache.get(key);
        if (node == null) {
            return null;
        }
        moveToHead(node);
        return node.value;
    }
    
    
    /** if the element associated with key exists, update the value
     * 
     * @param key
     * @param value
     */
    public void put(PageId key, Page value) {
        DLinkedNode node = cache.get(key);
        if (node == null) {
            // if key does not exist, create new node
            DLinkedNode newNode = new DLinkedNode(key, value);
            cache.put(key, newNode);
            addToHead(newNode);
            ++size;
            if (size > capacity) {
                // if full, delete tail from linked list
                DLinkedNode tail = removeTail();
                // if the removed page is dirty, flush to disk
                if (tail.value.isDirty()) {
                	try {
                		flushPage(tail.key);
					} catch (IOException e) {
						e.printStackTrace();
					}
                }
                // delete the last accessed node from cache
                cache.remove(tail.key);
                --size;
            }
        }
        else {
            // if key exists, update the value and move to head
            node.value = value;
            moveToHead(node);
        }
    }

    /** Remove the element associated with key pid, without write to disk or check dirty
     * 
     * @param pid
     */
    public DLinkedNode remove(PageId pid) {
    	DLinkedNode removedNode = cache.remove(pid);
    	if (removedNode == null) {
    		return null;
    	}
    	removeNode(removedNode);
    	return removedNode;
    }
    
    private void addToHead(DLinkedNode node) {
        node.prev = head;
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
    }

    private void removeNode(DLinkedNode node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }

    private void moveToHead(DLinkedNode node) {
        removeNode(node);
        addToHead(node);
    }

    private DLinkedNode removeTail() {
        DLinkedNode res = tail.prev;
        removeNode(res);
        return res;
    }
    
    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
    	DbFile tableFile = Database.getCatalog().getDbFile(pid.getTableId());
    	Page flushedPage = cache.get(pid).value;
    	tableFile.writePage(flushedPage);
    	flushedPage.markDirty(false, null);
    }
    
    public Iterator<PageId> keyIterator() {
    	return new keyIterator();
    }
    
    private class keyIterator implements Iterator<PageId>{
    	private DLinkedNode curNode = head.next;
    	
		@Override
		public boolean hasNext() {
			return curNode.key != null;
		}

		@Override
		public PageId next() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			PageId resPageId = curNode.key;
			curNode = curNode.next;
			return resPageId;
		}
    }
}