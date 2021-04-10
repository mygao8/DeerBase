package deerBase;

import java.io.*;
import java.util.*;

import deerBase.BufferPool;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see deerBase.HeapPage#HeapPage
 */
public class HeapFile extends DbFile {

	private static final long serialVersionUID = 1L;
	
	private TupleDesc td;
	private int tableId;
	
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
    	super(f, td);
    	this.td = td;
    	this.tableId = getFileId();
    }



    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * Each table is represented by one HeapFIle, so use tableId as HeapFIle id
     * to identify the corresponding HeapFile
     * 
     * @return an ID uniquely identifying this HeapFile, also the tableId
     */
    public int getId() {
        return this.tableId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }


    /**
    * Inserts the specified tuple to the file on behalf of transaction.
    * This method will acquire a lock on the affected pages of the file, and
    * may block until the lock can be acquired.
    *
    * @param tid The transaction performing the update
    * @param t The tuple to add.  This tuple should be updated to reflect that
    *          it is now stored in this file.
    * @return An ArrayList contain the pages that were modified
    * @throws DbException if the tuple cannot be added
    * @throws IOException if the needed file can't be read/written
    */
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, TransactionAbortedException {
    	ArrayList<Page> resPages = new ArrayList<Page>();
    	
    	int pageNo = 0;
    	for (pageNo = 0; pageNo < getNumPages(); pageNo++) {
    		if (!isFullPage(pageNo)) {
    			HeapPage heapPage  = (HeapPage) Database.getBufferPool()
    					.getPage(tid, new HeapPageId(tableId, pageNo), Permissions.READ_WRITE);
    			heapPage.insertTuple(t);
    			heapPage.markDirty(true, tid);
    			resPages.add(heapPage);
    			break;
    		}
    	}
    	
    	if (pageNo == getNumPages()) {
    		// add new page
    		HeapPage newPage = (HeapPage) Database.getBufferPool()
					.getPage(tid, new HeapPageId(tableId, pageNo), Permissions.READ_WRITE);
    		setNumPages(getNumPages() + 1);
    		setNotFullPagesList(pageNo, false);
    		newPage.insertTuple(t);
    		newPage.markDirty(true, tid);
    		resPages.add(newPage);
    	}
    	
    	return resPages;
    }

    /**
    * Removes the specified tuple from the file on behalf of the specified
    * transaction.
    * This method will acquire a lock on the affected pages of the file, and
    * may block until the lock can be acquired.
    *
    * @throws DbException if the tuple cannot be deleted or is not a member
    *   of the file
    */
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
    	ArrayList<Page> resPages = new ArrayList<Page>();
    	PageId pageId = t.getRecordId().getPageId();
    	HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
    	heapPage.deleteTuple(t);
    	heapPage.markDirty(true, tid);
    	setNotFullPagesList(pageId.pageNumber(), false);
    	resPages.add(heapPage);
    	return resPages;
    }

    /**
     * Returns an iterator over all the tuples stored in this DbFile. The
     * iterator must use {@link BufferPool#getPage}, rather than
     * {@link #readPage} to iterate through the pages.
     *
     * @return an iterator over all the tuples stored in this DbFile.
     */
    @Override
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileItr(tid);
    }
    
    private class HeapFileItr implements DbFileIterator {
		private static final long serialVersionUID = 1L;

		private TransactionId tid;
    	
    	private int curPgNo;
    	private Iterator<Tuple> pageItr = null;
    	
    	public HeapFileItr(TransactionId tid) {
			this.tid = tid;
		}
    	
    	@Override
        public void open() throws DbException, TransactionAbortedException {
    		this.curPgNo = 0;
    		this.setCurPageItr(this.curPgNo);
    	}
                    	
    	@Override
    	public boolean hasNext() throws DbException, TransactionAbortedException {
    		if (this.pageItr == null) {
    			// HeapFileItr is not open yet
    			return false;
    		}
    		
    		if (this.pageItr.hasNext()) {
    			return true;
    		} else {
    			// curPage is done, try to load the next page
    			this.curPgNo += 1;
        		if (this.curPgNo < getNumPages()) {
        			this.setCurPageItr(this.curPgNo);
        			return this.pageItr.hasNext();
        		} else {
        			return false;
        		}    	
    		}
    	}
    	
    	@Override
    	public Tuple next()
    	        throws DbException, TransactionAbortedException, NoSuchElementException {
    		if (this.hasNext()) {
    			return this.pageItr.next();
    		} else {
    			throw new NoSuchElementException();
    		}
    	}
    	
    	// set the pageItr based on curPgNo and tableId
    	private void setCurPageItr(int pgNo) 
    			throws DbException, TransactionAbortedException {
    		PageId pId = (PageId) new HeapPageId(tableId, pgNo);
    		HeapPage curPage = (HeapPage) Database.getBufferPool().
    				getPage(tid, pId, Permissions.READ_ONLY);
    		this.pageItr = curPage.iterator();
    	}
    	
    	@Override
    	public void rewind() throws DbException, TransactionAbortedException {
    		open();
    	}
    	
    	@Override
    	public void close() {
    		this.curPgNo = 0;
    		this.pageItr = null;
    	}
    }

}

