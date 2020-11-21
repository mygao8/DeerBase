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
 * @author Sam Madden
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
    	super(f);
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


    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
    }



    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
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

