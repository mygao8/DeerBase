
package deerBase;

import java.util.*;
import java.io.*;

/**
 * The abstract class for database files on disk. Each table is represented by
 * a single DbFile. DbFiles can fetch pages and iterate through tuples. Each
 * file has a unique id used to store metadata about the table in the Catalog.
 * DbFiles are generally accessed through the buffer pool, rather than directly
 * by operators.
 */
public abstract class DbFile implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private File f;
	private final TupleDesc td;
	private int numPages;
	private final int fileId;
	private ArrayList<Byte> notFullPages;
	
    /**
     * Constructs a database file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public DbFile(File f, TupleDesc td) {
    	this.td = td;
		this.f = f;
    	this.numPages = (int) (f.length() / BufferPool.getPageSize());
    	this.fileId = f.getAbsoluteFile().hashCode();
    	this.notFullPages = new ArrayList<Byte>(Collections.nCopies(numPages/8 + 1, (byte) 0));
    } 
	
    // only used for unit test, SkeletonFile
    public DbFile (int tableId) {
    	this.td = null;
		this.fileId = tableId;
    	this.notFullPages = new ArrayList<Byte>(Arrays.asList((byte) 0));
	}
    
    /**
     * Read the specified page from disk.
     *
     * @throws IllegalArgumentException if the page does not exist in this file.
     */
    public Page readPage(PageId pid) throws IllegalArgumentException {
    	if (pid == null) {
    		throw new IllegalArgumentException();
    	}
    	
    	Page resPage = null;
    	try (RandomAccessFile adFile = new RandomAccessFile(f, "r")) {
    		byte[] buf = new byte[BufferPool.getPageSize()];
    		int pos = pid.pageNumber() * BufferPool.getPageSize();
    		
    		adFile.seek(pos);
    		adFile.read(buf);
    		
    		resPage = new HeapPage((HeapPageId)pid, buf);
    	} catch (Exception e) {
			e.printStackTrace();
		}
    	
        return resPage;
    }

    /**
     * Push the specified page to disk.
     *
     * @param p The page to write.  page.getId().pageno() specifies the offset into the file where the page should be written.
     * @throws IOException if the write fails
     *
     */
    public void writePage(Page p) throws IOException {
    	if (p == null) {
    		throw new IllegalArgumentException();
    	}
    	
    	try (RandomAccessFile adFile = new RandomAccessFile(f, "rw")) {
    		byte[] buf = p.getPageData();
    		int pos = p.getId().pageNumber() * BufferPool.getPageSize();
    		
    		adFile.seek(pos);
    		adFile.read(buf);
    	} catch (Exception e) {
			e.printStackTrace();
		}
    }

    
    /**
     * Returns the File backing this DbFile on disk.
     */
    public File getFile() {
        return this.f;
    }
    
    /**
     * Returns the number of pages in this DbFile.
     */
    public int getNumPages() {
    	return this.numPages;
    }
    
    public int setNumPages(int numPages) {
    	return this.numPages = numPages;
    }
    
    /**
     * Returns a unique ID used to identify this DbFile in the Catalog. This id
     * can be used to look up the table via {@link Catalog#getDbFile} and
     * {@link Catalog#getTupleDesc} (in the case this DbFile is a HeapFile or 
     * SkeletonFile, i.e. fileId=tableId).
     * <p>
     *
     * @return an ID uniquely identifying this DbFile.
     */
    public int getFileId() {
    	return this.fileId;
    }
    
    public int getTableId() {
    	return this.fileId;
    }
    
    public ArrayList<Byte> getNotFullPagesList() {
		return this.notFullPages;
	}
    
    public void setNotFullPagesList(int pageIdx, boolean isFull) {
    	int slotIdx = pageIdx/8;
    	byte mask = (byte) (1 << (pageIdx%8));
    	
    	if (slotIdx > notFullPages.size()) {
    		notFullPages.add((byte) 0);
    	}
    	
    	if (isFull == true) {
    		notFullPages.set(slotIdx, (byte) (notFullPages.get(slotIdx) | mask));
    	} else {
    		notFullPages.set(slotIdx, (byte) (notFullPages.get(slotIdx) & ~mask));
    	}
	}
    
    /**
     * Returns true if associated page on this file is full.
     */
    public boolean isFullPage (int pageIdx) {
    	// if the first 18 pages are used, notFullPages looks like [11111111, 11111111, 00000011, ...]
    	int slotIdx = pageIdx/8;

    	if (((notFullPages.get(slotIdx) >>> (pageIdx%8)) & 0x01) == 1) {
    		return true;
    	} else {
    		return false;
    	}
    }
    

	/**
	 * Returns the TupleDesc of the table stored in this DbFile.
	 * 
	 * @return TupleDesc of this DbFile.
	 */
	public TupleDesc getTupleDesc() {
		return td;
	}
	
    
//    /**
//     * Inserts the specified tuple to the file on behalf of transaction.
//     * This method will acquire a lock on the affected pages of the file, and
//     * may block until the lock can be acquired.
//     *
//     * @param tid The transaction performing the update
//     * @param t The tuple to add.  This tuple should be updated to reflect that
//     *          it is now stored in this file.
//     * @return An ArrayList contain the pages that were modified
//     * @throws DbException if the tuple cannot be added
//     * @throws IOException if the needed file can't be read/written
//     */
//    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
//        throws DbException, IOException, TransactionAbortedException;
//
//    /**
//     * Removes the specified tuple from the file on behalf of the specified
//     * transaction.
//     * This method will acquire a lock on the affected pages of the file, and
//     * may block until the lock can be acquired.
//     *
//     * @throws DbException if the tuple cannot be deleted or is not a member
//     *   of the file
//     */
//    public Page deleteTuple(TransactionId tid, Tuple t)
//        throws DbException, TransactionAbortedException;

    /**
     * Returns an iterator over all the tuples stored in this DbFile. The
     * iterator must use {@link BufferPool#getPage}, rather than
     * {@link #readPage} to iterate through the pages.
     *
     * @return an iterator over all the tuples stored in this DbFile.
     */
    public abstract DbFileIterator iterator(TransactionId tid);

    /**
	 * Insert a tuple into this DbFile, keeping the tuples in sorted order. 
	 * May cause pages to split if the page where tuple t belongs is full.
	 * 
	 * @param tid - the transaction id
	 * @param t - the tuple to insert
	 * @return a list of all pages that were dirtied by this operation. Could include
	 * many pages since parent pointers will need to be updated when an internal node splits.
	 * @see #splitLeafPage(TransactionId, HashMap, BTreeLeafPage, Field)
	 */
	public abstract ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException;
    
//    /**
//     * Returns the TupleDesc of the table stored in this DbFile.
//     * @return TupleDesc of this DbFile.
//     */
//    public TupleDesc getTupleDesc();
	
}