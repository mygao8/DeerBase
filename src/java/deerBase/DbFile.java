
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
	private int numPages;
	private int fileId;
	
    /**
     * Constructs a database file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public DbFile(File f) {
    	this.f = f;
    	this.numPages = (int) (f.length() / BufferPool.PAGE_SIZE);
    	this.fileId = f.getAbsoluteFile().hashCode();
    }
	
    // only used for unit test, SkeletonFile
    public DbFile (int tableId) {
    	this.fileId = tableId;
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
    		byte[] buf = new byte[BufferPool.PAGE_SIZE];
    		int pos = pid.pageNumber() * BufferPool.PAGE_SIZE;
    		
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
    	// not implemented yet
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
    
//    /**
//     * Returns the TupleDesc of the table stored in this DbFile.
//     * @return TupleDesc of this DbFile.
//     */
//    public TupleDesc getTupleDesc();
}
