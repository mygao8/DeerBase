
package deerBase;

import java.io.*;
import java.util.*;

import javax.imageio.IIOException;

import java.lang.reflect.*;

/**
LogFile implements the recovery subsystem of deerbase.  This class is
able to write different log records as needed, but it is the
responsibility of the caller to ensure that write ahead logging and
two-phase locking discipline are followed.  <p>

<u> Locking note: </u>
<p>

Many of the methods here are synchronized (to prevent concurrent log
writes from happening); many of the methods in BufferPool are also
synchronized (for similar reasons.)  Problem is that BufferPool writes
log records (on page flushed) and the log file flushes BufferPool
pages (on checkpoints and recovery.)  This can lead to deadlock.  For
that reason, any LogFile operation that needs to access the BufferPool
must not be declared synchronized and must begin with a block like:

<p>
<pre>
    synchronized (Database.getBufferPool()) {
       synchronized (this) {

       ..

       }
    }
</pre>
*/

/**
<p> The format of the log file is as follows:

<ul>

<li> The first long integer of the file represents the offset of the
last written checkpoint, or -1 if there are no checkpoints

<li> All additional data in the log consists of log records.  Log
records are variable length.

<li> Each log record begins with an integer type and a long integer
transaction id.

<li> Each log record ends with a long integer file offset representing
the position in the log file where the record began.

<li> There are five record types: ABORT, COMMIT, UPDATE, BEGIN, and
CHECKPOINT

<li> ABORT, COMMIT, and BEGIN records contain no additional data

<li>UPDATE RECORDS consist of two entries, a before image and an
after image.  These images are serialized Page objects, and can be
accessed with the LogFile.readPageData() and LogFile.writePageData()
methods.  See LogFile.print() for an example.

<li> CHECKPOINT records consist of active transactions at the time
the checkpoint was taken and their first log record on disk.  The format
of the record is an integer count of the number of transactions, as well
as a long integer transaction id and a long integer first record offset
for each active transaction.

</ul>

*/

public class LogFile {

	private final static int LogFileDebugLevel = Debug.DEER_BASE;
	
    File logFile;
    RandomAccessFile raf;
    Boolean recoveryUndecided; // no call to recover() and no append to log

    static final int ABORT_RECORD = 1;
    static final int COMMIT_RECORD = 2;
    static final int UPDATE_RECORD = 3;
    static final int BEGIN_RECORD = 4;
    static final int CHECKPOINT_RECORD = 5;
    static final long NO_CHECKPOINT_ID = -1;

    static final int INT_SIZE = 4;
    static final int LONG_SIZE = 8;
    
    // beginning of the first record, skip the first long of checkpoint offset
    static final int FIRST_RECORD_OFFSET = LONG_SIZE;
    
    // beginning of each record: typeInt, tidLong
    static final int RECORD_BEGIN_SIZE = INT_SIZE + LONG_SIZE;
    // end of each record: startOffsetLong
    static final int RECORD_END_SIZE = LONG_SIZE;
    
    // size of records with fixed length type
    static final int BEGIN_SIZE = RECORD_BEGIN_SIZE + RECORD_END_SIZE;
    static final int COMMIT_SIZE = RECORD_BEGIN_SIZE + RECORD_END_SIZE;
    static final int ABORT_SIZE = RECORD_BEGIN_SIZE + RECORD_END_SIZE;
    
    long currentOffset = -1;
    int pageSize;
    int totalRecords = 0; // for PatchTest

    HashMap<Long,Long> tidToFirstLogRecord = new HashMap<Long,Long>();

    /** Constructor.
        Initialize and back the log file with the specified file.
        We're not sure yet whether the caller is creating a brand new DB,
        in which case we should ignore the log file, or whether the caller
        will eventually want to recover (after populating the Catalog).
        So we make this decision lazily: if someone calls recover(), then
        do it, while if someone starts adding log file entries, then first
        throw out the initial log file contents.

        @param f The log file's name
    */
    public LogFile(File f) throws IOException {
    	this.logFile = f;
        raf = new RandomAccessFile(f, "rw");
        recoveryUndecided = true;

        // install shutdown hook to force cleanup on close
        // Runtime.getRuntime().addShutdownHook(new Thread() {
                // public void run() { shutdown(); }
            // });

        //XXX WARNING -- there is nothing that verifies that the specified
        // log file actually corresponds to the current catalog.
        // This could cause problems since we log tableids, which may or
        // may not match tableids in the current catalog.
    }

    // we're about to append a log record. if we weren't sure whether the
    // DB wants to do recovery, we're sure now -- it didn't. So truncate
    // the log.
    void preAppend() throws IOException {
        totalRecords++;
        if(recoveryUndecided){
            recoveryUndecided = false;
            raf.seek(0);
            raf.setLength(0);
            raf.writeLong(NO_CHECKPOINT_ID);
            raf.seek(raf.length());
            currentOffset = raf.getFilePointer();
        }
    }

    public int getTotalRecords() {
        return totalRecords;
    }
    
    /** Write an abort record to the log for the specified tid, force
        the log to disk, and perform a rollback
        @param tid The aborting transaction.
    */
    public void logAbort(TransactionId tid) throws IOException {
        // must have buffer pool lock before proceeding, since this
        // calls rollback

        synchronized (Database.getBufferPool()) {

            synchronized(this) {
                preAppend();
                //Debug.log(LogFileDebugLevel, "ABORT");
                //should we verify that this is a live transaction?

                // must do this here, since rollback only works for
                // live transactions (needs tidToFirstLogRecord)
                rollback(tid);

                raf.writeInt(ABORT_RECORD);
                raf.writeLong(tid.getId());
                raf.writeLong(currentOffset);
                currentOffset = raf.getFilePointer();
                force(raf);
                tidToFirstLogRecord.remove(tid.getId());
                
                // print log
                Debug.log(LogFileDebugLevel, "after rollback, abort " + tid.getId());
                print(5);
            }
        }
    }

    /** Write a commit record to disk for the specified tid,
        and force the log to disk.

        @param tid The committing transaction.
    */
    public synchronized void logCommit(TransactionId tid) throws IOException {
        preAppend();
        Debug.log(LogFileDebugLevel, "COMMIT " + tid.getId());
        //should we verify that this is a live transaction?

        raf.writeInt(COMMIT_RECORD);
        raf.writeLong(tid.getId());
        raf.writeLong(currentOffset);
        currentOffset = raf.getFilePointer();
        force(raf);
        tidToFirstLogRecord.remove(tid.getId());
        
        // print log
        Debug.log(LogFileDebugLevel, "after commit" + tid.getId());
        print(5);
    }

    /** Write an UPDATE record to disk for the specified tid and page
        (with provided before and after images.)
        @param tid The transaction performing the write
        @param before The before image of the page
        @param after The after image of the page

        @see deerbase.Page#getBeforeImage
    */
    public synchronized void logWrite(TransactionId tid, Page before,
                                       Page after)
        throws IOException  {
        Debug.log(LogFileDebugLevel, "WRITE, offset = " + raf.getFilePointer());
        preAppend();
        /* update record conists of

           record type
           transaction id
           before page data (see writePageData)
           after page data
           start offset
        */
        raf.writeInt(UPDATE_RECORD);
        raf.writeLong(tid.getId());

        writePageData(raf,before);
        writePageData(raf,after);
        raf.writeLong(currentOffset);
        currentOffset = raf.getFilePointer();
        
        
        Debug.log(LogFileDebugLevel, "UPDATE: txn%d, before:%d [%s] dirtied by txn%d, after:%d \n", 
        		tid.getId(), before.hashCode(), ((HeapPage)after).oldData.toString(), tid.getId() , after.hashCode());
        
//        if (tid.getId() == 1) {
//        	Debug.log(LogFileDebugLevel, "BeforeImage:\n %s \n", ((HeapPage)before).toString(20));
//        	Debug.log(LogFileDebugLevel, "AfterImage:\n %s \n", ((HeapPage)after).toString(20));
//        }
        
        
        Debug.log(LogFileDebugLevel, "WRITE OFFSET = " + currentOffset);
    }

    void writePageData(RandomAccessFile raf, Page p) throws IOException{
        PageId pid = p.getId();
        int pageInfo[] = pid.serialize();

        //page data is:
        // page class name
        // id class name
        // id class bytes
        // id class data
        // page class bytes
        // page class data

        String pageClassName = p.getClass().getName();
        String idClassName = pid.getClass().getName();

        raf.writeUTF(pageClassName);
        raf.writeUTF(idClassName);

        raf.writeInt(pageInfo.length);
        for (int i = 0; i < pageInfo.length; i++) {
            raf.writeInt(pageInfo[i]);
        }
        byte[] pageData = p.getPageData();
        raf.writeInt(pageData.length);
        raf.write(pageData);
        //        Debug.log ("WROTE PAGE DATA, CLASS = " + pageClassName + ", table = " +  pid.getTableId() + ", page = " + pid.pageno());
    }

    Page readPageData(RandomAccessFile raf) throws IOException {
        PageId pid;
        Page newPage = null;

        String pageClassName = raf.readUTF();
        String idClassName = raf.readUTF();
        Debug.log(LogFileDebugLevel, "readPageData: pageClassName %s, idClassName %s\n%s", 
        		pageClassName, idClassName, Debug.stackTrace());
        Debug.log(LogFileDebugLevel, "pageClassName.len=%d, ==deerBase.HeapPage? %b. idClassName.len=%d", 
        		pageClassName.length(), "deerBase.HeapPage".equals(pageClassName), idClassName.length());
        
        try {
            Class<?> idClass = Class.forName(idClassName);
            Class<?> pageClass = Class.forName(pageClassName);

            Constructor<?>[] idConsts = idClass.getDeclaredConstructors();
            int numIdArgs = raf.readInt();
            Object idArgs[] = new Object[numIdArgs];
            for (int i = 0; i<numIdArgs;i++) {
                idArgs[i] = new Integer(raf.readInt());
            }
            pid = (PageId)idConsts[0].newInstance(idArgs);

            Constructor<?>[] pageConsts = pageClass.getDeclaredConstructors();
            int pageSize = raf.readInt();

            byte[] pageData = new byte[pageSize];
            raf.read(pageData); //read before image

            Object[] pageArgs = new Object[2];
            pageArgs[0] = pid;
            pageArgs[1] = pageData;

            newPage = (Page)pageConsts[0].newInstance(pageArgs);

            //            Debug.log(LogFileDebugLevel, "READ PAGE OF TYPE " + pageClassName + ", table = " + newPage.getId().getTableId() + ", page = " + newPage.getId().pageno());
        } catch (ClassNotFoundException e){
            e.printStackTrace();
            throw new IOException();
        } catch (InstantiationException e) {
            e.printStackTrace();
            throw new IOException();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            throw new IOException();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
            throw new IOException();
        }
        return newPage;

    }

    /** Write a BEGIN record for the specified transaction
        @param tid The transaction that is beginning

    */
    public synchronized  void logXactionBegin(TransactionId tid)
        throws IOException {
        Debug.log(LogFileDebugLevel, "BEGIN");
        if(tidToFirstLogRecord.get(tid.getId()) != null){
            System.err.printf("logXactionBegin: already began this tid\n");
            throw new IOException("double logXactionBegin()");
        }
        preAppend();
        raf.writeInt(BEGIN_RECORD);
        raf.writeLong(tid.getId());
        raf.writeLong(currentOffset);
        Debug.log(LogFileDebugLevel, "put in map: txn%d, offset:%d", tid.getId(), currentOffset);
        tidToFirstLogRecord.put(tid.getId(), currentOffset);
        currentOffset = raf.getFilePointer();

        Debug.log(LogFileDebugLevel, "BEGIN OFFSET = " + currentOffset);
    }

    /** Checkpoint the log and write a checkpoint record. */
    public void logCheckpoint() throws IOException {
        //make sure we have buffer pool lock before proceeding
        synchronized (Database.getBufferPool()) {
            synchronized (this) {
                //Debug.log(LogFileDebugLevel, "CHECKPOINT, offset = " + raf.getFilePointer());
                preAppend();
                long startCpOffset, endCpOffset;
                Set<Long> keys = tidToFirstLogRecord.keySet();
                Iterator<Long> els = keys.iterator();
                force(raf);
                Database.getBufferPool().flushAllPages();
                startCpOffset = raf.getFilePointer();
                raf.writeInt(CHECKPOINT_RECORD);
                raf.writeLong(-1); //no tid , but leave space for convenience

                //write list of outstanding transactions
                raf.writeInt(keys.size());
                while (els.hasNext()) {
                    Long key = els.next();
                    Debug.log(LogFileDebugLevel, "WRITING CHECKPOINT TRANSACTION ID: " + key);
                    raf.writeLong(key);
                    //Debug.log(LogFileDebugLevel, "WRITING CHECKPOINT TRANSACTION OFFSET: " + tidToFirstLogRecord.get(key));
                    raf.writeLong(tidToFirstLogRecord.get(key));
                }

                //once the CP is written, make sure the CP location at the
                // beginning of the log file is updated
                endCpOffset = raf.getFilePointer();
                raf.seek(0);
                raf.writeLong(startCpOffset);
                raf.seek(endCpOffset);
                raf.writeLong(currentOffset);
                currentOffset = raf.getFilePointer();
                //Debug.log(LogFileDebugLevel, "CP OFFSET = " + currentOffset);
            }
        }

        logTruncate();
    }

    // 1. find checkpoint
    // 2. find the min offset in all outstanding txns in checkpoint and the offset of checkpoint
    // 3. discard all logs before the min offset from step 2
    // 4. rewrite
    /** Truncate any unneeded portion of the log to reduce its space
        consumption */
    public synchronized void logTruncate() throws IOException {
    	Debug.log(LogFileDebugLevel, "before rewrite");
    	print(5);
        preAppend();
        raf.seek(0);
        long cpLoc = raf.readLong();

        long minLogRecord = cpLoc;

        if (cpLoc != -1L) {
            raf.seek(cpLoc);
            int cpType = raf.readInt();
            @SuppressWarnings("unused")
            long cpTid = raf.readLong();

            if (cpType != CHECKPOINT_RECORD) {
                throw new RuntimeException("Checkpoint pointer does not point to checkpoint record");
            }

            int numOutstanding = raf.readInt();

            for (int i = 0; i < numOutstanding; i++) {
                @SuppressWarnings("unused")
                long tid = raf.readLong();
                long firstLogRecord = raf.readLong();
                if (firstLogRecord < minLogRecord) {
                    minLogRecord = firstLogRecord;
                }
            }
        }

        // we can truncate everything before minLogRecord
        File newFile = new File("logtmp" + System.currentTimeMillis());
        RandomAccessFile logNew = new RandomAccessFile(newFile, "rw");
        logNew.seek(0);
        logNew.writeLong((cpLoc - minLogRecord) + LONG_SIZE);

        raf.seek(minLogRecord);

        //have to rewrite log records since offsets are different after truncation
        while (true) {
            try {
                int type = raf.readInt();
                long record_tid = raf.readLong();
                long newStart = logNew.getFilePointer();

                Debug.log(LogFileDebugLevel, "NEW START = " + newStart);

                logNew.writeInt(type);
                logNew.writeLong(record_tid);

                switch (type) {
                case UPDATE_RECORD:
                    Page before = readPageData(raf);
                    Page after = readPageData(raf);

                    writePageData(logNew, before);
                    writePageData(logNew, after);
                    break;
                case CHECKPOINT_RECORD:
                    int numXactions = raf.readInt();
                    logNew.writeInt(numXactions);
                    while (numXactions-- > 0) {
                        long xid = raf.readLong();
                        long xoffset = raf.readLong();
                        logNew.writeLong(xid);
                        logNew.writeLong((xoffset - minLogRecord) + LONG_SIZE);
                    }
                    break;
                case BEGIN_RECORD:
                	Debug.log(LogFileDebugLevel, "Rewrite: put in map txn%d, offset%d", record_tid, newStart);
                    tidToFirstLogRecord.put(record_tid,newStart);
                    break;
                }

                //all xactions finish with a pointer
                logNew.writeLong(newStart);
                raf.readLong();

            } catch (EOFException e) {
                break;
            }
        }
        
        //force(logNew);
        
        Debug.log(LogFileDebugLevel, "TRUNCATING LOG;  WAS " + raf.length() + " BYTES ; NEW START : " + minLogRecord + " NEW LENGTH: " + (raf.length() - minLogRecord));

        force(raf);
        raf.close();
        
//        try {
//        	java.nio.file.Files.delete(logFile.toPath());
//        } catch (Exception e) {
//        	e.printStackTrace();
//        }
//        
//        if (!logFile.delete()) {
//        	throw new IOException("Failed to delete old log in rewrite");
//        }
//     
//        if (!newFile.renameTo(logFile)) {
//        	throw new IOException("Failed to rename tmp log in rewrite");
//        }
        
        raf = new RandomAccessFile(logFile, "rw");
        raf.seek(raf.length());
        newFile.delete();

        currentOffset = raf.getFilePointer();
        
        Debug.log(LogFileDebugLevel, "after rewrite");
        print(5);
    }

    /** Rollback the specified transaction, setting the state of any
        of pages it updated to their pre-updated state.  To preserve
        transaction semantics, this should not be called on
        transactions that have already committed (though this may not
        be enforced by this method.)

        @param tid The transaction to rollback
    */
    public void rollback(TransactionId tid)
        throws NoSuchElementException, IOException {
        synchronized (Database.getBufferPool()) {
            synchronized(this) {
                preAppend();
                // some code goes here
                
                // Log Record:
                // typeInt, tidLong
                // 
                // UPDATE: before image, after image
                // image: pageUTF, pidUTF (utfLenShort, [byte, byte, ..])
                // pageInfoLenInt, [int, int, ..]
                // pageDataLenInt, [byte, byte, ..]
                // 
                // CHECKPOINT: numTxnsInt, [(tidLong, 1stOffsetLong)]
                // 
                // startOffsetLong
                
                // start from the begin record of tid
                long beginOffset = tidToFirstLogRecord.get(tid.getId());              
                // skip begin record
                raf.seek(beginOffset);              
                Debug.log(LogFileDebugLevel, "Roolback: set file ptr for txn%d, offset:%d, ptr:%d\n", 
                		tid.getId(), beginOffset, raf.getFilePointer());                
                
                Debug.log(LogFileDebugLevel, "before rollback");
                print(5);
                
                int numUpdate = 0;
                while (true) {
                	int type;
                	long tidLong;
                	long offset;
                	try {
                		 offset = raf.getFilePointer();
                		 if (offset >= raf.length()) break;
                		 
                    	 type = raf.readInt();
                    	 tidLong = raf.readLong();
                    	 Debug.log(LogFileDebugLevel, "Rollback: read log [offset%d type:%d, tid%d]\n", 
                    			 offset, type, tidLong);
                	}
                    catch (EOFException e) {                
                    	Debug.log(LogFileDebugLevel, "Reach EOF when roll back txn%d\n", tid.getId());
                    	break;
    				}
                	
	                if (tidLong == tid.getId() && type == UPDATE_RECORD) {
	                	// undo for Update record
	                	numUpdate++;
	                	
						Page beforeImage = readPageData(raf);
						// skip after image
						Page afterImage = readPageData(raf);
						long tmpOffset = raf.readLong();
						
						int tableId = beforeImage.getId().getTableId();
						DbFile dbFile = Database.getCatalog().getDbFile(tableId);
						if (numUpdate == 1) {
							Debug.log(LogFileDebugLevel, "Rollback: undo update with page%s, log (Offset %d: UPDATE [tid%d]\n)", 
									beforeImage.getId().toString(), tmpOffset, tidLong);
							dbFile.writePage(beforeImage);
							Database.getBufferPool().discardPage(beforeImage.getId());
						}

						Debug.log(LogFileDebugLevel, "Rollback: \n");
						Debug.log(LogFileDebugLevel, "Before image:\n%s\n", ((HeapPage) beforeImage).toString(5));
						Debug.log(LogFileDebugLevel, "After image:\n%s\n", ((HeapPage) afterImage).toString(5));
						
						// restore raf pointer??
						Database.getBufferPool().discardPage(beforeImage.getId());
						tidToFirstLogRecord.remove(tid.getId());
						break;	
	                }
	                else {
	                	// skip according to record type
	                	int bytesSkipped;
	                	switch (type) {
						case BEGIN_RECORD:
							bytesSkipped = raf.skipBytes(RECORD_END_SIZE);
							if (bytesSkipped != RECORD_END_SIZE) {
								throw new IIOException("EOF: " + raf.getFilePointer());
							}
							break;
						case COMMIT_RECORD:
							bytesSkipped = raf.skipBytes(RECORD_END_SIZE);
							if (bytesSkipped != RECORD_END_SIZE) {
								throw new IIOException("EOF: " + raf.getFilePointer());
							}
							break;
						case ABORT_RECORD:
							bytesSkipped = raf.skipBytes(RECORD_END_SIZE);
							if (bytesSkipped != RECORD_END_SIZE) {
								throw new IIOException("EOF: " + raf.getFilePointer());
							}
							break;
						case UPDATE_RECORD:
							// skip before image
							readPageData(raf);
							// skip after image
							readPageData(raf);
							
							bytesSkipped = raf.skipBytes(RECORD_END_SIZE);
							if (bytesSkipped != RECORD_END_SIZE) {
								throw new IIOException("EOF: " + raf.getFilePointer());
							}
							break;
						case CHECKPOINT_RECORD:
							// CHECKPOINT: numTxnsInt, [(tidLong, 1stOffsetLong)]
							
							int numTxns = raf.readInt();
							// outstanding txns (tidLong, 1stOffsetLong)
							int toSkip = (LONG_SIZE * 2) * numTxns;
							bytesSkipped = raf.skipBytes(toSkip);
							if (bytesSkipped != toSkip) {
								throw new IIOException("EOF: " + raf.getFilePointer());
							}
							
							bytesSkipped = raf.skipBytes(RECORD_END_SIZE);
							if (bytesSkipped != RECORD_END_SIZE) {
								throw new IIOException("EOF: " + raf.getFilePointer());
							}
							break;
						}
	                }
                }
                
                // don't forget to recover the offset for future write log
                raf.seek(currentOffset);
            }
        }
    }
    
    // return the size of image in Update record
    private long skipImage(RandomAccessFile raf) throws IOException {
    	// UPDATE: before image, after image
        // image: pageUTF, pidUTF (utfLenShort, [byte, byte, ..])
        // pageInfoLenInt, [int, int, ..]
        // pageDataLenInt, [byte, byte, ..]
    	
    	long startOffset = raf.getFilePointer();
    	
    	// read UTF
    	short pageUTFLen = raf.readShort();
    	raf.skipBytes(pageUTFLen);
    	short pidUTFLen = raf.readShort();
    	raf.skipBytes(pidUTFLen);
    	
    	// read pageInfo[], i.e. page constructor args[]
    	int pageInfoLen = raf.readInt();
    	raf.skipBytes(pageInfoLen * INT_SIZE);
    	
    	// read pageData[], i.e. the whole page content in bytes[]
    	int pageDataLen = raf.readInt();
    	raf.skipBytes(pageDataLen);
    	
    	long endOffset = raf.getFilePointer();
    	
    	return endOffset - startOffset;
    }
    
    /** Shutdown the logging system, writing out whatever state
        is necessary so that start up can happen quickly (without
        extensive recovery.)
    */
    public synchronized void shutdown() {
        try {
            logCheckpoint();  //simple way to shutdown is to write a checkpoint record
            raf.close();
        } catch (IOException e) {
            System.out.println("ERROR SHUTTING DOWN -- IGNORING.");
            e.printStackTrace();
        }
    }

    // NOTE: the before image of each update record of a txn will be the same
    // because the before image is set when the last txn committed
    // and during the current txn, if a update exists, current txn holds XLock
    // no other txns can update a page that current txn already updated
    /** Recover the database system by ensuring that the updates of
        committed transactions are installed and that the
        updates of uncommitted transactions are not installed.
    */
    public void recover() throws IOException {
        synchronized (Database.getBufferPool()) {
            synchronized (this) {
            	Debug.log(LogFileDebugLevel, "before recover");
            	print(5);
            	
                recoveryUndecided = false;
                // some code goes here
                
                final Long NO_UPDATE = -1L;
                final Long COMMIT = -2L;
                        
                // map tid to offset of the last update to redo. When encounter commit, redo directly
                // the remaining txns in this map  after reading the whole log are losers, undo them
                HashMap<Long, Long> latestUpdateMap = new HashMap<>();
                HashMap<Long, Long> outstandingTxnBeginOffsetMap = new HashMap<>();
                
                raf.seek(0);
                final long checkPointOffset = raf.readLong();
                
                if (checkPointOffset != -1) {
                	raf.seek(checkPointOffset);
                
	                // Log Record:
	                // typeInt, tidLong
	                // 
	                // UPDATE: before image, after image
	                // image: pageUTF, pidUTF (utfLenShort, [byte, byte, ..])
	                // pageInfoLenInt, [int, int, ..]
	                // pageDataLenInt, [byte, byte, ..]
	                // 
	                // CHECKPOINT: numTxnsInt, [(tidLong, 1stOffsetLong)]
	                // 
	                // startOffsetLong
                	
	                int type = raf.readInt();
	                long tid = raf.readLong();
	                long recordPos;
	                if (type != CHECKPOINT_RECORD || tid != -1) {
	                	Debug.log(LogFileDebugLevel, "Recover: checkPointOffset ERROR, Offset:%d [type%d, tid%d]", checkPointOffset, type, tid);
	                	return;
	                }	                
	                
	                // mark outstanding txns as losers initially
	                int numTxns = raf.readInt();
	                // outstanding txns (tidLong, beginOffsetLong)
					long[][] outstandingTxns = new long[numTxns][2];
					for (int i = 0; i < numTxns; i++) {
						outstandingTxns[i][0] = raf.readLong();
						outstandingTxns[i][1] = raf.readLong();
						
						outstandingTxnBeginOffsetMap.put(outstandingTxns[i][0], outstandingTxns[i][1]);
						// use NO_UPDATE to mark this is not the update record offset
						latestUpdateMap.put(outstandingTxns[i][0], NO_UPDATE);
					}
					
					recordPos = raf.readLong();
					Debug.log(LogFileDebugLevel, "Recover: Offset %d: CHECKPOINT  [tid%d]\n", recordPos, tid);
					for (int i = 0; i < numTxns; i++) {
						Debug.log(LogFileDebugLevel, "Recover: outstanding txn: [tid%d, offset:%d]\n", outstandingTxns[i][0], outstandingTxns[i][1]);
					}
                }
				
                while (true) {
                	int type;
                	long tid, recordPos;
                	try {
                		 recordPos = raf.getFilePointer();
                		 if (recordPos >= raf.length()) break;
                		 
                    	 type = raf.readInt();
                    	 tid = raf.readLong();
                    	 Debug.log(LogFileDebugLevel, "Recover: read log [offset%d type:%d, tid%d]\n", 
                    			 recordPos, type, tid);
                	}
                    catch (EOFException e) {                
                    	Debug.log(LogFileDebugLevel, "Reach EOF when recover\n");
                    	break;
    				}                	
                	
                	// skip according to record type
                	int bytesSkipped;
                	switch (type) {
					case BEGIN_RECORD:
						// No need to store. If only begin without update, no redo / undo.			
						bytesSkipped = raf.skipBytes(RECORD_END_SIZE);
						if (bytesSkipped != RECORD_END_SIZE) {
							throw new IIOException("EOF: " + raf.getFilePointer());
						}
						break;
					case COMMIT_RECORD:
						// find a winner, redo
						Long latestUpdateOffset = latestUpdateMap.remove(tid);
						Debug.log(LogFileDebugLevel, "Recover: redo txn%d with update log [offset: %d]", tid, latestUpdateOffset);
						
						// NO_UPDATE: outstanding txn, no updates after checkpoint
						// may update before checkpoint, may not. need to mark
						if (latestUpdateOffset == NO_UPDATE) {
							latestUpdateMap.put(tid, COMMIT);
						}
						else if (latestUpdateOffset != null) { // null: txn starts after checkpoint, but no updates until commit
							long originOffset = raf.getFilePointer();
							
							// deal with latest UPDATE to redo
							raf.seek(latestUpdateOffset + RECORD_BEGIN_SIZE);
							Page beforeImage = readPageData(raf);
							Page afterImage = readPageData(raf);
							
							int tableId = afterImage.getId().getTableId();
							DbFile dbFile = Database.getCatalog().getDbFile(tableId);
							Debug.log(LogFileDebugLevel, "Recover: redo update with page%s, log (Offset %d: UPDATE [tid%d]), because encounter COMMIT after checkpoint", 
									afterImage.getId().toString(), latestUpdateOffset, tid);
							dbFile.writePage(afterImage);
	
							Debug.log(LogFileDebugLevel, "Recover: \n");
							Debug.log(LogFileDebugLevel, "Before image:\n%s\n", ((HeapPage) beforeImage).toString(5));
							Debug.log(LogFileDebugLevel, "After image:\n%s\n", ((HeapPage) afterImage).toString(5));
							// done redo
							
							// back to commit record, and skip record end
							raf.seek(originOffset);
						}
						
						
						bytesSkipped = raf.skipBytes(RECORD_END_SIZE);
						if (bytesSkipped != RECORD_END_SIZE) {
							throw new IIOException("EOF: " + raf.getFilePointer());
						}
						break;
					case ABORT_RECORD:
						// No need to undo. First rollback, then write ABORT log.
						// exist abort log, means already rollback
						outstandingTxnBeginOffsetMap.remove(tid);
						latestUpdateMap.remove(tid);
						
						bytesSkipped = raf.skipBytes(RECORD_END_SIZE);
						if (bytesSkipped != RECORD_END_SIZE) {
							throw new IIOException("EOF: " + raf.getFilePointer());
						}
						break;
					case UPDATE_RECORD:
						latestUpdateMap.put(tid, recordPos);
						// if a outstanding txn has update after checkpoint
						// no need to distinguish it with txns begin after checkpoint
						outstandingTxnBeginOffsetMap.remove(tid);
						
						// skip before image
						readPageData(raf);
						// skip after image
						readPageData(raf);
						
						bytesSkipped = raf.skipBytes(RECORD_END_SIZE);
						if (bytesSkipped != RECORD_END_SIZE) {
							throw new IIOException("EOF: " + raf.getFilePointer());
						}
						break;
					case CHECKPOINT_RECORD:
						// Should be impossible, we started from the last checkpoint
						Debug.log(LogFileDebugLevel, "Recover: ERROR, find newer checkpoint");
						// CHECKPOINT: numTxnsInt, [(tidLong, 1stOffsetLong)]
						
						int numTxns = raf.readInt();
						// outstanding txns (tidLong, 1stOffsetLong)
						int toSkip = (LONG_SIZE * 2) * numTxns;
						bytesSkipped = raf.skipBytes(toSkip);
						if (bytesSkipped != toSkip) {
							throw new IIOException("EOF: " + raf.getFilePointer());
						}
						
						bytesSkipped = raf.skipBytes(RECORD_END_SIZE);
						if (bytesSkipped != RECORD_END_SIZE) {
							throw new IIOException("EOF: " + raf.getFilePointer());
						}
						break;
					}
                }
                
                
                // check outstanding txns at first
                // case1: not in map --update after checkpoint and commit. done, no need to concern
                // case2: NO_UPDATE --no commit, no update after checkpoint, backward from checkpoint
                //	case2.1: update before checkpoint ? 
                //				undo with any beforeImage : already reach begin, no updates, discard
                // case3: COMMIT --commit, no update after checkpoint, not sure update before checkpoint, backward from checkpoint
                // 	case3.1: update before checkpoint ? 
                //				redo with latest afterImage before checkpoint : already reach begin, no updates, discard
                // case4: other offsets --update after checkpoint, same as txns begin after checkpoint now
                
                // how about no checkpoint??
            	// backward read from checkpoint
                long curRecordOffset = checkPointOffset;
                while (outstandingTxnBeginOffsetMap.size() > 0) {                	
                	raf.seek(curRecordOffset-LONG_SIZE);
                	// beginning of current record
                	curRecordOffset = raf.readLong();
                	if (curRecordOffset < FIRST_RECORD_OFFSET) {
                		Debug.log(LogFileDebugLevel, "Recover ERROR: reach the begining of print log, but there is still outstanding txns");
                		break;
                	}
                	
                	raf.seek(curRecordOffset);
                	
                	int type;
                	long tid;
                 	try {
                     	 type = raf.readInt();
                     	 tid = raf.readLong();
                 	}
                    catch (EOFException e) {                
        	         	Debug.log(LogFileDebugLevel, "Reach EOF when print log\n");
        	         	break;
        			}
                 	
                	// if tid in map, if BEGIN, remove
                	// if UPDATE. check COMMIT: redo or NO_UPDATE: undo
                 	if (outstandingTxnBeginOffsetMap.containsKey(tid)) {
                 		if (type == BEGIN_RECORD) {
                 			outstandingTxnBeginOffsetMap.remove(tid);
                 			latestUpdateMap.remove(tid);
                 		} else if (type == UPDATE_RECORD) {
                 			outstandingTxnBeginOffsetMap.remove(tid);
                 			Long state = latestUpdateMap.remove(tid);
                 			
            				if (state == COMMIT) {
            					// redo with after image
                     			Page before = readPageData(raf);
                				Page afterImage = readPageData(raf);
                				
                				int tableId = afterImage.getId().getTableId();
    							DbFile dbFile = Database.getCatalog().getDbFile(tableId);
    							Debug.log(LogFileDebugLevel, "Recover: redo update with page%s, log (Offset %d: UPDATE [tid%d]), because encounter UPDATE when backforward", 
    									afterImage.getId().toString(), curRecordOffset, tid);
    							dbFile.writePage(afterImage);		
            				} else if (state == NO_UPDATE) {
            					// undo with before image
            					Page beforeImage = readPageData(raf);
                				
                				int tableId = beforeImage.getId().getTableId();
    							DbFile dbFile = Database.getCatalog().getDbFile(tableId);
    							Debug.log(LogFileDebugLevel, "Recover: undo update with page%s, log (Offset %d: UPDATE [tid%d]), because encounter UPDATE when backforward", 
    									beforeImage.getId().toString(), curRecordOffset, tid);
    							dbFile.writePage(beforeImage);
            				} else {
            					Debug.log(LogFileDebugLevel, "Recover ERROR: oustandingTxn with update after checkpoint is not removed from map");
            				}
                 		} else {
                 			Debug.log(LogFileDebugLevel, "Recover ERROR: backforward from checkpoint, outstanding txn log (Offset %d: COMMIT/ABORT [tid%d]",
                 					curRecordOffset, tid);
                 		}
                 	}
                }
                
                // finally, undo the remaining txns.
                // all txns in map: with latest update, update but not commit after checkpoint
                latestUpdateMap.forEach((tid, latestUpdateOffset) -> {
                	try {
                		Debug.log(LogFileDebugLevel, "\n");
						Debug.log(LogFileDebugLevel, "Recover: undo update with log (Offset %d: UPDATE [tid%d]), this txn updates after checkpoint", 
								latestUpdateOffset, tid);
						
                		raf.seek(latestUpdateOffset + RECORD_BEGIN_SIZE);
	                	Page beforeImage = readPageData(raf);
	    				
	    				int tableId = beforeImage.getId().getTableId();
						DbFile dbFile = Database.getCatalog().getDbFile(tableId);

						dbFile.writePage(beforeImage);
                	} catch (IOException e) {
						// TODO: handle exception
                		e.printStackTrace();
					}
                });
                
                
                // set current offset
                raf.seek(raf.length());
                currentOffset = raf.getFilePointer();
                force(raf);
            }
         }
    }

    /** Print out a human readable representation of the log */
    public void print(int imageLen) throws IOException {
        // some code goes here
    	
    	long originOffset = raf.getFilePointer();
    	raf.seek(0);
    	
    	Debug.log(LogFileDebugLevel, "\n======Print LOG======\n");
    	Debug.log(LogFileDebugLevel, "checkPoint Offset: %d\n", raf.readLong());
    	
    	// print log records
        while (raf.getFilePointer() < raf.length()) {
         	int type;
         	long tidLong;
         	long recordPos;
         	try {
             	 type = raf.readInt();
             	 tidLong = raf.readLong();
         	}
            catch (EOFException e) {                
	         	Debug.log(LogFileDebugLevel, "Reach EOF when print log\n");
	         	break;
			}
         	
        	 
         	// skip according to record type
         	switch (type) {
			case BEGIN_RECORD:
				recordPos = raf.readLong();
				Debug.log(LogFileDebugLevel, "Offset %d: BEGIN  [tid%d]\n", recordPos, tidLong);
				break;
			case COMMIT_RECORD:
				recordPos = raf.readLong();
				Debug.log(LogFileDebugLevel, "Offset %d: COMMIT [tid%d]\n", recordPos, tidLong);
				break;
			case ABORT_RECORD:
				recordPos = raf.readLong();
				Debug.log(LogFileDebugLevel, "Offset %d: ABORT  [tid%d]\n", recordPos, tidLong);
				break;
			case UPDATE_RECORD:
				Page before = readPageData(raf);
				Page after = readPageData(raf);
				
				recordPos = raf.readLong();
				Debug.log(LogFileDebugLevel, "Offset %d: UPDATE [tid%d]\n", recordPos, tidLong);
				
				Debug.log(LogFileDebugLevel, "Before image\n%s", ((HeapPage) before).toString(imageLen));
				Debug.log(LogFileDebugLevel, "After image\n%s", ((HeapPage) after).toString(imageLen));
				
				break;
			case CHECKPOINT_RECORD:
				// CHECKPOINT: numTxnsInt, [(tidLong, 1stOffsetLong)]
				
				int numTxns = raf.readInt();
				
				// outstanding txns (tidLong, 1stOffsetLong)
				long[][] txns = new long[numTxns][2];
				for (int i = 0; i < numTxns; i++) {
					txns[i][0] = raf.readLong();
					txns[i][1] = raf.readLong();
				}
				
				recordPos = raf.readLong();
				Debug.log(LogFileDebugLevel, "Offset %d: CHECKPOINT  [tid%d]\n", recordPos, tidLong);
				Debug.log(LogFileDebugLevel, "Txns:\n");
				for (int i = 0; i < numTxns; i++) {
					Debug.log(LogFileDebugLevel, "[tid%d, offset:%d]\n", txns[i][0], txns[i][1]);
				}
				break;
			}
         }
        
        raf.seek(originOffset);
        
        Debug.log(LogFileDebugLevel, "=====LOG END=====\n\n");
    }
    
    public synchronized void force() throws IOException {
        raf.getChannel().force(true);
    }
    
    public synchronized void force(RandomAccessFile raf) throws IOException {
        raf.getChannel().force(true);
    }

}
