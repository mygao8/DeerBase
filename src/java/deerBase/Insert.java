package deerBase;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private DbIterator child;
    private int tableId;
    private TupleDesc td;
    private boolean hasCalledFectchNxt;
    
    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId tid,DbIterator child, int tableId)
            throws DbException {
    	this.tid = tid;
    	this.child = child;
        this.tableId = tableId;
        this.td = new TupleDesc(
    			new Type[] {Type.INT_TYPE},
    			new String[]{"#insertedRecords"}
    		);
        this.hasCalledFectchNxt = false;
    }

    public TupleDesc getTupleDesc() {
    	return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. 
     * Note that insert DOES NOT need check to see if a particular tuple is
     * a duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @throws IOException 
     * @throws NoSuchElementException 
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException, NoSuchElementException {
    	if (hasCalledFectchNxt) {
    		return null;
    	}    	
    	hasCalledFectchNxt = true;
    	
    	int count = 0;
    	while (child.hasNext()) {
    		Database.getBufferPool().insertTuple(tid, tableId, child.next());
    		count++;
    	}
    	
        Tuple resTuple = new Tuple(td);
    	resTuple.setField(0, new IntField(count));
    	return resTuple;
    }

    @Override
    public DbIterator[] getChildren() {
    	return new DbIterator[] { this.child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
		if (this.child!=children[0]) {
		    this.child = children[0];
		}
	}
}
