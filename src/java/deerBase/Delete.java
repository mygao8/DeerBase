package deerBase;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private DbIterator child;
    private TupleDesc td;
    private boolean hasCalledFectchNxt;
    
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId tid, DbIterator child) {
    	this.tid = tid;
    	this.child = child;
        this.td = new TupleDesc(
    			new Type[] {Type.INT_TYPE},
    			new String[]{"#deletedRecords"}
    		);
        this.hasCalledFectchNxt = false;
    }

    public TupleDesc getTupleDesc() {
    	return td;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if (hasCalledFectchNxt) {
    		return null;
    	}    	
    	hasCalledFectchNxt = true;
    	
    	int count = 0;
    	while (child.hasNext()) {
    		Database.getBufferPool().deleteTuple(tid, child.next());
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
