package deerBase;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private Predicate predicate;
    private DbIterator child;
    private TupleDesc td;
    
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param predicate
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate predicate, DbIterator child) {
        this.predicate = predicate;
        this.child = child;
        this.td = child.getTupleDesc();
    }

    public Predicate getPredicate() {
    	return predicate;
    }

    public TupleDesc getTupleDesc() {
    	return td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        child.open();
        super.open();
    }

    public void close() {
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the {@link Predicate#filter(Tuple)} returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
    	while (child.hasNext()) {
    		Tuple t = child.next();
    		if (predicate.filter(t)) {
    			return t;
    		}
    	}
    	return null;
    }

    @Override
    public DbIterator[] getChildren() {
    	return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        if (child != children[0]) {
        	child = children[0];
        }
    }

}
