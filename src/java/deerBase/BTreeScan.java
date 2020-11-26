package deerBase;

import java.util.*;

/**
 * BTreeScan is an operator which reads tuples in sorted order 
 * according to a predicate
 */
public class BTreeScan implements DbIterator {

	private static final long serialVersionUID = 1L;

	private boolean isOpen = false;
	private TransactionId tid;
	private TupleDesc td;
	private IndexPredicate idxPred = null;
	private transient DbFileIterator it;
	private String tablename;
	private String alias;

	/**
	 * Creates a B+ tree scan over the specified table as a part of the
	 * specified transaction.
	 * 
	 * @param tid
	 *            The transaction this scan is running as a part of.
	 * @param tableid
	 *            the table to scan.
	 * @param tableAlias
	 *            the alias of this table (needed by the parser); the returned
	 *            tupleDesc should have fields with name tableAlias.fieldName
	 *            (note: this class is not responsible for handling a case where
	 *            tableAlias or fieldName are null. It shouldn't crash if they
	 *            are, but the resulting name can be null.fieldName,
	 *            tableAlias.null, or null.null).
	 * @param idxPred
	 * 			  The index predicate to match. If null, the scan will return all tuples
	 *            in sorted order
	 */
	public BTreeScan(TransactionId tid, int tableid, String tableAlias, IndexPredicate idxPred) {
		this.tid = tid;
		this.idxPred = idxPred;
		reset(tableid, tableAlias);
	}

	public BTreeScan(TransactionId tid, int tableid, IndexPredicate idxPred) {
		this(tid, tableid, Database.getCatalog().getTableName(tableid), idxPred);
	}
	
	/**
	 * @return
	 *       return the table name of the table the operator scans. This should
	 *       be the actual name of the table in the catalog of the database
	 * */
	public String getTableName() {
		return this.tablename;
	}

	/**
	 * @return Return the alias of the table this operator scans. 
	 * */
	public String getAlias()
	{
		return this.alias;
	}

	/**
	 * Reset the tableid, and tableAlias of this operator.
	 * @param tableid
	 *            the table to scan.
	 * @param tableAlias
	 *            the alias of this table (needed by the parser); the returned
	 *            tupleDesc should have fields with name tableAlias.fieldName
	 *            (note: this class is not responsible for handling a case where
	 *            tableAlias or fieldName are null. It shouldn't crash if they
	 *            are, but the resulting name can be null.fieldName,
	 *            tableAlias.null, or null.null).
	 */
	public void reset(int tableid, String tableAlias) {
		this.isOpen=false;
		this.alias = tableAlias;
		this.tablename = Database.getCatalog().getTableName(tableid);
		if(idxPred == null) {
			this.it = Database.getCatalog().getDbFile(tableid).iterator(tid);
		}
		else {
			this.it = ((BTreeFile) Database.getCatalog().getDbFile(tableid)).indexIterator(tid, idxPred);
		}
		td = Database.getCatalog().getTupleDesc(tableid);
		String[] newNames = new String[td.numFields()];
		Type[] newTypes = new Type[td.numFields()];
		for (int i = 0; i < td.numFields(); i++) {
			String name = td.getFieldName(i);
			Type t = td.getFieldType(i);

			newNames[i] = tableAlias + "." + name;
			newTypes[i] = t;
		}
		td = new TupleDesc(newTypes, newNames);
	}

	public void open() throws DbException, TransactionAbortedException {
		if (isOpen)
			throw new DbException("double open on one DbIterator.");

		it.open();
		isOpen = true;
	}

	/**
	 * Returns the TupleDesc with field names from the underlying BTreeFile,
	 * prefixed with the tableAlias string from the constructor. This prefix
	 * becomes useful when joining tables containing a field(s) with the same
	 * name.
	 * 
	 * @return the TupleDesc with field names from the underlying BTreeFile,
	 *         prefixed with the tableAlias string from the constructor.
	 */
	public TupleDesc getTupleDesc() {
		return td;
	}

	public boolean hasNext() throws TransactionAbortedException, DbException {
		if (!isOpen)
			throw new IllegalStateException("iterator is closed");
		return it.hasNext();
	}

	public Tuple next() throws NoSuchElementException,
	TransactionAbortedException, DbException {
		if (!isOpen)
			throw new IllegalStateException("iterator is closed");

		return it.next();
	}

	public void close() {
		it.close();
		isOpen = false;
	}

	public void rewind() throws DbException, NoSuchElementException,
	TransactionAbortedException {
		close();
		open();
	}
}
