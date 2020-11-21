package deerBase;

import java.util.*;

import deerBase.Database;
import deerBase.TupleDesc.TDItem;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;
    
    private TransactionId tid;
    private int tableId;
    private String tableAlias;
    private DbFileIterator tupleItr;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableId
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableId, String tableAlias) {
        // some code goes here
    	this.tid = tid;
    	this.tableId = tableId;
    	this.tableAlias = tableAlias;
    	this.tupleItr = Database.getCatalog().getDbFile(tableId).iterator(tid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableId);
    }
    
    /**
     * @return Return the alias of the table this operator scans. 
     * */
    public String getAlias() {
        return this.tableAlias;
    }

    /**
     * Reset the tableId, and tableAlias of this operator.
     * @param tableId
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     */
    public void reset(int tableId, String tableAlias) {
    	this.tableId = tableId;
    	this.tableAlias = tableAlias;
    }

    // in the case without tableAlias, the tableAlias is just the tableId
    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        tupleItr.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * 
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc tupleDesc = Database.getCatalog().getTupleDesc(tableId);
        Iterator<TDItem> filedItr = tupleDesc.iterator();
        
        int numFields = tupleDesc.numFields();
        Type[] typeAr = new Type[numFields];
        String[] fieldNameAr = new String[numFields];
        
        /** 
         *  in this class is not responsible for handling a case where
         *  tableAlias or fieldName is null. If they are, the resulting name
         *  can be null.fieldName tableAlias.null, or null.null.
         */
        String prefix = (tableAlias == null) ? "null" : tableAlias;
        
        TDItem tdItem;
        String fieldName;
        int i = 0;
        while (filedItr.hasNext()) {
        	tdItem = filedItr.next();
        	typeAr[i] = tdItem.fieldType;
        	fieldName = (tdItem.fieldName == null) ? "null" : tdItem.fieldName;
        	fieldNameAr[i] = prefix + fieldName;
        	i++;
        }
        
        return new TupleDesc(typeAr, fieldNameAr);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
    	return tupleItr.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
    	return tupleItr.next();
    }

    public void close() {
        tupleItr.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        tupleItr.rewind();
    }
}
