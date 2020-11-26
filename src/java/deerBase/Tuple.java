package deerBase;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import deerBase.TupleDesc.TDItem;
import deerBase.Field;
import deerBase.RecordId;
import deerBase.TupleDesc;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    private TupleDesc tupleDesc;
    
    private Field[] fields;

    private RecordId recordId;
    
    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
    	this.tupleDesc = td;
    	this.fields = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return this.recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
    	this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
    	if (i < 0 || i >= this.fields.length) {
    		throw new IllegalArgumentException("index out of bound");
    	}
    	this.fields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int fieldIdx) {
        // some code goes here
    	if (fieldIdx < 0 || fieldIdx >= this.fields.length) {
    		throw new IllegalArgumentException("index out of bound");
    	}
        return this.fields[fieldIdx];
    }

    /**
     * Returns the contents of this Tuple as a string as follows:
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     */
    public String toString() {
        // some code goes here
    	StringBuffer rowStr = new StringBuffer();
    	for (int i = 0; i < this.fields.length - 1; i++) {
    		rowStr.append(this.fields[i]);
    		rowStr.append('\t');
    	}
    	rowStr.append(this.fields[this.fields.length-1]);
    	rowStr.append('\n');
    	
    	return rowStr.toString();
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return new FieldItr();
    }
    
    private class FieldItr implements Iterator<Field> {
    	int cur = 0;
    	
    	public boolean hasNext() {
    		return cur < fields.length;
    	}
    	
    	public Field next() {
    		try {
    			Field resField = fields[cur];
    			cur++;
    			return resField;
    		} catch (IndexOutOfBoundsException e) {
    			throw new NoSuchElementException();
    		}
    	}
    }
}
