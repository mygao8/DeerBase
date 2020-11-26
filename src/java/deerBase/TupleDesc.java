package deerBase;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private static final long serialVersionUID = 1L;
    
	private int numFields;
	private TDItem[] TDItemAr;
	
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        Type fieldType;
        
        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return new TDItemItr();
    }

    private class TDItemItr implements Iterator<TDItem> {
    	int cur = 0;
    	
    	public boolean hasNext() {
    		return cur < numFields;
    	}  
    	
    	public TDItem next() {
    		try {
    			TDItem res = TDItemAr[cur];
    			cur++;
    			return res;
    		} catch (IndexOutOfBoundsException e) {
    			throw new NoSuchElementException();
    		}
    	}
    }    

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldNameAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldNameAr) {
		if (typeAr.length == 0) {
		    throw new IllegalArgumentException("typeAr must contain at least one entry.");
		}
		if (typeAr.length != fieldNameAr.length) {
		    throw new IllegalArgumentException("the length of typeAr must be the same as that of fieldAr");
		}
		
    	this.numFields = typeAr.length;
    	this.TDItemAr = new TDItem[this.numFields];
    	
    	for (int i = 0; i < this.numFields; i++) {
    		TDItemAr[i] = new TDItem(typeAr[i], fieldNameAr[i]);
    	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
    	this(typeAr, new String[typeAr.length]);
    }
    
    /**
     * private constructor only used for merging two TupleDesc objects
     */
    private TupleDesc(int numFields, TDItem[] TDItemAr) {
    	if (TDItemAr == null || TDItemAr.length == 0) {
    		throw new IllegalArgumentException("TupleDesc must contain at least one entry.");
    	}
    	this.TDItemAr = TDItemAr;
    	this.numFields = numFields;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.numFields;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
    	if (i < 0 || i >= this.numFields) {
    		throw new NoSuchElementException();
    	}
        return TDItemAr[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
    	if (i < 0 || i >= this.numFields) {
    		throw new NoSuchElementException();
    	}
        return TDItemAr[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        if (name == null) {
            throw new NoSuchElementException();
        }
        String fieldName;
        for (int i = 0; i < TDItemAr.length; i++) {
            if ((fieldName = TDItemAr[i].fieldName) != null && fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException(name);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
    	int totalSize = 0;
        for (TDItem item : TDItemAr) {
        	totalSize += item.fieldType.getLen();
        }
        return totalSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
    	int totalNumFields = td1.numFields + td2.numFields;
    	TDItem[] mergedAr = new TDItem[td1.TDItemAr.length + td2.TDItemAr.length];
    	
    	System.arraycopy(td1.TDItemAr, 0, mergedAr, 0, td1.TDItemAr.length);
    	System.arraycopy(td2.TDItemAr, 0, mergedAr, td1.TDItemAr.length, td2.TDItemAr.length);
    	
    	return new TupleDesc(totalNumFields, mergedAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
    	if (this == o) {
    		return true;
    	}
    	if (o instanceof TupleDesc) {
    		TupleDesc td = (TupleDesc) o;
    		if (td.getSize() != this.getSize() || td.numFields != this.numFields) {
    			return false;
    		}
    		// these two tbs have equal number of fields here
    		for (int i = 0; i < this.numFields; i++) {
    			 if (td.TDItemAr[i].fieldType != this.TDItemAr[i].fieldType) {
    				 return false;
    			 }
    		}
    		return true;
    	}
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
    	StringBuffer str = new StringBuffer();
    	for (TDItem item : this.TDItemAr) {
    		str.append(item.toString());
    		str.append(", ");
    	}
        return str.toString();
    }
}
