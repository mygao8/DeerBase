package deerBase;

import java.io.Serializable;

/**
 * IndexPredicate compares a field which has index on it against a given value
 * @see deerBase.IndexDbIterator
 */
public class IndexPredicate implements Serializable {
	
    private static final long serialVersionUID = 1L;
	
    private Predicate.Op op;
    private Field fieldValue;

    /**
     * Constructor.
     *
     * @param fvalue The value that the predicate compares against.
     * @param op The operation to apply (as defined in Predicate.Op); either
     *   Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN, Predicate.Op.EQUAL,
     *   Predicate.Op.GREATER_THAN_OR_EQ, or Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public IndexPredicate(Predicate.Op op, Field fvalue) {
        this.op = op;
        this.fieldValue = fvalue;
    }

    public Field getField() {
        return fieldValue;
    }

    public Predicate.Op getOp() {
        return op;
    }

    /** Return true if the fieldValue in the supplied predicate
        is satisfied by this predicate's fieldValue and
        operator.
        @param ipd The field to compare against.
    */
    public boolean equals(IndexPredicate ipd) {
        if (ipd == null)
            return false;
        return (op.equals(ipd.op) && fieldValue.equals(ipd.fieldValue));
    }

}
