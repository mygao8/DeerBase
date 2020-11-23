package deerBase;

import java.io.Serializable;

import deerBase.Predicate.Op;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private int fieldIdx1;
    private int fieldIdx2;
    private Op op;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @param fieldIdx1
     *            The field index into the first tuple in the predicate
     * @param fieldIdx2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public JoinPredicate(int fieldIdx1, Predicate.Op op, int fieldIdx2) {
        this.fieldIdx1 = fieldIdx1;
        this.fieldIdx2 = fieldIdx2;
        this.op = op;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
    	return t1.getField(fieldIdx1).compare(op, t2.getField(fieldIdx2));
    }
    
    public int getFieldIdx1() {
    	return fieldIdx1;
    }
    
    public int getFieldIdx2() {
    	return fieldIdx2;
    }
    
    public Predicate.Op getOperator()
    {
    	return op;
    }
}
