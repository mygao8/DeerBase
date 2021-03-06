package deerBase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

import javax.sound.sampled.Line;

import com.google.common.hash.BloomFilter;


/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    private JoinPredicate p;
    private DbIterator child1;
    private DbIterator child2;
    private Tuple[] leftBuffer;
    private Tuple[] rightBuffer;
    private ArrayList<Tuple> tempTps;
    private Predicate.Op op;
    private File f1, f2;

    //131072 is the default buffer of mysql join operation
    private static final int BLOCKMEMORY = 131072 * 5;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        this.p = p;
        this.op = p.getOperator();
        this.child1 = child1;
        this.child2 = child2;
    }

    public JoinPredicate getJoinPredicate() {
        return p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        SeqScan scan = (SeqScan)child1;
        return scan.getTableName();
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        SeqScan scan = (SeqScan)child2;
        return scan.getAlias();
    }

    /**
     * @see deerbase.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc td1 = child1.getTupleDesc();
        TupleDesc td2 = child2.getTupleDesc();
        return TupleDesc.merge(td1, td2);
    }

    public void open() throws DbException, TransactionAbortedException, NoSuchElementException, IOException {
        super.open();
        child1.open();
        child2.open();
     
//        try {
//			tpIter = new tupleItr();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
       tpIter = getAllFetchNext();
    }

    public void close() {
        child1.close();
        child2.close();
        super.close();
        tpIter = null;
    }

    public void rewind() throws DbException, TransactionAbortedException, NoSuchElementException, IOException {
        child1.rewind();
        child2.rewind();
//        try {
//			tpIter = new tupleItr();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
        tpIter = getAllFetchNext();
    }
    
    private Iterator<Tuple> tpIter = null;
    
    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @throws IOException 
     * @throws NoSuchElementException 
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException, NoSuchElementException, IOException {    	
        if (tpIter == null) {
				//tpIter = new tupleItr();
        		tpIter = getAllFetchNext();

        	if (tpIter == null) {
             	return null;
        	}
        }
        
        Tuple tp = null;
        if (tpIter.hasNext()){
            tp = tpIter.next();
        }
        return tp;
    }
    
    private class tupleItr implements Iterator<Tuple> {
    	int offset1, offset2;
    	File f1, f2;
    	Tuple[] buffer1, buffer2;
    	int idx1, idx2;
    	Tuple next;
		JoinPredicate pEqual, pGreater, pLess;
    	
    	public tupleItr() throws IOException {      
			try {
				this.f1 = enternalSort(p.getFieldIdx1(), child1);
				this.f2 = enternalSort(p.getFieldIdx2(), child2);
			} catch (Exception e) {
				e.printStackTrace();
			}
			this.offset1 = 0; this.offset2 = 0;
			this.idx1 = 0; this.idx2 = 0;
			this.next = null;
			this.buffer1 = readSortedTuplesToBuffer(f1, offset1, child1.getTupleDesc());
			this.buffer2 = readSortedTuplesToBuffer(f2, offset2, child2.getTupleDesc());
			this.pEqual = new JoinPredicate(p.getFieldIdx1(), Predicate.Op.EQUALS, p.getFieldIdx2());
			this.pGreater = new JoinPredicate(p.getFieldIdx1(), Predicate.Op.EQUALS, p.getFieldIdx2());
			this.pLess = new JoinPredicate(p.getFieldIdx1(), Predicate.Op.EQUALS, p.getFieldIdx2());
    	}
    	
		@Override
		public boolean hasNext() {
			if (next == null) {
				try {
					next = getNext();
				} catch (IOException e) {
					e.printStackTrace();
				}
				if (next == null) {
					return false;
				}
			}
			return true;
		}

		@Override
		public Tuple next() {
			if (hasNext()) {
				Tuple tmp = next;
				next = null;
				return tmp;
			}
			return null;
		}
		
		private Tuple getNext() throws IOException {
			if (buffer1 == null || buffer2 == null) {
				return null;
			}
			if (idx1 >= buffer1.length) {
				idx1 = 0;
				// +2 for the new line character
				offset1 += (child1.getTupleDesc().getSize() + 2) * buffer1.length;
				buffer1 = readSortedTuplesToBuffer(f1, offset1, child1.getTupleDesc());
				if (buffer1 == null) {
					return null;
				}
			}
			if (idx2 >= buffer2.length) {
				idx2 = 0;
				// +2 for the new line character
				offset2 += (child2.getTupleDesc().getSize() + 2) * buffer2.length;
				// buffer not full??
				buffer2 = readSortedTuplesToBuffer(f2, offset2, child2.getTupleDesc());
				if (buffer2 == null) {
					return null;
				}
			}
			
			switch (op){
            case EQUALS:
                while (idx1 < buffer1.length && idx2 < buffer2.length) {
                	Tuple t1 = buffer1[idx1], t2 = buffer2[idx2];
                	if (pEqual.filter(t1, t2)) {
                		idx1++;
                		idx2++;
                		return mergeTuple(t1, t2);
                	}
                	else if (pGreater.filter(t1, t2)) {
                		idx2++;
                		if (idx2 >= buffer2.length) {
                			// read the following pages to fill buffer
                			idx2 = 0;
                			offset2 += (child2.getTupleDesc().getSize() + 2) * buffer2.length;
                			buffer2 = readSortedTuplesToBuffer(f2, offset2, child2.getTupleDesc());
                			if (buffer2 == null) {
                				return null;
                			}
                		}
                	}
                	else if (pLess.filter(t1, t2)) {
                		idx1++;
                		if (idx1 >= buffer1.length) {
                			// read the following pages to fill buffer
                			idx1 = 0;
                			offset1 += (child1.getTupleDesc().getSize() + 2) * buffer1.length;
                			buffer1 = readSortedTuplesToBuffer(f1, offset1, child1.getTupleDesc());
                			if (buffer1 == null) {
                				return null;
                			}
                		}
                	}
                }
                break;
                
            case GREATER_THAN:
            case GREATER_THAN_OR_EQ:
                while (idx1 < buffer1.length && idx2 < buffer2.length) {
                	Tuple t1 = buffer1[idx1], t2 = buffer2[idx2];

                	if (pGreater.filter(t1, t2)) {
                		idx2++;
                		if (idx2 >= buffer2.length) {
                			idx1++;
                			idx2 = 0;
                			
                			if (idx1 >= buffer1.length) {
                    			// reset to buffer begining, load next buffer2
                    			idx1 = 0;
                    			offset2 += (child2.getTupleDesc().getSize() + 2) * buffer2.length;
                    			buffer2 = readSortedTuplesToBuffer(f2, offset2, child2.getTupleDesc());
                    			
                				// the whole table2 is already iterated
                    			if (buffer2 == null) {                				
                    				// load next buffer1
                    				idx1 = 0;
                        			offset1 += (child1.getTupleDesc().getSize() + 2) * buffer1.length;
        	                		buffer1 = readSortedTuplesToBuffer(f1, offset1, child1.getTupleDesc());
        	                		if (buffer1 == null) {
        	                			return null;
        	                		}
        	                		// set table2 from begining
        	                		offset2 = 0;
        	                		buffer2 = readSortedTuplesToBuffer(f2, offset2, child2.getTupleDesc());
                        		
                    			}
                    		}
                		}
                		return mergeTuple(t1, t2);
                	}
                	else { // less or equal
                		idx1++;
                		idx2 = 0;
                		if (idx1 >= buffer1.length) {
                			// reset to buffer begining, load next buffer2
                			idx1 = 0;
                			offset2 += (child2.getTupleDesc().getSize() + 2) * buffer2.length;
                			buffer2 = readSortedTuplesToBuffer(f2, offset2, child2.getTupleDesc());
                			
            				// the whole table2 is already iterated
                			if (buffer2 == null) {                				
                				// load next buffer1
                				idx1 = 0;
                    			offset1 += (child1.getTupleDesc().getSize() + 2) * buffer1.length;
    	                		buffer1 = readSortedTuplesToBuffer(f1, offset1, child1.getTupleDesc());
    	                		if (buffer1 == null) {
    	                			return null;
    	                		}
    	                		// set table2 from begining
    	                		offset2 = 0;
    	                		buffer2 = readSortedTuplesToBuffer(f2, offset2, child2.getTupleDesc());
                    		
                			}
                		}
                	}
                }
                break;
                
            case LESS_THAN:
            case LESS_THAN_OR_EQ:
                while (idx1 < buffer1.length && idx2 < buffer2.length) {
                	Tuple t1 = buffer1[idx1], t2 = buffer2[idx2];
	
            		idx2++;
            		if (idx2 >= buffer2.length) {
            			idx1++;
            			idx2 = 0;
            			
            			if (idx1 >= buffer1.length) {
                			// reset to buffer begining, load next buffer2
                			idx1 = 0;
                			offset2 += (child2.getTupleDesc().getSize() + 2) * buffer2.length;
                			buffer2 = readSortedTuplesToBuffer(f2, offset2, child2.getTupleDesc());
                			
            				// the whole table2 is already iterated
                			if (buffer2 == null) {                				
                				// load next buffer1
                				idx1 = 0;
                    			offset1 += (child1.getTupleDesc().getSize() + 2) * buffer1.length;
    	                		buffer1 = readSortedTuplesToBuffer(f1, offset1, child1.getTupleDesc());
    	                		if (buffer1 == null) {
    	                			return null;
    	                		}
    	                		// set table2 from begining
    	                		offset2 = 0;
    	                		buffer2 = readSortedTuplesToBuffer(f2, offset2, child2.getTupleDesc());
                    		
                			}
                		}
            		}
                		
                    if (pLess.filter(t1, t2)) { 
                    	return mergeTuple(t1, t2);
                    }
                }
                
                break;
			}
			
			// should never reach
			return null;
		}
		
    }
    
    
    private Tuple[] readSortedTuplesToBuffer(File f, int offset, TupleDesc td) throws IOException {
//    	int bufferSize = BLOCKMEMORY / 
//    			(child1.getTupleDesc().getSize() + child2.getTupleDesc().getSize());    
//        Tuple[] buffer = new Tuple[bufferSize];
        if (offset > f.length()) {
        	return null;
        }
    	
        RandomAccessFile adFile = new RandomAccessFile(f, "r");
        adFile.seek(offset);
        int tupleSize = td.getSize();
        int numTuple = BLOCKMEMORY / tupleSize;
        byte[] buffer = new byte[tupleSize * numTuple];
        for (int i = 0; i < numTuple; i++) {
        	// eof??
        	String line;
        	if ((line = adFile.readLine()) != null) {
        		byte[] tmp = line.getBytes();
        		Debug.log(line + " " + td.toString());
        		Debug.log(tmp.length + "	" + tupleSize);
            	for (int j = 0; j < tupleSize; j++) {
            		buffer[i*tupleSize + j] = tmp[j];
            	}
        	}
        	else {
        		break;
        	}
        }
        adFile.close();
        
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(buffer));	
        
        Tuple[] resTuples = new Tuple[numTuple];
        Tuple t = new Tuple(td);
        try {
        	for (int i = 0; i < numTuple; i++) {
	            for (int j = 0; j < td.numFields(); j++) {
	                Field field = td.getFieldType(j).parse(dis);
	                t.setField(j, field);
	            }
	            resTuples[i] = t;
        	}
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }
        
        return resTuples;
    }

    

    private File enternalSort(int fieldIdx, DbIterator child) throws NoSuchElementException, DbException, TransactionAbortedException, IOException {
    	TuplesExternalSort exSort = new TuplesExternalSort(child.getTupleDesc(), fieldIdx);
        
        // use sorted-merge algorithm
        Tuple[] buffer = new Tuple[BLOCKMEMORY / child.getTupleDesc().getSize()];
        
        int numTuple = 0;
        while (child.hasNext()){
            buffer[numTuple++] = child.next();
            Debug.log(buffer[numTuple-1].toString());

            // buffer is full
            if (numTuple == buffer.length) {
            	// inner sort
            	sort(buffer, numTuple, fieldIdx, false);
            	// external Sort
            	exSort.addSortedFile(buffer, numTuple);
            	// clean the buffer
            	numTuple = 0;
            }
        }
        if (numTuple > 0) {
        	sort(buffer, numTuple, fieldIdx, false);
        	exSort.addSortedFile(buffer, numTuple);
        }
        
    	return exSort.mergeFile();
    }
    
    /** 
     * sort-merge join
     * @return an iterator on all joined result tuples
     * @throws TransactionAbortedException
     * @throws DbException
     * @throws IOException 
     * @throws NoSuchElementException 
     */
    private Iterator<Tuple> getAllFetchNext() throws TransactionAbortedException, DbException, NoSuchElementException, IOException {
        
    	
    	int tpSize1 = child1.getTupleDesc().numFields();
        int tpSize2 = child2.getTupleDesc().numFields();
        tempTps = new ArrayList<Tuple>();
        
        // use sorted-merge algorithm
        int leftBufferSize = BLOCKMEMORY / child1.getTupleDesc().getSize();
        int rightBufferSize = BLOCKMEMORY / child2.getTupleDesc().getSize();
        
        leftBuffer = new Tuple[leftBufferSize];
        rightBuffer = new Tuple[rightBufferSize];
        
        int leftIndex = 0;
        int rightIndex = 0;

        while (child1.hasNext()){
            Tuple tp1 = child1.next();
            leftBuffer[leftIndex++] = tp1;

            // buffer is full
            if (leftIndex == leftBufferSize) {
            	sort(leftBuffer, leftBufferSize, p.getFieldIdx1(), false);
            	
            	leftIndex = 0;
            }
        }
        
        
        while (child1.hasNext()){
            Tuple tp1 = child1.next();
            leftBuffer[leftIndex++] = tp1;

            if (leftIndex < leftBufferSize) continue;
            
            while (child2.hasNext()){
                Tuple tp2 = child2.next();
                rightBuffer[rightIndex++] = tp2;
                
                if (rightIndex < rightBufferSize) continue; 
                
                sortMerge(leftIndex, rightIndex);

                rightIndex = 0;
            }

            if (rightIndex < rightBufferSize) {
                
                sortMerge(leftIndex, rightIndex);
                
                rightIndex = 0;
            }

            //reset buffer
            leftIndex = 0;
            child2.rewind();
        }

        if (leftIndex != 0) {

            while (child2.hasNext()){
                Tuple tp2 = child2.next();
                rightBuffer[rightIndex++] = tp2;
                
                if (rightIndex < rightBufferSize) continue; 
                
                sortMerge(leftIndex, rightIndex);

                rightIndex = 0;
            }

            if (rightIndex < rightBufferSize) {
                
                sortMerge(leftIndex, rightIndex);
                rightIndex = 0;
            }

        }

        return tempTps.iterator();
    }

    private void sortMerge(int leftSize, int rightSize) {
        if (leftSize == 0 || rightSize == 0 ) return;

        //EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;
        switch (p.getOperator()){
            case EQUALS:
                handleEqual(leftSize, rightSize);            
                break;
            case GREATER_THAN:
            case GREATER_THAN_OR_EQ:
                handleGreaterThan(leftSize, rightSize);
                break;
            case LESS_THAN:
            case LESS_THAN_OR_EQ:
                handleLessThan(leftSize, rightSize);
                break;
        }
        
    }

    private void handleLessThan(int leftSize, int rightSize) {
        sort(leftBuffer, leftSize, p.getFieldIdx1(), true);
        sort(rightBuffer, rightSize, p.getFieldIdx2(), true);
        int left = 0;
        int right = 0;
        
        while (left < leftSize && right < rightSize) {
            Tuple ltp = leftBuffer[left];
            Tuple rtp = rightBuffer[right];

            if (p.filter(ltp, rtp)){
                for (int i = right; i < rightSize; i++) {
                    Tuple rtpTemp = rightBuffer[i];
                    Tuple tp = mergeTuple(ltp, rtpTemp);    
                    tempTps.add(tp);
                }
                left++;
            } else {
                right++;
            } 
        }
    }

    private void handleGreaterThan(int leftSize, int rightSize) {
        sort(leftBuffer, leftSize, p.getFieldIdx1(), true);
        sort(rightBuffer, rightSize, p.getFieldIdx2(), true);

        int left = 0;
        int right = 0;
        
        while (left < leftSize && right < rightSize) {
            Tuple ltp = leftBuffer[left];
            Tuple rtp = rightBuffer[right];

            if (p.filter(ltp, rtp)){

                for (int i = right; i < rightSize; i++) {
                    Tuple rtpTemp = rightBuffer[i];
                    Tuple tp = mergeTuple(ltp, rtpTemp);
                    tempTps.add(tp);
                }
                left++;
            } else {
                right++;
            }
        }
    }

    private void handleEqual(int leftSize, int rightSize) {
        sort(leftBuffer, leftSize, p.getFieldIdx1(), false);
        sort(rightBuffer, rightSize, p.getFieldIdx2(), false);

        int left = 0;
        int right = 0;

        JoinPredicate greatThan = new JoinPredicate(p.getFieldIdx1(), Predicate.Op.GREATER_THAN, p.getFieldIdx2());
        
        boolean equalFlag = true;
        int leftFlag = 0;

        while (left < leftSize && right < rightSize) {
            Tuple ltp = leftBuffer[left];
            Tuple rtp = rightBuffer[right];

            if (p.filter(ltp, rtp)){
                if (equalFlag) {
                    leftFlag = left;
                    equalFlag = !equalFlag;
                }
                Tuple tp = mergeTuple(ltp, rtp);
                tempTps.add(tp);
                left++;

                if (right < rightSize && left >= leftSize) {
                    right++;
                    left = leftFlag;
                    equalFlag = !equalFlag;    
                }
                
            } else if (greatThan.filter(ltp, rtp)){
                right++;
                left = leftFlag;
                equalFlag = !equalFlag;
            } else {
                left++;
            }
        }
    }


    private void sort(Tuple[] buffer, int length, int field, boolean reverse) {

        CompareTp co = new CompareTp(reverse, field);
        Arrays.sort(buffer, 0, length, co);
        //stupid ð������
        // for (int i = 1; i < length; i++) {
        //     for (int j = 0; j < length - i; j++) {
        //         if (reverse) {                    
        //             if (greatThan.filter(buffer[j+1], buffer[j])) {
        //                 Tuple temp = buffer[j];
        //                 buffer[j] = buffer[j + 1];
        //                 buffer[j + 1] = temp;
        //             }
        //         } else {
        //             if (greatThan.filter(buffer[j], buffer[j+1])) {
        //                 Tuple temp = buffer[j];
        //                 buffer[j] = buffer[j + 1];
        //                 buffer[j + 1] = temp;
        //             }
        //         }
        //     }
        // }

    }

    class CompareTp implements Comparator<Tuple>{
        
        private JoinPredicate cop;

        public CompareTp(boolean reverse, int field){
            super();
            if (reverse) {
                cop = new JoinPredicate(field, Predicate.Op.LESS_THAN, field);
            } else {
                cop = new JoinPredicate(field, Predicate.Op.GREATER_THAN, field);
            }
        }

        @Override
        public int compare(Tuple t1, Tuple t2){
            // t1>t2
            if (cop.filter(t1, t2)){
                return 1;
            } else if (cop.filter(t2, t1)){
                return -1;
            } else {
                return 0;
            }
        }
    }

    private Tuple mergeTuple(Tuple tp1, Tuple tp2) {
        int tpSize1 = tp1.getTupleDesc().numFields();
        int tpSize2 = tp2.getTupleDesc().numFields();

        Tuple tempTp = new Tuple(getTupleDesc());
        int i = 0;
        for (; i < tpSize1; i++){
            tempTp.setField(i, tp1.getField(i));
        }

        for (; i < tpSize2 + tpSize1 ; i++){
            tempTp.setField(i, tp2.getField(i-tpSize1));
        }

        return tempTp;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{ child1, child2 };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        child1 = children[0];
        child2 = children[1];
    }

}