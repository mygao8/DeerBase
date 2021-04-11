package deerBase;

import java.util.*;

import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    HeapPageId pid;
    TupleDesc td;
    byte header[];
    Tuple tuples[];
    int numSlots;

    boolean isDirty;
    TransactionId dirtier;
    byte[] oldData;	// data of the page before a modify transaction, used for recovery

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#PAGE_SIZE
     */
    public HeapPage(HeapPageId pid, byte[] data) throws IOException {
        this.pid = pid;
        this.td = Database.getCatalog().getTupleDesc(pid.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        this.header = new byte[getHeaderSize()];
        for (int i=0; i<this.header.length; i++) {
            this.header[i] = dis.readByte();
        }
        
        try{
            // allocate and read the actual records of this page
            this.tuples = new Tuple[this.numSlots];
            for (int i=0; i<this.tuples.length; i++) {
                this.tuples[i] = readNextTuple(dis,i);
            }
        }catch(NoSuchElementException e) {
            e.printStackTrace();
        }
        dis.close();

        this.isDirty = false;        
        setBeforeImage();
    }
    
    public HeapPage (HeapPageId pid) {
    	this.pid = pid;
        this.td = Database.getCatalog().getTupleDesc(pid.getTableId());
        this.numSlots = getNumTuples();
        this.header = new byte[getHeaderSize()];
        this.tuples = new Tuple[this.numSlots];
        this.isDirty = false;        
        setBeforeImage();
	}

    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
    */
    private int getNumTuples() {
    	if (this.numSlots != 0) {
    		return this.numSlots;
    	}
    	// Each tuple requires tuple size * 8 bits for its content and 1 bit for the header
    	return (BufferPool.getPageSize() * 8) / (this.td.getSize() * 8 + 1);
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {        
    	return (int) Math.ceil(getNumTuples() / 8.0);
    }
    
    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            return new HeapPage(this.pid, this.oldData);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }
    
    public void setBeforeImage() {
        this.oldData = getPageData().clone();
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
    	return this.pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                	// useless reading, just adjust the pos of dis from where we read next time
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (int i=0; i<header.length; i++) {
            try {
                dos.writeByte(header[i]);
            } catch (IOException e) {
                // this shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page;  the tuple should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
    	RecordId recordId = t.getRecordId();
    	PageId requestPid = recordId.getPageId();
    	
    	if (!pid.equals((HeapPageId) requestPid)) {
    		throw new DbException("requested tuple is not on the page");
    	}
    	if (!isSlotUsed(recordId.getTupleNo())) {
    		throw new DbException("tuple slot is already empty");
    	}
    	
    	tuples[recordId.getTupleNo()] = null;
    	markSlotUsed(recordId.getTupleNo(), false);
    	Database.getCatalog().getDbFile(pid.getTableId())
		.setNotFullPagesList(pid.pageNumber(), false);
    }

    /**
     * Add the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
    	int numEmptySlots = getNumEmptySlots();
    	if (numEmptySlots == 0) {
    		throw new DbException("this page is full");
    	}
    	if (!td.equals(t.getTupleDesc())) {
    		throw new DbException("tupleDsec is not matched");
    	}
    	
    	for (int i = 0; i < numSlots; i++) {
    		if (!isSlotUsed(i)) {
    			tuples[i] = t;
    			t.setRecordId(new RecordId(pid, i));
    			markSlotUsed(i, true);
    			break;
    		}
    	}
    	
    	// update the status of full page immediately
    	if (getNumEmptySlots() == 0) {
    		Database.getCatalog().getDbFile(pid.getTableId())
    			.setNotFullPagesList(pid.pageNumber(), true);
    	}
    	return;
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
    	dirtier = tid;
    	isDirty = dirty;
    }
    

	public boolean isDirty() {
		return isDirty;
	}

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId getDirtier() {
    	if (isDirty) {
    		return dirtier;
    	} else {
    		return null;
    	}
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
    	// an efficient method with weak readability
    	int numUsedSlots = 0;
    	for (byte curByte : this.header) {
        	while (curByte != 0) {
        		// curByte-1 will eliminate the lowest '1' in curByte
        		// the following calculation will do nothing but change the lowest '1' to '0'
        		curByte &= (curByte-1);
        		numUsedSlots += 1;
        	}
    	}
    	return this.numSlots - numUsedSlots;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
    	// if the first 18 slots are used, this.header looks like [11111111, 11111111, 00000011, ...]
    	int slotIdx = i/8;

    	if ((((this.header[slotIdx]) >>> (i%8)) & 0x01) == 1) {
    		return true;
    	} else {
    		return false;
    	}
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     * @see isSlotUsed(int)
     */
    private void markSlotUsed(int i, boolean value) {
    	int slotIdx = i/8;
    	byte mask = (byte) (1 << (i%8));
    	
    	if (value == true) {
    		header[slotIdx] |= mask;
    	} else {
    		header[slotIdx] &= ~mask;
    	}
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        return new pageItr();
    }
    
    private class pageItr implements Iterator<Tuple> {
		private int slotIdx = 0;
		private int numIteratedTuples = 0;
		private int numUsedSlots = numSlots - getNumEmptySlots();
				
		@Override
		public boolean hasNext() {
			return slotIdx < numSlots && numIteratedTuples < numUsedSlots;
		}
		
		@Override
		public Tuple next() {
			try {
				// find the first non-empty slots
				while (!isSlotUsed(slotIdx)) {
					slotIdx++;
				}
				numIteratedTuples++;
				return tuples[slotIdx++];
			} catch (IndexOutOfBoundsException e) {
				throw new NoSuchElementException();
			}
		}
		
		@Override
		public void remove() throws UnsupportedOperationException {
			throw new UnsupportedOperationException();
		}
	}
}

