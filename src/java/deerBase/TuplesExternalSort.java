package deerBase;

import java.io.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

import com.google.code.externalsorting.ExternalSort;


public class TuplesExternalSort {
	private List<File> sortedFiles;
	private TupleDesc td;
	private File mergedFile;
	private int filedIdx;
	
	public TuplesExternalSort(TupleDesc td, int fieldIdx) {
		this.td = td;
		this.filedIdx = fieldIdx;
		this.sortedFiles = new ArrayList<>();
		this.mergedFile = new File("tmpMerged.txt");
	}

	public void addSortedFile(Tuple[] sortedTuples, int numTuple) throws IOException {
		File newTempFile = File.createTempFile("tempFile_SortedTuples", ".txt");
		RandomAccessFile adFile = new RandomAccessFile(newTempFile, "rw");
		
		int len = numTuple * td.getSize();
		ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);
		
        System.err.println("numTuple:"+numTuple + " numFields:" + td.numFields());
		for (int i = 0; i < numTuple; i++) {
			for (int j=0; j<td.numFields(); j++) {
                Field f = sortedTuples[i].getField(j);
                System.out.println("tuple:" + sortedTuples[i].toString() 
                		+ " field:" + f.toString());
                try {
                    f.serialize(dos);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            // add new line
            //dos.write(new byte[]{13,10});
		}
		
		byte[] buf = baos.toByteArray();
		//System.out.println("buf:" + buf);
		adFile.write(buf);

		
		
		System.out.println(adFile.readLine());
		adFile.close();
		
		sortedFiles.add(newTempFile);
	}
	
	public File mergeFile() throws IOException {
		ExternalSort.mergeSortedFiles(
				sortedFiles, mergedFile, new strCmp(filedIdx, td));
		return mergedFile;
	}
	
 	
	class strCmp implements Comparator<String>{
		private TupleDesc td;
		private int filedIdx;
        
		public strCmp(int filedIdx, TupleDesc td) {
			this.filedIdx = filedIdx;
			this.td = td;
		}

		@Override
		public int compare(String o1, String o2) {
			byte[] buf1 = o1.getBytes();
			byte[] buf2 = o2.getBytes();
			DataInputStream dis1 = new DataInputStream(new ByteArrayInputStream(buf1));
			DataInputStream dis2 = new DataInputStream(new ByteArrayInputStream(buf2));
			
			int pos = 0, j;
			for (j = 0; j < filedIdx; j++) {
                pos += td.getFieldType(j).getLen();
            }
			
			Field f1, f2;
			try {
				dis1.skip(pos);
				dis2.skip(pos);
				f1 = td.getFieldType(j).parse(dis1);
				f2 = td.getFieldType(j).parse(dis1);
				if (f1.compare(Predicate.Op.GREATER_THAN, f2)) {
					return 1;
				}
				if (f1.compare(Predicate.Op.EQUALS, f2)) {
					return 0;
				}
				if (f1.compare(Predicate.Op.LESS_THAN, f2)) {
					return 1;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			// wrong return
			return 0;
		}
		
	}
	
	
}