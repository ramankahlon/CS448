package relop;

import heap.HeapFile;
import index.HashIndex;
import global.SearchKey;
import global.RID;
import global.AttrOperator;
import global.AttrType;

public class HashJoin extends Iterator {

	private IndexScan scan1, scan2;
	private Iterator iter1, iter2;
	private Integer col1, col2;
	//int currentHash;
	private HashTableDup hashDup;
	private Tuple foundTup[] = null;
	private Tuple nextTup;
	private Tuple rightTup;
	private int count = -1;
	private SearchKey key = null;
	//private boolean isOpen;
	
	public HashJoin(Iterator aIter1, Iterator aIter2, int aJoinCol1, int aJoinCol2){
		//throw new UnsupportedOperationException("Not implemented");
		//Your code here
		this.iter1 = aIter1;
		this.iter2 = aIter2;
		this.schema = Schema.join(aIter1.schema, aIter2.schema);
		this.col1 = aJoinCol1;
		this.col2 = aJoinCol2;

		hashDup = new HashTableDup();
		//isOpen = false;

		HeapFile left_file = new HeapFile(null);
		HashIndex left_index = new HashIndex(null);
		HeapFile right_file = new HeapFile(null);
		HashIndex right_index = new HashIndex(null);
		while(iter1.hasNext()){
			Tuple tup1 = iter1.getNext();
			left_index.insertEntry(new SearchKey(tup1.getField(col1)), left_file.insertRecord(tup1.getData()));
		}
		while(iter2.hasNext()){
			Tuple tup2 = iter2.getNext();
			right_index.insertEntry(new SearchKey(tup2.getField(col2)), right_file.insertRecord(tup2.getData()));
		}
		scan1 = new IndexScan(iter1.getSchema(), left_index, left_file);
		scan2 = new IndexScan(iter2.getSchema(), right_index, right_file);
		while(scan1.hasNext()){
			Tuple tup = scan1.getNext();
			hashDup.add(new SearchKey(tup.getField(col1)), tup);
		}
	}

	@Override
	public void explain(int depth) {
		throw new UnsupportedOperationException("Not implemented");
		//Your code here
	}

	@Override
	public void restart() {
		//throw new UnsupportedOperationException("Not implemented");
		//Your code here
		scan1.restart();
		scan2.restart();
		hashDup.clear();
		while (scan1.hasNext())
		{
			Tuple tup = scan1.getNext();
			SearchKey skey = new SearchKey(tup.getField(col1));
			hashDup.add(skey, tup);
		}
	}

	@Override
	public boolean isOpen() {
		//throw new UnsupportedOperationException("Not implemented");
		//Your code here
		if(iter1.isOpen() && iter2.isOpen())
			return true;
		return false;
	}

	@Override
	public void close() {
		//throw new UnsupportedOperationException("Not implemented");
		//Your code here
		iter1.close();
		scan1.close();
		iter2.close();
		scan2.close();
		hashDup.clear();
	}

	@Override
	public boolean hasNext() {
		//throw new UnsupportedOperationException("Not implemented");
		//Your code here
		if (count == -1){
			if(scan2.hasNext() == false){
				nextTup = null;
				return false;
			}
			else
			{
				count = 0;
				rightTup = scan2.getNext();
				key = new SearchKey(rightTup.getField(col2));
				foundTup = hashDup.getAll(key);
			}
		}
		if (foundTup.length <= count || foundTup == null){
			count = -1;
			return hasNext();
		}
		if (rightTup.getField(col2).equals(foundTup[count].getField(col1))) {
			nextTup = Tuple.join(foundTup[count], rightTup, schema);
			count++;
			return true;
		}
		else
		{
			count++;
			return false;
		}
	}

	@Override
	public Tuple getNext() {
		//throw new UnsupportedOperationException("Not implemented");
		//Your code here
		if(nextTup != null)
			return nextTup;
		else
			throw new IllegalStateException("no more tuples");
	}
} // end class HashJoin;
