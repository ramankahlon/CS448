package relop;

import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
//added import
import index.BucketScan;

/**
 * Wrapper for bucket scan, an index access method.
 */
public class IndexScan extends Iterator {

    private HashIndex index = null;
    private HeapFile file = null;
    private BucketScan scan = null;
    private boolean isOpen;

  /**
   * Constructs an index scan, given the hash index and schema.
   */
  public IndexScan(Schema schema, HashIndex index, HeapFile file) {
	  //throw new UnsupportedOperationException("Not implemented");
  //Your code here
      this.schema = schema;
      this.index = index;
      this.file = file;
      this.scan = index.openScan();
      isOpen = true;
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	  //throw new UnsupportedOperationException("Not implemented");
  //Your code here
      for(int i = 0; i < depth; i++){
          System.out.printf("    ");
      }
      System.out.printf("IndexScan\n");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  //throw new UnsupportedOperationException("Not implemented");
  //Your code here
      if(isOpen)
        scan.close();
      scan = index.openScan();
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
	  //throw new UnsupportedOperationException("Not implemented");
  //Your code here
      return isOpen;
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
	  //throw new UnsupportedOperationException("Not implemented");
  //Your code here
      if(isOpen)
        scan.close();
      isOpen = false;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	  //throw new UnsupportedOperationException("Not implemented");
  //Your code here
      if(isOpen)
          return scan.hasNext();
      return false;
  }

  /**
   * Gets the next tuple in the iteration.
   *
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	  //throw new UnsupportedOperationException("Not implemented");
  //Your code here
      if(isOpen){
          byte [] data = file.selectRecord(scan.getNext());  // get the data of the next record
          return new Tuple(schema, data);    // create the new tuple
      }
      throw new IllegalStateException("no more tuples");
  }

  /**
   * Gets the key of the last tuple returned.
   */
  public SearchKey getLastKey() {
	  //throw new UnsupportedOperationException("Not implemented");
  //Your code here
      return scan.getLastKey();
  }

  /**
   * Returns the hash value for the bucket containing the next tuple, or maximum
   * number of buckets if none.
   */
  public int getNextHash() {
	  //throw new UnsupportedOperationException("Not implemented");
  //Your code here
      return scan.getNextHash();
  }

} // public class IndexScan extends Iterator
