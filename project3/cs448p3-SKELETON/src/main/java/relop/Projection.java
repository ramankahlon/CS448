package relop;

/**
 * The projection operator extracts columns from a relation; unlike in
 * relational algebra, this operator does NOT eliminate duplicate tuples.
 */
public class Projection extends Iterator {

    private Schema schema = null;
    private Iterator it = null;
    private Integer [] fields = null;
    private boolean isOpen;
	
  /**
   * Constructs a projection, given the underlying iterator and field numbers.
   */
  public Projection(Iterator aIter, Integer... aFields) {
	  //throw new UnsupportedOperationException("Not implemented");
    //Your code here
      this.fields = aFields;
      this.schema = new Schema(fields.length);
      this.it = aIter;
      for(int i = 0; i < fields.length; i++){
          schema.initField(i, it.getSchema().fieldType(fields[i]), it.getSchema().fieldLength(fields[i]), it.getSchema().fieldName(fields[i]));
      }
      setSchema(schema);
      isOpen = true;
  }

  /**
   * Gives a one-line explanation of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	  //throw new UnsupportedOperationException("Not implemented");
    //Your code here
      for(int i = 0; i < depth; i++){
          System.out.printf("    ");
      }
      System.out.printf("Projection\n");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  //throw new UnsupportedOperationException("Not implemented");
    //Your code here
      if(isOpen)
          it.close();
      it.restart();
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
          it.close();
      isOpen = false;
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	  //throw new UnsupportedOperationException("Not implemented");
    //Your code here
      if(isOpen)
          return it.hasNext();
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
          Tuple nextTuple = it.getNext();
          Tuple nextTuple_dup = new Tuple(schema);

          for(int i=0; i<fields.length; i++){
              nextTuple_dup.setField(i, nextTuple.getField(fields[i]));
          }

          return nextTuple_dup;
      }
      else
          throw new IllegalStateException("no more tuples");
  }

} // public class Projection extends Iterator
