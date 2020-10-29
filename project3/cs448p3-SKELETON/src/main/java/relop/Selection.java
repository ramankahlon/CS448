package relop;

/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Minibase, this condition is simply a set of independent predicates logically
 * connected by OR operators.
 */
public class Selection extends Iterator {

    private Iterator it = null;
    private Predicate [] preds = null;
    private Tuple nextTuple = null;
    private boolean isOpen;

  /**
   * Constructs a selection, given the underlying iterator and predicates.
   */
  public Selection(Iterator aIter, Predicate... aPreds) {
	  //throw new UnsupportedOperationException("Not implemented");
    //Your code here
      this.it = aIter;
      this.schema = it.getSchema();
      this.preds = aPreds;
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
      System.out.printf("Selection\n");
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
      while(it.hasNext()){
          Tuple t = it.getNext();
          if(preds.length == 0){
              nextTuple = t;
              return true;
          }
          for(int i=0; i<preds.length; i++){
              if(preds[i].evaluate(t)){
                  nextTuple = t;
                  return true;
              }
          }
      }
      nextTuple = null;
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
      if(nextTuple == null)
          throw new IllegalStateException("No More Tuples");
      else
          return nextTuple;
  }

} // public class Selection extends Iterator
