package relop;

import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import index.HashScan;

/**
 * Wrapper for hash scan, an index access method.
 */
public class KeyScan extends Iterator {

    private HashIndex index = null;
    private SearchKey key = null;
    private HeapFile file = null;
    private HashScan scan = null;
    private boolean isOpen;

    /**
     * Constructs an index scan, given the hash index and schema.
     */
    public KeyScan(Schema aSchema, HashIndex aIndex, SearchKey aKey, HeapFile aFile) {
        //throw new UnsupportedOperationException("Not implemented");
        //Your code here
        this.schema = aSchema;
        this.index = aIndex;
        this.key = aKey;
        this.file = aFile;
        this.scan = index.openScan(key);
        isOpen = true;
    }

    /**
     * Gives a one-line explaination of the iterator, repeats the call on any
     * child iterators, and increases the indent depth along the way.
     */
    public void explain(int depth) {
        //throw new UnsupportedOperationException("Not implemented");
        for(int i = 0; i < depth; i++){
            System.out.printf("    ");
        }
        System.out.printf("KeyScan\n");
    }

    /**
     * Restarts the iterator, i.e. as if it were just constructed.
     */
    public void restart() {
        if(isOpen)
            scan.close();
        scan = index.openScan(key);
    }

    /**
     * Returns true if the iterator is open; false otherwise.
     */
    public boolean isOpen() {
        return isOpen;
    }

    /**
     * Closes the iterator, releasing any resources (i.e. pinned pages).
     */
    public void close() {
        if(isOpen)
            scan.close();
        isOpen = false;
    }

    /**
     * Returns true if there are more tuples, false otherwise.
     */
    public boolean hasNext() {
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
            byte [] data = file.selectRecord(scan.getNext());
            return new Tuple(schema, data);
        }
        else
            throw new IllegalStateException("no more tuples");
    }

} // public class KeyScan extends Iterator