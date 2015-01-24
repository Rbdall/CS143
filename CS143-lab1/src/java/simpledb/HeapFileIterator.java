package simpledb;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator{
	  /**
   * Opens the iterator
   * @throws DbException when there are problems opening/accessing the database.
   */
	private TransactionId tid;
	private HeapFile file;
	private HeapPage currPage;
	private PageId currPageId;
	private int currPageNumber;
	private Iterator<Tuple> tupleIterator;
	
	private HeapPage nextPage;
	private PageId nextPageId;
	private int nextPageNumber;
	private Iterator<Tuple> nextTupleIterator;
	
	
	private boolean open;
	
	public HeapFileIterator(TransactionId inputTid, HeapFile target){
		tid = inputTid;
		file = target;
		currPage = null;
		currPageId = null;
		currPageNumber = 0;
		tupleIterator = null;
		open = false;
	}
	
  public void open()
      throws DbException, TransactionAbortedException{
  	currPageId = new HeapPageId(file.getId(), currPageNumber);
  	currPage = (HeapPage)(Database.getBufferPool().getPage(tid, currPageId, null));
  	//currPage = (HeapPage)(file.readPage(currPageId));
  	tupleIterator = currPage.iterator();
  	open = true;
  }

  /** @return true if there are more tuples available. */
  public boolean hasNext()
      throws DbException, TransactionAbortedException{ 
  	if(!open){
  		return false;
  	}
  	if(tupleIterator.hasNext()){
  		return true;
  	}
  	else if(currPageNumber < file.numPages()){
  		for(int i = 1; currPageNumber+i < file.numPages(); i++ ){
  			nextPageNumber = currPageNumber+i;
      		nextPageId = new HeapPageId(file.getId(), nextPageNumber);
          	nextPage = (HeapPage)(Database.getBufferPool().getPage(tid, nextPageId, null));
          	nextTupleIterator = nextPage.iterator();
          	if(nextTupleIterator.hasNext()){
          		return true;
          	}
  		}
  	}
  	return false;
  	
  }

  /**
   * Gets the next tuple from the operator (typically implementing by reading
   * from a child operator or an access method).
   *
   * @return The next tuple in the iterator.
   * @throws NoSuchElementException if there are no more tuples
   */
  public Tuple next()
      throws DbException, TransactionAbortedException, NoSuchElementException{ 
  	if(!open /*|| !hasNext()*/){
  		throw new NoSuchElementException();
  	}
  	else if(tupleIterator.hasNext()){
  		return tupleIterator.next();
  	}
  	else if(currPageNumber < file.numPages()){
  		currPageId = nextPageId;
  		currPage = nextPage;
  		currPageNumber = nextPageNumber;
  		tupleIterator = nextTupleIterator;
  		return tupleIterator.next();
    
  	}
  	throw new NoSuchElementException();
  }

  /**
   * Resets the iterator to the start.
   * @throws DbException When rewind is unsupported.
   */
  public void rewind() throws DbException, TransactionAbortedException{
  	currPageNumber = 0;
  	open();
  }

  /**
   * Closes the iterator.
   */
  public void close(){
  	currPageId = null;
  	currPage = null;
  	tupleIterator = null;
  	open = false;
  }
}
