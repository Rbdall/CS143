package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {
	
	private TransactionId t;
	private DbIterator child;

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.t = t;
        this.child = child;
    }

    public TupleDesc getTupleDesc() {
        return child.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
    }

    public void close() {
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	int count = 0;
    	while(child.hasNext()){
    		Tuple next = child.next();
    		try {
				Database.getBufferPool().deleteTuple(t, next);
				count++;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	
    	TupleDesc resultTd = new TupleDesc(new Type[] {Type.INT_TYPE});
        Tuple result = new Tuple(resultTd);
        result.setField(0, new IntField(count));
        return result;
    }

    @Override
    public DbIterator[] getChildren() {
    	DbIterator[] arr = {child};
        return arr;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        child = children[0];
    }

}
