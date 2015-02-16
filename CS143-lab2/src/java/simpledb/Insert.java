package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {
	
	private TransactionId t;
	private DbIterator child;
	private int tid;

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        this.t = t;
        this.child = child;
        this.tid = tableid;
        if(this.child.getTupleDesc() != Database.getCatalog().getTupleDesc(this.tid))
        	throw new DbException("TupleDesc of child differs from table");
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
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        int count = 0;
        while(child.hasNext()){
        	Tuple next = child.next();
        	try {
				Database.getBufferPool().insertTuple(t, tid, next);
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
