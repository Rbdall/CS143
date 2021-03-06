package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    
    private JoinPredicate p;
    private DbIterator child1;
    private DbIterator child2;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
    	this.p = p;
        this.child1 = child1;
        this.child2 = child2;
    }

    public JoinPredicate getJoinPredicate() {
        return this.p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        return child1.getTupleDesc().getFieldName(p.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
    	return child2.getTupleDesc().getFieldName(p.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        return TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
    	super.open();
    	child1.open();
    	child2.open();
    }

    public void close() {
    	super.close();
    	child1.close();
    	child2.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	child1.rewind();
    	firstTup = null;
    	keepGoing = true;
    	child2.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    
    //Need a value to hold the current tuple being cross-product-ed between calls of fetchNext()
    private Tuple firstTup = null;
    //keepGoing represents the value of child1.hasNext() stored between calls
    //Gets set to false when child1 runs out of tuples to join
    private boolean keepGoing = true; 
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	while(keepGoing){
    		//If there isn't a stored tuple, get the next candidate from child1
    		if(firstTup == null){
    			firstTup = child1.next();
    		}
    		while(child2.hasNext()){
    	        Tuple secondTup = child2.next();
    	        if(p.filter(firstTup, secondTup)){
    	        	TupleDesc combinedDesc = getTupleDesc();
    	        	Tuple combined = new Tuple(combinedDesc);
    	        	for(int i = 0; i < firstTup.getTupleDesc().numFields(); i++){
    	        		combined.setField(i, firstTup.getField(i));
    	        	}
    	        	for(int j = 0; j < secondTup.getTupleDesc().numFields(); j++){
    	        		combined.setField(j+firstTup.getTupleDesc().numFields(), secondTup.getField(j));
    	        	}
    	        	return combined;
    	        }
    		}
    		//When the stored tuple has been compared to all tuples in child2, reset
    		if(child1.hasNext() == false){
    			keepGoing = false;
    		}
    		firstTup = null;
    		child2.rewind();
    	}
    	return null;
        
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
    	DbIterator[] arr = {child1, child2};
        return arr;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        child1 = children[0];
        child2 = children[1];
    }

}
