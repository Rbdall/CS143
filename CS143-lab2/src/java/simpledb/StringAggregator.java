package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    private HashMap<Field, ArrayList<Field>> groupings;
    
    
    private int groupField;
    private Type groupFieldType;
    private int aggField;
    private Op operator;
    
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    	groupField = gbfield;
        groupFieldType = gbfieldtype;
        aggField = afield;
        operator = what;
        
        groupings = new HashMap<Field, ArrayList<Field>>();
        if(groupField == Aggregator.NO_GROUPING){
        	groupings.put(new StringField("No Group", "No Group".length()), new ArrayList<Field>());
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	if(groupField == Aggregator.NO_GROUPING){
        	groupings.get("No Group").add(tup.getField(aggField));
        }
        else{
        	if(groupings.containsKey(tup.getField(groupField))){
        		groupings.get(tup.getField(groupField)).add(tup.getField(aggField));
        	}
        	else{
        		groupings.put(tup.getField(groupField), new ArrayList<Field>());
        		groupings.get(tup.getField(groupField)).add(tup.getField(aggField));
        	}
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
    	ArrayList<Tuple> resultTuples = new ArrayList<Tuple>();
    	TupleDesc resultDesc;
    	if(groupField == Aggregator.NO_GROUPING){
        	resultDesc = new TupleDesc(new Type[]{Type.STRING_TYPE});
        	Tuple resultTup = new Tuple(resultDesc);
        	switch(operator){
	    		case COUNT:
	    			int count = groupings.get("No Group").size();
	    			resultTup.setField(0, new IntField(count));
	    			resultTuples.add(resultTup);
	    			break;
	    		default:
	    			break;
        	}
        	return new TupleIterator(resultDesc, resultTuples);
    	}
    	else{
    		resultDesc = new TupleDesc(new Type[]{groupFieldType, Type.STRING_TYPE});
    		for(Field key : groupings.keySet()){
    			Tuple resultTup = new Tuple(resultDesc);
    			switch(operator){
		    		case COUNT:
		    			int count = groupings.get(key).size();
		    			resultTup.setField(0, key);
		    			resultTup.setField(1, new IntField(count));
		    			resultTuples.add(resultTup);
		    			break;
		    		default:
		    			break;
    			}
        	}
    		return new TupleIterator(resultDesc, resultTuples);
    	}
    }

}
