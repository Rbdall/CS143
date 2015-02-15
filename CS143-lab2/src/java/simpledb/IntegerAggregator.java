package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    private HashMap<Field, ArrayList<Field>> groupings;
    
    
    private int groupField;
    private Type groupFieldType;
    private int aggField;
    private Op operator;
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        groupField = gbfield;
        groupFieldType = gbfieldtype;
        aggField = afield;
        operator = what;
        
        groupings = new HashMap<Field, ArrayList<Field>>();
        if(groupField == Aggregator.NO_GROUPING){
        	groupings.put(new IntField(Aggregator.NO_GROUPING), new ArrayList<Field>());
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if(groupField == Aggregator.NO_GROUPING){
        	groupings.get(Aggregator.NO_GROUPING).add(tup.getField(aggField));
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
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
    	ArrayList<Tuple> resultTuples = new ArrayList<Tuple>();
    	TupleDesc resultDesc;
    	if(groupField == Aggregator.NO_GROUPING){
        	resultDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        	Tuple resultTup = new Tuple(resultDesc);
        	switch(operator){
	    		case MIN:
	    			int min = Integer.MAX_VALUE;
	    			for(Field value : groupings.get(Aggregator.NO_GROUPING)){
	    				IntField castValue = (IntField)value;
	    				if(castValue.getValue() < min){
	    					min = castValue.getValue();
	    				}
	    			}
	    			resultTup.setField(0, new IntField(min));
	    			resultTuples.add(resultTup);
	    			break;
	    		case MAX:
	    			int max = Integer.MIN_VALUE;
	    			for(Field value : groupings.get(Aggregator.NO_GROUPING)){
	    				IntField castValue = (IntField)value;
	    				if(castValue.getValue() > max){
	    					max = castValue.getValue();
	    				}
	    			}
	    			resultTup.setField(0, new IntField(max));
	    			resultTuples.add(resultTup);
	    			break;
	    		case COUNT:
	    			int count = groupings.get(Aggregator.NO_GROUPING).size();
	    			resultTup.setField(0, new IntField(count));
	    			resultTuples.add(resultTup);
	    			break;
	    		case AVG:
	    			int average = 0;
	    			for(Field value : groupings.get(Aggregator.NO_GROUPING)){
	    				IntField castValue = (IntField)value;
	    				average += castValue.getValue();
	    			}
	    			resultTup.setField(0, new IntField(average/groupings.get(Aggregator.NO_GROUPING).size()));
	    			resultTuples.add(resultTup);
	    			break;
	    		case SUM:
	    			int sum = 0;
	    			for(Field value : groupings.get(Aggregator.NO_GROUPING)){
	    				IntField castValue = (IntField)value;
	    				sum += castValue.getValue();
	    			}
	    			resultTup.setField(0, new IntField(sum));
	    			resultTuples.add(resultTup);
	    			break;
	    		default:
	    			break;
        	}
        	return new TupleIterator(resultDesc, resultTuples);
    	}
    	else{
    		resultDesc = new TupleDesc(new Type[]{groupFieldType, Type.INT_TYPE});
    		for(Field key : groupings.keySet()){
    			Tuple resultTup = new Tuple(resultDesc);
    			switch(operator){
		    		case MIN:
		    			int min = Integer.MAX_VALUE;
		    			for(Field value : groupings.get(key)){
		    				IntField castValue = (IntField)value;
		    				if(castValue.getValue() < min){
		    					min = castValue.getValue();
		    				}
		    			}
		    			resultTup.setField(0, key);
		    			resultTup.setField(1, new IntField(min));
		    			resultTuples.add(resultTup);
		    			break;
		    		case MAX:
		    			int max = Integer.MIN_VALUE;
		    			for(Field value : groupings.get(key)){
		    				IntField castValue = (IntField)value;
		    				if(castValue.getValue() > max){
		    					max = castValue.getValue();
		    				}
		    			}
		    			resultTup.setField(0, key);
		    			resultTup.setField(1, new IntField(max));
		    			resultTuples.add(resultTup);
		    			break;
		    		case COUNT:
		    			int count = groupings.get(key).size();
		    			resultTup.setField(0, key);
		    			resultTup.setField(1, new IntField(count));
		    			resultTuples.add(resultTup);
		    			break;
		    		case AVG:
		    			int average = 0;
		    			for(Field value : groupings.get(key)){
		    				IntField castValue = (IntField)value;
		    				average += castValue.getValue();
		    			}
		    			resultTup.setField(0, key);
		    			resultTup.setField(1, new IntField(average/groupings.get(key).size()));
		    			resultTuples.add(resultTup);
		    			break;
		    		case SUM:
		    			int sum = 0;
		    			for(Field value : groupings.get(key)){
		    				IntField castValue = (IntField)value;
		    				sum += castValue.getValue();
		    			}
		    			resultTup.setField(0, key);
		    			resultTup.setField(1, new IntField(sum));
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
