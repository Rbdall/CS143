package simpledb;
import java.util.*;
/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
	private int minVal;
	private int maxVal;
	private double bucketWidth = 0;
	private ArrayList<ArrayList<Integer>> bucketList;
	private ArrayList<Integer> bucketMinList;
	private int numTuples = 0;
    public IntHistogram(int buckets, int min, int max) {
    	minVal = min;
    	maxVal = max;
    	bucketWidth = (maxVal-minVal)/buckets;
    	if(bucketWidth < 1){
    		bucketWidth = 1;
    	}
    	
    	int currBucketMin = minVal;
    	bucketList = new ArrayList<ArrayList<Integer>>();
    	bucketMinList = new ArrayList<Integer>();
    	for(int i = 0; i < buckets; i++){
    		bucketList.add(new ArrayList<Integer>());
    		bucketMinList.add(currBucketMin);
    		currBucketMin += bucketWidth;
    	}

    	
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	for(int i = 0; i < bucketMinList.size(); i++){
    		if(v <= bucketMinList.get(i)){
    			bucketList.get(i).add(v);
    			numTuples++;
    			return;
    		}
    	}
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	int numInSmallerBuckets = 0;
    	int numInLargerBuckets = 0;
    	double result = -1.0;
    	switch(op){
    		case EQUALS:
    			for(int i = 0; i < bucketMinList.size(); i++){
    	    		if(v <= bucketMinList.get(i)){
    	    			result = (bucketList.get(i).size()/bucketWidth)/numTuples;
    	    			break;
    	    		}
    	    	}
    			break;
    		case NOT_EQUALS:
    			for(int i = 0; i < bucketMinList.size(); i++){
    	    		if(v <= bucketMinList.get(i)){
    	    			result = (numTuples - (bucketList.get(i).size()/bucketWidth))/numTuples;
    	    			break;
    	    		}
    	    	}
    			break;
    		case GREATER_THAN:
    			if(v > maxVal){
    				return 0.0;
    			}
    			else if (v < minVal){
    				return 1.0;
    			}
    			for(int i = 0; i < bucketMinList.size(); i++){
    	    		if(v <= bucketMinList.get(i)){
    	    			numInLargerBuckets = 0; 
    	    			for(int j = i+1; j < bucketList.size(); j++){
    	    				numInLargerBuckets += bucketList.get(j).size();
    	    			}
    	    			int bRight = 0;
    	    			if(i+1 == bucketMinList.size()){
    	    				bRight = maxVal;
    	    			}
    	    			else{
    	    				bRight = bucketMinList.get(i+1)-1;
    	    			}
    	    			double numToRightInBucket = ((bRight-v) * (bucketList.get(i).size()/bucketWidth));
    	    			result = (numToRightInBucket+numInLargerBuckets)/numTuples;
    	    			break;
    	    		}
    	    	}
    			break;
    		case GREATER_THAN_OR_EQ:
    			if(v > maxVal){
    				return 0.0;
    			}
    			else if (v < minVal){
    				return 1.0;
    			}
    			for(int i = 0; i < bucketMinList.size(); i++){
    	    		if(v <= bucketMinList.get(i)){
    	    			numInLargerBuckets = 0; 
    	    			for(int j = i+1; j < bucketList.size(); j++){
    	    				numInLargerBuckets += bucketList.get(j).size();
    	    			}
    	    			int bRight = 0;
    	    			if(i+1 == bucketMinList.size()){
    	    				bRight = maxVal;
    	    			}
    	    			else{
    	    				bRight = bucketMinList.get(i+1)-1;
    	    			}
    	    			double numToRightInBucket = (((bRight-v)+1) * (bucketList.get(i).size()/bucketWidth));
    	    			result = (numToRightInBucket+numInLargerBuckets)/numTuples;
    	    			break;
    	    		}
    	    	}
    			break;
    		case LESS_THAN:
    			if(v > maxVal){
    				return 1.0;
    			}
    			else if (v < minVal){
    				return 0.0;
    			}
    			numInSmallerBuckets = 0;
    			for(int i = 0; i < bucketMinList.size(); i++){
    	    		if(v <= bucketMinList.get(i)){
    	    			int bLeft = bucketMinList.get(i);
    	    			double numToLeftInBucket = ((v-bLeft) * (bucketList.get(i).size()/bucketWidth));
    	    			result = (numToLeftInBucket+numInSmallerBuckets)/numTuples;
    	    			break;
    	    		}
    	    		else{
    	    			numInSmallerBuckets += bucketList.get(i).size();
    	    		}
    	    	}
    			break;
    		case LESS_THAN_OR_EQ:
    			if(v > maxVal){
    				return 1.0;
    			}
    			else if (v < minVal){
    				return 0.0;
    			}
    			numInSmallerBuckets = 0;
    			for(int i = 0; i < bucketMinList.size(); i++){
    	    		if(v <= bucketMinList.get(i)){
    	    			int bLeft = bucketMinList.get(i);
    	    			double numToLeftInBucket = (((v-bLeft)+1) * (bucketList.get(i).size()/bucketWidth));
    	    			result = (numToLeftInBucket+numInSmallerBuckets)/numTuples;
    	    			break;
    	    		}
    	    		else{
    	    			numInSmallerBuckets += bucketList.get(i).size();
    	    		}
    	    	}
    			break;
    		default:
    			return -1.0;
    	}
    	//System.out.println(result);
    	return result;
        
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
    	String histo = "";
    	for(int i = 0; i < bucketMinList.size(); i++){
    		histo += "Bucket " + i + '\n';
    		histo += '\t' + "Min Value: " + bucketMinList.get(i) + '\n';
    		histo += '\t' + "Tuples in bucket: " + bucketList.get(i).size() + '\n';
    	}
        return histo;
    }
}
