package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.*;
/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    private ConcurrentHashMap<Integer, IntHistogram> intFieldStats;
    private ArrayList<Integer> maxValue;
    private ArrayList<Integer> minValue;
    private ConcurrentHashMap<Integer, StringHistogram> stringFieldStats;
    private int numPages;
    private double IOCost;
    private double numTuples;
    TupleDesc desc;
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
    	
    	HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
    	HeapFileIterator fileIt = (HeapFileIterator) file.iterator(null);
    	numPages = file.numPages();
    	desc = file.getTupleDesc();
    	numTuples = 0;
    	IOCost = ioCostPerPage;
    	
    	intFieldStats = new ConcurrentHashMap<Integer, IntHistogram>();
    	maxValue = new ArrayList<Integer>();
    	minValue = new ArrayList<Integer>();
    	for(int i = 0; i < desc.numFields(); i++){
    		maxValue.add(null);
    		minValue.add(null);
    	}
    	stringFieldStats = new ConcurrentHashMap<Integer, StringHistogram>();
    	

    	try{
    		fileIt.open();
    		while(fileIt.hasNext()){
    			Tuple next = fileIt.next();
    			for(int i = 0; i < desc.numFields(); i++){
    				if(next.getField(i).getType() == Type.INT_TYPE){
    					if(maxValue.get(i) == null){
    						maxValue.set(i, ((IntField)next.getField(i)).getValue());
    					}
    					else if(maxValue.get(i) < ((IntField)next.getField(i)).getValue()){
    						maxValue.set(i, ((IntField)next.getField(i)).getValue());
    					}
    					if(minValue.get(i) == null){
    						minValue.set(i, ((IntField)next.getField(i)).getValue());
    					}
    					else if(minValue.get(i) > ((IntField)next.getField(i)).getValue()){
    						minValue.set(i, ((IntField)next.getField(i)).getValue());
    					}
    				}
    			}
    		}
    		
    		for(int i = 0; i < desc.numFields(); i++){
    			if(desc.getFieldType(i) == Type.INT_TYPE){
    				intFieldStats.put(i, new IntHistogram(NUM_HIST_BINS, minValue.get(i), maxValue.get(i)));
    			}
    			else{
    				stringFieldStats.put(i, new StringHistogram(NUM_HIST_BINS));
    			}
    		}
    		
    		fileIt.rewind();
    		while(fileIt.hasNext()){
    			Tuple next = fileIt.next();
    			numTuples++;
    			for(int i = 0; i < desc.numFields(); i++){
    				if(next.getField(i).getType() == Type.INT_TYPE){
    					intFieldStats.get(i).addValue(((IntField)next.getField(i)).getValue());
    				}
    				else{
    					stringFieldStats.get(i).addValue(((StringField)next.getField(i)).getValue());
    				}
    			}
    		}
    	}
    	catch (Exception e){
    		e.printStackTrace();
    	}
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return IOCost * numPages;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
    	int result = (int)(numTuples*selectivityFactor);
        return result;
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
    	if(desc.getFieldType(field) == Type.INT_TYPE){
    		return(intFieldStats.get(desc.getFieldName(field)).avgSelectivity());
    	}
    	else{
    		return(stringFieldStats.get(desc.getFieldName(field)).avgSelectivity());
    	}
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
    	if(desc.getFieldType(field) == Type.INT_TYPE){
    		return(intFieldStats.get(field).estimateSelectivity(op, ((IntField)constant).getValue()));
    	}
    	else{
    		return(stringFieldStats.get(field).estimateSelectivity(op, ((StringField)constant).getValue()));
    	}
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return (int)numTuples;
    }

}
