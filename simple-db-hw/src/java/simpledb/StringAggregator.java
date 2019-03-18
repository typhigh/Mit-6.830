package simpledb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Object stringArr;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	if(what != Op.COUNT) {
    		throw new IllegalArgumentException();
    	}
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.what = what;
    	
    	if(gbfieldtype == null) {
    		stringArr = (Object) new Integer(0);
    	}
    	else if(gbfieldtype == Type.INT_TYPE){
    		stringArr = (Object) new TreeMap<Integer, Integer>();
    	}
    	else if(gbfieldtype == Type.STRING_TYPE) {
    		stringArr = (Object) new TreeMap<String, Integer>();
    	}
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	if(gbfieldtype == null) {
    		stringArr = (Object) ((Integer) stringArr + 1);
    	}
    	else if(gbfieldtype == Type.INT_TYPE) {
    		TreeMap<Integer, Integer> arr = (TreeMap<Integer, Integer>) stringArr;
    		Integer key = ((IntField) tup.getField(gbfield)).getValue();
    		if(!arr.containsKey(key)) {
    			arr.put(key, 0);
    		}
    		Integer val = arr.get(key);
    		val = val + 1;
    		arr.put(key, val);
    		//System.out.println(val + " " + arr.get(key));
    	}
    	else if(gbfieldtype == Type.STRING_TYPE) {
    		TreeMap<String, Integer> arr = (TreeMap<String, Integer>) stringArr;
    		String key = ((StringField) tup.getField(gbfield)).getValue();
    		if(!arr.containsKey(key)) {
    			arr.put(key, 0);
    		}
    		Integer val = arr.get(key);
    		val = val + 1;
    		arr.put(key, val);
    		//System.out.println(val + " " + arr.get(key));
    	}
    	else {
    		
    	}
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public class StringArrIter implements OpIterator {
    	private ArrayList<Tuple> tuples;
    	private Iterator<Tuple> iter;
    	private TupleDesc td;
    	public StringArrIter() {
    		iter = null;
    		if(gbfieldtype == null) {
    			td = new TupleDesc(new Type[] {Type.INT_TYPE});
    			tuples = new ArrayList<Tuple>(1);
    			Tuple e = new Tuple(td);
    			Integer i = (Integer) stringArr;
    			e.setField(0, new IntField(i));
    			tuples.add(e);
    		}
    		else if(gbfieldtype == Type.INT_TYPE) {
    			td = new TupleDesc(new Type[] {Type.INT_TYPE, Type.INT_TYPE});
    			TreeMap<Integer, Integer> arr = (TreeMap<Integer, Integer>) stringArr;
    			Set<Integer> keys = arr.keySet();
    			tuples = new ArrayList<Tuple> (keys.size());
    			for(Integer key: keys) {
    				Integer val = arr.get(key);
    				Tuple e = new Tuple(td);
    				e.setField(0, new IntField(key));
    				e.setField(1, new IntField(val));
    				tuples.add(e);
    			}
    		}
    		else if(gbfieldtype == Type.STRING_TYPE) {
    			td = new TupleDesc(new Type[] {Type.STRING_TYPE, Type.INT_TYPE});
    			TreeMap<String, Integer> arr = (TreeMap<String, Integer>) stringArr;
    			Set<String> keys = arr.keySet();
    			tuples = new ArrayList<Tuple> (keys.size());
    			for(String key: keys) {
    				Integer val = arr.get(key);
    				Tuple e = new Tuple(td);
    				e.setField(0, new StringField(key, key.length()));
    				e.setField(1, new IntField(val));
    				tuples.add(e);
    			} 
    		}
    	}
		@Override
		public void open() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			iter = tuples.iterator();
		}

		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			return iter.hasNext();
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
			// TODO Auto-generated method stub
			return iter.next();
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			close();
			open();
		}

		@Override
		public TupleDesc getTupleDesc() {
			// TODO Auto-generated method stub
			return td;
		}

		@Override
		public void close() {
			// TODO Auto-generated method stub
			iter = null;
		}
    	
    }
    public OpIterator iterator() {
        // some code goes here
        return new StringArrIter();
    }

}
