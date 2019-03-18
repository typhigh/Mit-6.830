package simpledb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    //private int noGroupRes;
    
    private Object intArr;
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

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.what = what;
    	if(gbfieldtype == null) {
    		intArr = (Object)(new ArrayList<Integer> ());
    	}
    	else if(gbfieldtype == Type.INT_TYPE) {
    		intArr = (Object)(new TreeMap<Integer, ArrayList<Integer>> ());
    	}
    	else if(gbfieldtype == Type.STRING_TYPE) {
    		intArr = (Object)(new TreeMap<String, ArrayList<Integer>> ());
    	}
    	//this.noGroupRes = 0;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    @SuppressWarnings("unchecked")
	public void mergeTupleIntoGroup(Tuple tup) {
    	// some code goes here
    	
    	if(gbfieldtype == null) {
    		((ArrayList<Integer>) intArr).add(((IntField)tup.getField(afield)).getValue());
    	}
    	else if(gbfieldtype == Type.INT_TYPE) {
    		TreeMap<Integer, ArrayList<Integer>> arr = (TreeMap<Integer, ArrayList<Integer>>) intArr;
    		int arrval = ((IntField) tup.getField(afield)).getValue();
    		int groupval = ((IntField) tup.getField(gbfield)).getValue();
    		if(!arr.containsKey(groupval)) {
    			arr.put(groupval, new ArrayList<Integer>());
    		}
    		ArrayList<Integer> l = arr.get(groupval);
    		l.add(arrval);
    	}
    	else {
    		TreeMap<String, ArrayList<Integer>> arr = (TreeMap<String, ArrayList<Integer>>) intArr;
    		int arrval = ((IntField) tup.getField(afield)).getValue();
    		String groupval = ((StringField) tup.getField(gbfield)).getValue();
    		if(!arr.containsKey(groupval)) {
    			arr.put(groupval, new ArrayList<Integer>());
    		}
    		ArrayList<Integer> l = arr.get(groupval);
    		l.add(arrval);
    	}
    	
    }
    
    
 
    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    
    public class IntegerAggIter implements OpIterator{
    	private ArrayList<Tuple> tuples;
    	private Iterator<Tuple> iter;
		private TupleDesc td;
    	public IntegerAggIter() {
    		if(gbfieldtype == null) {
    			td = new TupleDesc(new Type[] {Type.INT_TYPE});
    			ArrayList<Integer> arr = (ArrayList<Integer>) intArr;
    			tuples = new ArrayList<Tuple>(1);
    			Tuple e = new Tuple(td);
    			e.setField(0, new IntField(calc(arr)));
    			tuples.add(e);
    		}
    		else if(gbfieldtype == Type.INT_TYPE){
    			td = new TupleDesc(new Type[] {Type.INT_TYPE, Type.INT_TYPE});
    			TreeMap<Integer, ArrayList<Integer>> arr = (TreeMap<Integer, ArrayList<Integer>>) intArr;
    			Set<Integer> keys = arr.keySet();
    			tuples = new ArrayList<Tuple>(keys.size());
    			for(Integer key : keys) {
    				ArrayList<Integer> a = arr.get(key);
    				Tuple e = new Tuple(td);
        			e.setField(0, new IntField(key));
        			e.setField(1, new IntField(calc(a)));
    				tuples.add(e);
    			}
    		}
    		else if(gbfieldtype == Type.STRING_TYPE) {
    			td = new TupleDesc(new Type[] {Type.STRING_TYPE, Type.INT_TYPE});
    			TreeMap<String, ArrayList<Integer>> arr = (TreeMap<String, ArrayList<Integer>>) intArr;
    			Set<String> keys = arr.keySet();
    			tuples = new ArrayList<Tuple>(keys.size());
    			for(String key: keys) {
    				ArrayList<Integer> a = arr.get(key);
    				Tuple e = new Tuple(td);
    				e.setField(0, new StringField(key, key.length()));
    				e.setField(1, new IntField(calc(a)));
    				tuples.add(e);
    			}
    		}
    		else {
    			td = null;
    		}
    		
    		
    	}
    	
    	private int calc(ArrayList<Integer> a) {
    		int ans;
    		switch(what) {
    		case MIN:
    			ans = a.get(0);
				for(int val : a) {
    				ans = Math.min(ans, val);
    			}
				return ans;
    		case MAX:
    			ans = a.get(0);
    			for(int val: a) {
    				ans = Math.max(ans, val);
    			}
    			return ans;
    		case SUM:
    			ans = 0;
    			for(int val: a) {
    				ans += val;
    			}
    			return ans;
    		case AVG:
    			ans = 0;
    			for(int val: a) {
    				ans += val;
    			}
    			return ans / a.size();
    		case COUNT:
    			return a.size();
    		default:
    			return 0;
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
    	return new IntegerAggIter();
    }

}
