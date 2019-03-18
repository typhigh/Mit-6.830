package simpledb;

import java.util.*;

import simpledb.Aggregator.Op;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;
    private Aggregator arr;
    private OpIterator iter;
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
    	this.child = child;
    	this.afield = afield;
    	this.gfield = gfield;
    	this.aop = aop;
    	this.iter = null;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
    	return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
    	if(gfield == -1) return null;
    	else 
    		return child.getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
    	return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
    	return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
    	return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
    	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	// some code goes here
    	Type afieldtype = child.getTupleDesc().getFieldType(afield);
    	Type gfieldtype;
    	if(gfield == -1) 
    		gfieldtype = null;
    	else 
    		gfieldtype = child.getTupleDesc().getFieldType(gfield);
    	if(afieldtype == Type.INT_TYPE) {
    		arr = (Aggregator) new IntegerAggregator(gfield, gfieldtype, afield, aop);
    	}
    	else if(afieldtype == Type.STRING_TYPE) {
    		arr = (Aggregator) new StringAggregator(gfield, gfieldtype, afield, aop);
    	}
    	
    	child.open();
    	while(child.hasNext()) {
    		Tuple e = child.next();
    		arr.mergeTupleIntoGroup(e);
    	}
    	child.close();
    	super.open();
    	iter = arr.iterator();
    	iter.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
    	if(iter.hasNext()) {
    		return iter.next();
    	}
    	return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
    	close();
    	open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
    	TupleDesc td = child.getTupleDesc();
    	String afieldname = td.getFieldName(afield);
    	
    	if(gfield == -1) {
    		return new TupleDesc(
    				new Type[] {td.getFieldType(afield)},
    				new String[] {aop.toString() + "(" + afieldname + ")"}
    				);
    	}
    	else {
    		return new TupleDesc(
    				new Type[] {td.getFieldType(gfield), td.getFieldType(afield)},
    				new String[] {td.getFieldName(gfield), 
    						aop.toString() + "(" + afieldname + ")"}
    				);
    	}
    }

    public void close() {
	// some code goes here
    	super.close();
    	child.close();
    }

    @Override
    public OpIterator[] getChildren() {
	// some code goes here
    	return new OpIterator[] {child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
	// some code goes here
    	if(children[0] != child) {
    		child = children[0];
    	}
    }
    
}
