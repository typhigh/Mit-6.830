package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

	private int buckets;
	private int min;
	private int max;
	private int ntups;
	private int height[];
	private int cw;
	private int lastw;
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
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
    	this.buckets = buckets;
    	this.min = min;
    	this.max = max;
    	this.height = new int[buckets];
    	for(int i = 0; i < buckets; ++i) 
    		this.height[i] = 0;
    	this.cw = (max - min + 1) / buckets;
    	this.cw = Math.max(1, this.cw);
    	this.lastw = (max - min + 1) - (buckets - 1) * cw;
    	this.ntups = 0;
    	assert(cw > 0);
    	
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
    	assert(v >= min && v <= max);
    	int bucket = Math.min((v - min) / cw, buckets - 1);
    	++ntups;
    	++height[bucket];
    }

    private double estimateEQ(int b, int w, int v) {
    	if(v > max || v < min) return 0;
    	return 1.0 * height[b] / w / ntups; 
    }
    
    private double estimateLT(int b, int w, int v) {
    	if(v > max) return 1.0;
    	if(v <= min) return 0;
    	double ret = 0;
    	for(int i = 0; i < b; ++i) {
    		ret += height[i];
    	}
    	int lval = (cw * b) + min;
    	int cnt = v - lval;
    	assert(cnt >= 0);
    	ret += 1.0 * height[b] * cnt / w;
    	ret /= ntups;
    	return ret;
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

    	// some code goes here
    	if(ntups == 0) {
    		return 0;
    	}
    	double ans = 0;
    	int bucket = Math.min((v - min) / cw, buckets - 1); 
    	int w = bucket < buckets - 1 ? cw : lastw;
    	
    	
    	switch(op) {
    	case EQUALS:
    		ans = estimateEQ(bucket, w, v);
    		break;
    	case GREATER_THAN:
    		ans = 1.0 - estimateLT(bucket, w, v) - estimateEQ(bucket, w, v);
    		break;
    	case LESS_THAN:
    		ans = estimateLT(bucket, w, v);
    		break;
    	case LESS_THAN_OR_EQ:
    		ans = estimateLT(bucket, w, v) + estimateEQ(bucket, w, v);
    		break;
    	case GREATER_THAN_OR_EQ:
    		ans = 1.0 - estimateLT(bucket, w, v);
    		break;
    	case NOT_EQUALS:
    		ans = 1.0 - estimateEQ(bucket, w, v);
    		break;
    	default:
    		return -1;
    	}
    	//System.out.println(v+" "+ans);
        return ans;
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
        // some code goes here
        return height.toString();
    }
}
