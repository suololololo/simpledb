package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int min;
    private int max;
    private int ntups;
    private int[] bucketList;
    private final double width;

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        this.min = min;
        this.max = max;
        this.bucketList = new int[buckets];
//        this.width = (1 + max - min) * 1.0 / buckets;
        this.width = (1. + max - min) / buckets;
        ntups = 0;

    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        if (v >= min && v <= max) {
            bucketList[getIndex(v)]++;
            ntups++;
        }

    }


    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        if (op == Predicate.Op.LESS_THAN) {
            if (v <= min) {
                return 0.0;
            }
            if (v >= max) {
                return 1.0;
            }
            int index = getIndex(v);
//                double sum = 1.0 * bucketList[index] * (v - index * width - min) / width;
//                for (int i = index - 1; i >= 0; i--) {
//                    sum += bucketList[i];
//                }
//                return sum / ntups;
            double cnt = 0;
            for (int i = 0; i < index; ++i) {
                cnt += bucketList[i];
            }
            cnt += bucketList[index] / width * (v - index * width - min);
            return cnt / ntups;
        } else if (op == Predicate.Op.EQUALS) {
            return estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v) - estimateSelectivity(Predicate.Op.LESS_THAN, v);
        } else if (op == Predicate.Op.GREATER_THAN) {
            return 1 - estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v);
        } else if (op == Predicate.Op.NOT_EQUALS) {
            return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
        } else if (op == Predicate.Op.LESS_THAN_OR_EQ) {
            return estimateSelectivity(Predicate.Op.LESS_THAN, v + 1);
        } else if (op == Predicate.Op.GREATER_THAN_OR_EQ) {
            return estimateSelectivity(Predicate.Op.GREATER_THAN, v - 1);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        // some code goes here
        int cnt = 0;
        for (int bucket : bucketList) {
            cnt += bucket;
        }
        if (cnt == 0)
            return 0.0;
        return cnt * 1.0 / ntups;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return String.format("IntHistgram(buckets=%d, min=%d, max=%d",
                bucketList.length, min, max);

    }

    private int getIndex(int v) {
        if (v < min || v > max)
            throw new IllegalStateException("value out of range");
        return (int) ((v - min) / width);
    }

}
