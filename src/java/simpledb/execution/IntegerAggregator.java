package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Field, Integer> groupMap;
    private Map<Field, Integer> countMap;
    private Map<Field, List<Integer>> avgMap;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        groupMap = new HashMap<>();
        countMap = new HashMap<>();
        avgMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        // get the field which want to  group by
        IntField intField = (IntField) tup.getField(afield);
        // get the field which tuple was grouped by
        Field gbfield = this.gbfield == NO_GROUPING ? null : tup.getField(this.gbfield);
        int value = intField.getValue();
        if (gbfield != null && gbfield.getType() != gbfieldtype) {
            throw new IllegalStateException("Given type is wrong type");
        }
        switch (this.what) {
            case AVG:
                if (!avgMap.containsKey(gbfield)) {
                    List<Integer> list = new ArrayList<>();
                    list.add(value);
                    avgMap.put(gbfield, list);
                } else {
                    List<Integer> list = avgMap.get(gbfield);
                    list.add(value);
                }
                break;
            case MAX:
                if (!groupMap.containsKey(gbfield)) {
                    groupMap.put(gbfield, value);
                } else {
                    int temp = groupMap.get(gbfield);
                    if (temp < value) {
                        groupMap.put(gbfield, value);
                    }
                }
                break;
            case MIN:
                if (!groupMap.containsKey(gbfield)) {
                    groupMap.put(gbfield, value);
                } else {
                    int temp = groupMap.get(gbfield);
                    if (value < temp) {
                        groupMap.put(gbfield, value);
                    }
                }
                break;

            case SUM:
                if (!groupMap.containsKey(gbfield)) {
                    groupMap.put(gbfield, value);
                } else {
                    groupMap.put(gbfield, groupMap.get(gbfield) + value);
                }
                break;
            case COUNT:
                if (!groupMap.containsKey(gbfield)) {
                    groupMap.put(gbfield, 1);
                } else {
                    groupMap.put(gbfield, groupMap.get(gbfield) + 1);
                }
                break;
            case SC_AVG:
            case SUM_COUNT:
                if (!groupMap.containsKey(gbfield)) {
                    groupMap.put(gbfield, value);
                    countMap.put(gbfield, 1);
                } else {
                    groupMap.put(gbfield, groupMap.get(gbfield) + value);
                    countMap.put(gbfield, countMap.get(gbfield) + 1);
                }
            default:
                throw new IllegalStateException("Aggregate not supported");

        }


    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new IntAggIterator();
    }

    private class IntAggIterator extends AggregateIterator {
        private Iterator<Map.Entry<Field, List<Integer>>> avgIterator;
        private boolean isAvg;
        private boolean isSumCount;
        private boolean isSCAvg;

        public IntAggIterator() {
            super(groupMap, gbfieldtype);
            isAvg = what.equals(Op.AVG);
            isSCAvg = what.equals(Op.SC_AVG);
            isSumCount = what.equals(Op.SUM_COUNT);
            if (isSumCount) {
                this.tupleDesc = new TupleDesc(new Type[]{this.itGbFieldType, Type.INT_TYPE, Type.INT_TYPE}, new String[]{"grooupVal", "sumVal","countVal" });
            }
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            super.open();
            if (isAvg || isSumCount) {
                this.avgIterator = avgMap.entrySet().iterator();
            } else {
                this.avgIterator = null;
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (isAvg || isSumCount) {
                return avgIterator.hasNext();
            }
            return super.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            Tuple tuple = new Tuple(tupleDesc);
            if (isAvg || isSumCount) {
                Map.Entry<Field,List<Integer>> entry = avgIterator.next();
                Field field = entry.getKey();
                List<Integer> avgOrSumCountList = entry.getValue();
                if (this.isAvg) {
                    int value = sumList(avgOrSumCountList) / avgOrSumCountList.size();
                    setFileds(tuple,value,field);
                } else {
                    this.setFileds(tuple,sumList(avgOrSumCountList),field);
                    if ( field != null) {
                        tuple.setField(2,new IntField(avgOrSumCountList.size()));
                    } else {
                        tuple.setField(1,new IntField(avgOrSumCountList.size()));
                    }
                }
                return tuple;
            } else if (isSCAvg) {
                Map.Entry<Field,Integer> entry = it.next();
                Field field = entry.getKey();
                setFileds(tuple,entry.getValue() / countMap.get(field),field);
                return tuple;
            }
            return super.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            super.rewind();
            if (isAvg || isSumCount) {
                avgIterator = avgMap.entrySet().iterator();
            }

        }

        @Override
        public void close() {
            super.close();
            if (isAvg || isSumCount) {
                avgIterator = null;
            }
        }

        private int sumList(List<Integer> list) {
            int sum = 0;
            for (int i : list) {
                sum += i;
            }
            return sum;
        }
    }

}
