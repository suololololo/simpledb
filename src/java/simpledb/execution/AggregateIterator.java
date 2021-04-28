package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public class AggregateIterator implements OpIterator {
    TupleDesc tupleDesc;
    protected Iterator<Map.Entry<Field, Integer>> it;
    private Map<Field, Integer> groupMap;
    protected Type itGbFieldType;

    public AggregateIterator(Map<Field, Integer> groupMap, Type itGbFieldType) {
        this.groupMap = groupMap;
        this.itGbFieldType = itGbFieldType;
        if (itGbFieldType == null) {
            // is null show that not group by  only have aggregateVal
            tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateVal"});
        } else {
            tupleDesc = new TupleDesc(new Type[]{this.itGbFieldType, Type.INT_TYPE}, new String[]{"groupValue", "aggregateValue"});
        }

    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.it = groupMap.entrySet().iterator();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return it.hasNext();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        Map.Entry<Field, Integer> entry = it.next();
        Field field = entry.getKey();
        Tuple tuple = new Tuple(tupleDesc);
        setFileds(tuple, entry.getValue(), field);
        return tuple;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.it = groupMap.entrySet().iterator();
    }

    @Override
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    @Override
    public void close() {
        this.it = null;
        this.tupleDesc = null;
    }

    void setFileds(Tuple tuple, int value, Field f) {
        if (f == null) {
            tuple.setField(0, new IntField(value));
        } else {
            tuple.setField(0, f);
            tuple.setField(1, new IntField(value));
        }
    }
}
