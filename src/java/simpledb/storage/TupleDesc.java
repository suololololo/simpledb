package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;

import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable,Cloneable {
    private       List<TDItem>  list  = new ArrayList<>();

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;

        /**
         * The name of the field
         * */
        public final String fieldName;
//        public  String fieldName;
        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return  list.iterator();

    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        int length = typeAr.length;
        if(fieldAr!=null){
            for(int i = 0; i < length ; i++){
                list.add(new TDItem(typeAr[i],fieldAr[i]));
            }
        }
        else {
            for(int i = 0; i < length ; i++){
                list.add(new TDItem(typeAr[i],null));
            }
        }

    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this(typeAr,null);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return list.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        try {
            return  list.get(i).fieldName;
        }
        catch (NoSuchElementException e){
            throw  new NoSuchElementException();
        }
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        try {
            return  list.get(i).fieldType;
        }
        catch (NoSuchElementException e){
            throw  new NoSuchElementException();
        }
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        if(name==null){
            throw   new NoSuchElementException();
        }
        // some code goes here
        try {
            for(int i = 0 ; i< list.size();i++){
                if (list.get(i).fieldName==null){

                }
                else if(list.get(i).fieldName.equals(name)){
                    return  i;
                }
            }
            throw   new NoSuchElementException();
        }
        catch (NoSuchElementException e){
            throw   new NoSuchElementException();
        }

    }

    /**
     * tuples jilu de  zijiedaxiao
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int result = 0 ;
        for(int i = 0 ; i< list.size();i++){
            result +=list.get(i).fieldType.getLen();
        }
        return result;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        Type[]  type = new Type[td1.list.size()+td2.list.size()];
        String[] sr = new String[td1.list.size()+td2.list.size()];
        for(int i =0; i < td1.list.size();i++){
            type[i] =  td1.list.get(i).fieldType;
            sr[i] = td1.list.get(i).fieldName;
        }
        int length = td1.list.size();
        for(int i = 0 ; i < td2.list.size();i++){
            type[i+length] = td2.list.get(i).fieldType;
            sr[i+length]  = td2.list.get(i).fieldName;
        }
        return  new TupleDesc(type,sr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
//        if(!(o instanceof  TupleDesc)){
//            return  false;
//        }
//        else {
//            TupleDesc temp = (TupleDesc) o;
//            if(this.getSize()!=temp.getSize()){
//                return  false;
//            }
//            for(int i = 0 ; i< temp.list.size();i++){
//                if (!temp.list.get(i).toString().equals(this.list.get(i).toString()))
//                    return  false;
//            }
//        }
//        return true;
        if(this.getClass().isInstance(o)) {
            TupleDesc two = (TupleDesc) o;
            if (numFields() == two.numFields()) {
                for (int i = 0; i < numFields(); ++i) {
                    if (!list.get(i).fieldType.equals(two.list.get(i).fieldType)) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;

    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    @Override
    public String toString() {
//        return "TupleDesc{" +
//                "list=" + list +
//                '}';
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<list.size()-1;++i){
            sb.append(list.get(i).fieldName + "(" + list.get(i).fieldType + "), ");
        }
        sb.append(list.get(list.size()-1).fieldName + "(" + list.get(list.size()-1).fieldType + ")");
        return sb.toString();
    }

    public TupleDesc clone(String prefix) throws  CloneNotSupportedException{
        TupleDesc tupleDesc = (TupleDesc) super.clone();
        tupleDesc.list = new ArrayList<>();

        for (Iterator iterator = this.iterator();iterator.hasNext();){
            TDItem old = (TDItem) iterator.next();
            tupleDesc.list.add(new TDItem(old.fieldType,prefix+"."+old.fieldName));
        }
        return tupleDesc;
    }
}
