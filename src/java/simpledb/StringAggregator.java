package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfieldNo;
    private Type gbfieldType;
    private Op aggregationOp;
    private int afieldNo;
    private HashMap<Field, group> gfieldToGroup;
    private TupleDesc td;

    class group{
        private int count;
        private group(){
            this.count=1;
        }
        private void update(Op aggregationOp){
            if (aggregationOp == Op.COUNT) {
                this.count++;
            } else {
                throw new IllegalArgumentException();
            }
        }
    }
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
        this.afieldNo=afield;
        this.aggregationOp=what;
        this.gbfieldNo=gbfield;
        this.gbfieldType=gbfieldtype;
        this.gfieldToGroup=new HashMap<>();
        this.td=null;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gfield = tup.getField(this.gbfieldNo);
        //添加或更新gfieldToGroup
        if(!gfieldToGroup.containsKey(tup.getField(this.gbfieldNo))) gfieldToGroup.put(gfield,new group());
        else gfieldToGroup.get(gfield).update(this.aggregationOp);

        //若首次merge需要创建TupleDesc
        if(this.td==null){
            Type[] typeAr;
            String[] fieldAr;
            if(gbfieldNo==Aggregator.NO_GROUPING) {
                typeAr = new Type[]{Type.INT_TYPE};
                fieldAr = new String[]{tup.getTupleDesc().getFieldName(this.afieldNo)};
            }
            else{
                typeAr=new Type[]{this.gbfieldType,Type.INT_TYPE};
                fieldAr=new String[]{tup.getTupleDesc().getFieldName(this.gbfieldNo),tup.getTupleDesc().getFieldName(this.afieldNo)};
            }
            this.td=new TupleDesc(typeAr,fieldAr);
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

    Iterator<Field> gbfieldIt;
    public OpIterator iterator() {
        // some code goes here
        //通过gfieldToGroup的Key集合（Field）遍历全部group
        return new OpIterator() {
            @Override
            public void open() throws DbException, TransactionAbortedException {
                gbfieldIt = gfieldToGroup.keySet().iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return gbfieldIt.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(!this.hasNext()) throw new NoSuchElementException();
                Field nextField = gbfieldIt.next();
                //创建新Tuple
                Tuple newTp = new Tuple(td);
                if(gbfieldNo!=Aggregator.NO_GROUPING) {
                    newTp.setField(0,nextField);
                    newTp.setField(1,new IntField(gfieldToGroup.get(nextField).count));
                }
                else{
                    newTp.setField(0,new IntField(gfieldToGroup.get(nextField).count));
                }
                return newTp;
                //return null;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                this.close();
                this.open();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return td;
            }

            @Override
            public void close() {
                gbfieldIt=null;
            }
        };
    }

}
