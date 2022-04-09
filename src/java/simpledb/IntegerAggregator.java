package simpledb;

import java.util.*;

import static simpledb.Aggregator.Op.AVG;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfieldNo;
    private Type gbfieldType;
    private Op aggregationOp;
    private int afieldNo;
    private List<Tuple> groups;//也可不建立此list，当需要iterate时根据group直接生成new Tuple，实现方式具见StringAggregator
    private HashMap<Field,group> gfieldToGroup;//HashMap支持Key为null
    private TupleDesc td;

    class group{
        private int  AVG_count;
        private int aggregateVal;
        private int index;//该group在groups中的index
        private group(int index,int aggregateVal){
            this.AVG_count=1;
            this.aggregateVal=aggregateVal;
            this.index=index;
        }
        private void update(int newAggregateVal){
            this.AVG_count++;
            switch (aggregationOp){
                case AVG:
                    this.aggregateVal+=newAggregateVal;
                    break;
                case MAX:
                    this.aggregateVal=Math.max(this.aggregateVal,newAggregateVal);
                    break;
                case MIN:
                    this.aggregateVal=Math.min(this.aggregateVal,newAggregateVal);
                    break;
                case COUNT:
                    this.aggregateVal=this.AVG_count;
                    break;
                case SUM:
                    this.aggregateVal+=newAggregateVal;
                    break;
            }
        }
        private int getValue(){
            if(aggregationOp.equals(AVG)) return this.aggregateVal/this.AVG_count;
            else return this.aggregateVal;
        }
    }
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
        this.afieldNo=afield;
        this.aggregationOp=what;
        this.gbfieldNo=gbfield;
        this.gbfieldType=gbfieldtype;
        this.groups=new ArrayList<>();
        this.gfieldToGroup=new HashMap<>();
        this.td=null;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
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

        Field gfield = tup.getField(this.gbfieldNo);//若gbfieldNo==Aggregator.NO_GROUPING,gfield=null
        //添加或更新gfieldToGroup
        if(!gfieldToGroup.containsKey(gfield))
            gfieldToGroup.put(gfield,
                    new group(gfieldToGroup.size(),((IntField)tup.getField(this.afieldNo)).getValue()));
        else
            gfieldToGroup.get(gfield).
                    update(((IntField)tup.getField(this.afieldNo)).getValue());
        //创建新Tuple
        Tuple newTp = new Tuple(this.td);
        if(gbfieldNo!=Aggregator.NO_GROUPING) {
            newTp.setField(0,tup.getField(this.gbfieldNo));
            newTp.setField(1,new IntField(gfieldToGroup.get(gfield).getValue()));
        }
        else{
            newTp.setField(0,new IntField(gfieldToGroup.get(gfield).getValue()));
        }
        //创建或更新groups(List<Tuple>)
        if(gfieldToGroup.get(gfield).index==groups.size()) groups.add(newTp);
        else groups.set(gfieldToGroup.get(gfield).index,newTp);
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        //直接通过groups遍历
        final int[] nowIndex = new int[1];
        return new OpIterator() {
            @Override
            public void open() throws DbException, TransactionAbortedException {
                nowIndex[0] = 0;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return (nowIndex[0] < groups.size()) && (nowIndex[0] >= 0);
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(!this.hasNext()) throw new NoSuchElementException();
                return groups.get(nowIndex[0]++);
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
                nowIndex[0]=-1;
            }
        };
        //throw new
       // UnsupportedOperationException("please implement me for lab2");
    }

}
