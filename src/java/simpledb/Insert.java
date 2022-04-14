package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private OpIterator child;
    private int tableId;
    private TupleDesc td;

    private boolean isCalled;//插入操作的每个child仅被执行一次插入即可，故仅需使用bool记录当前是否执行过全部child的插入。
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        if(!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId)))
            throw new DbException("TupleDesc doesn't match");
        this.tid=t;
        this.child=child;
        this.tableId=tableId;
        this.td=new TupleDesc(new Type[]{Type.INT_TYPE},new String[]{null});
        this.isCalled=false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        child.open();
        super.open();
        isCalled=false;
    }

    public void close() {
        // some code goes here
        child.close();
        super.close();
        isCalled=true;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        isCalled=false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(isCalled) return null;
        int numIns = 0;
        while(child.hasNext()){
            try {
                Database.getBufferPool().insertTuple(tid, tableId, child.next());
                numIns++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        isCalled=true;
        Tuple numInserted=new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}));
        numInserted.setField(0,new IntField(numIns));
        return numInserted;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child=children[0];
    }
}
