package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private OpIterator child;
    private TupleDesc td;

    private boolean isCalled;//插入操作的每个child仅被执行一次删除即可，故仅需使用bool记录当前是否执行过全部child的删除。
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.td = child.getTupleDesc();
        this.isCalled = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        child.open();
        super.open();
        this.isCalled = false;
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
        this.isCalled = true;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        this.isCalled=false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(isCalled) return null;
        int numdel= 0;
        while(child.hasNext()){
            try {
                Database.getBufferPool().deleteTuple(tid, child.next());
                numdel++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        isCalled=true;
        Tuple numdeleted=new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}));
        numdeleted.setField(0,new IntField(numdel));
        return numdeleted;
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
