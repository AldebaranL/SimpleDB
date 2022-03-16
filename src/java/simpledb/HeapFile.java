package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    //List<HeapPage> heapPages;
    File file;
    TupleDesc tupleDesc;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td){
        // some code goes here
        //heapPages = new ArrayList<>();
        //InputStream in = new FileInputStream(f);
        this.file=f;
        this.tupleDesc=td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
        //throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
        //throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        Page page = null;
        try(RandomAccessFile file = new RandomAccessFile(this.file,"r")){
            int pos = pid.getPageNumber() * BufferPool.getPageSize();
            file.seek(pos);
            byte[] pageData = new byte[BufferPool.getPageSize()];
            file.read(pageData,pos,pageData.length);
            page = new HeapPage((HeapPageId) pid,pageData);
            file.close();
        }
        catch(IOException e){
            e.printStackTrace();
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil(this.file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
        /*DbFileIterator iterator;
        public void open() throws DbException, TransactionAbortedException{

        }
        PageId pid;
        for(int pageNo = 0; pageNo < this.numPages(); pageNo++){

            Page page = BufferPool.getPage(tid, new HeapPageId(this.getId(), pageNo), Permissions.READ_WRITE);
        }

        return null;*/
    }

}
/**
 * Helper class that implements the Java Iterator for tuples on a HeapFile.
 */
class HeapFileIterator extends AbstractDbFileIterator {

    Iterator<Tuple> it = null;
    HeapPage curp = null;

    TransactionId tid;
    HeapFile f;

    /**
     * Constructor for this iterator
     * @param f - the BTreeFile containing the tuples
     * @param tid - the transaction id
     */
    public HeapFileIterator(HeapFile f, TransactionId tid) {
        this.f = f;
        this.tid = tid;
    }

    /**
     * Open this iterator by getting an iterator on the first page,
     * Remind that the heapPages in heapfile are arranged in natural order,
     * which will be accessed in ascending order of natural number.
     * (I'm not sure if it is the expected implementation of HeapfileIterator.
     * As Heap is able to be stored in an array, which can be added
     * heap-adjustment-method later on, I think it's fine for now.)
     * --By lyy, 20220316
     */
    public void open() throws DbException, TransactionAbortedException {
        PageId pid;
        int pageNo = 0;
        //for(int pageNo = 0; pageNo < this.f.numPages(); pageNo++){

        curp = (HeapPage) Database.getBufferPool().getPage(tid,
                            new HeapPageId(this.f.getId(), pageNo),
                            Permissions.READ_ONLY);
        //}
       // BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) Database.getBufferPool().getPage(
       //         tid, BTreeRootPtrPage.getId(f.getId()), Permissions.READ_ONLY);
       //BTreePageId root = rootPtr.getRootId();
        //curp = f.findLeafPage(tid, root, Permissions.READ_ONLY, null);
        it = curp.iterator();
    }

    /**
     * Read the next tuple either from the current page if it has more tuples or
     * from the next page by following the right sibling pointer.
     *
     * @return the next tuple, or null if none exists
     */
    @Override
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        //1.当前page未读完的情形, 无需处理
        //2.当前page已读完的情形, 换页
        if (it != null && !it.hasNext())
            it = null;
        while (it == null && curp != null) {//以防某些Page无tuple，翻页，读到有为止
            int nextPageNo = curp.getId().getPageNumber() + 1;
            if(nextPageNo >= this.f.numPages()) {
                curp = null;//finished all pages
            }
            else {
                curp = (HeapPage) Database.getBufferPool().getPage(tid,
                        new HeapPageId(this.f.getId(), nextPageNo),
                        Permissions.READ_ONLY);//read next page
                it = curp.iterator();
                if (!it.hasNext())
                    it = null;
            }
        }
        if (it == null)
            return null;
        return it.next();
    }
    /**
     * rewind this iterator back to the beginning of the tuples
     */
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }
    /**
     * close the iterator
     */
    public void close() {
        super.close();
        it = null;
        curp = null;
    }
}
