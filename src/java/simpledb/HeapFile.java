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
        //System.out.println(this.file.getAbsoluteFile().hashCode());
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
        //System.out.println("2");
        try(RandomAccessFile rafile = new RandomAccessFile(this.file,"r")){

            int pos = pid.getPageNumber() * BufferPool.getPageSize();
            //Database.getBufferPool();
            byte[] pageData = new byte[BufferPool.getPageSize()];
            rafile.seek(pos);
            rafile.read(pageData,0,pageData.length);
            page = new HeapPage((HeapPageId) pid,pageData);
        }
        catch(IOException e){
            e.printStackTrace();
        }
        return page;
    }

    // see DbFile.java for javadocs
    /**
     * Push the specified page to disk.
     *
     * @param page The page to write.  page.getId().pageno() specifies the offset into the file where the page should be written.
     * @throws IOException if the write fails
     *
     */
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pageNo=page.getId().getPageNumber();
       // System.out.println(BufferPool.getPageSize());
        if(pageNo>numPages()||pageNo<0) throw new IndexOutOfBoundsException();

        RandomAccessFile raf=new RandomAccessFile(file,"rw");
        raf.seek(pageNo*BufferPool.getPageSize());
        raf.write(page.getPageData());
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil(this.file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    /**
     * Inserts the specified tuple to the file on behalf of transaction.
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     *
     * @param tid The transaction performing the update
     * @param t The tuple to add.  This tuple should be updated to reflect that
     *          it is now stored in this file.
     * @return An ArrayList contain the pages that were modified
     * @throws DbException if the tuple cannot be added
     * @throws IOException if the needed file can't be read/written
     */
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        //return null;
        //System.out.println("Heapfile.insert");
        ArrayList<Page> modifiedPages = new ArrayList<>();
        for(int i=0;i<numPages();i++){
            HeapPage nowPage=(HeapPage)Database.getBufferPool().getPage(tid,new HeapPageId(getId(),i),Permissions.READ_WRITE);
            if(nowPage.getNumEmptySlots()!=0) {
                nowPage.insertTuple(t);//t???RecordId????????????????????????
                modifiedPages.add(nowPage);
                //System.out.println("exist free page");
                return modifiedPages;
            }
        }
        //System.out.println("no pages is free");
        //if no pages are free,add page first
        HeapPage nowPage = ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(),numPages()),Permissions.READ_WRITE));
        nowPage.insertTuple(t);
        modifiedPages.add(nowPage);
        this.writePage(nowPage);
        return modifiedPages;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    /**
     * Removes the specified tuple from the file on behalf of the specified
     * transaction.
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     *
     * @param tid The transaction performing the update
     * @param t The tuple to delete.  This tuple should be updated to reflect that
     *          it is no longer stored on any page.
     * @return An ArrayList contain the pages that were modified
     * @throws DbException if the tuple cannot be deleted or is not a member
     *   of the file
     */
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> modifiedPages=new ArrayList<>();
        HeapPage nowPage=(HeapPage)Database.getBufferPool().getPage(tid,t.getRecordId().getPageId(),Permissions.READ_WRITE);
        nowPage.deleteTuple(t);//Exception will be throw out in this func
        modifiedPages.add(nowPage);
        return modifiedPages;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

}
/**
 * Helper class that implements the Java Iterator for tuples on a HeapFile.
 * By lyy
 */
class HeapFileIterator extends AbstractDbFileIterator {

    Iterator<Tuple> it = null;
    HeapPage curp = null;

    TransactionId tid;
    HeapFile heapFile;

    /**
     * Constructor for this iterator
     * @param f - the BTreeFile containing the tuples
     * @param tid - the transaction id
     */
    public HeapFileIterator(HeapFile f, TransactionId tid) {
        this.heapFile = f;
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
        curp = (HeapPage) Database.getBufferPool().getPage(tid,
                            new HeapPageId(this.heapFile.getId(), 0),
                            Permissions.READ_ONLY);
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
        //1.the current page has not been finished, nothing need to be done.
        //2.the current page has been finished, read next page
        if (it != null && !it.hasNext())
            it = null;
        //use while to avoid some Page don't have any tuple
        //read next page until done
        while (it == null && curp != null) {
            int nextPageNo = curp.getId().getPageNumber() + 1;
            //System.out.printf("%d,%d\n",nextPageNo,this.heapFile.numPages());
            if(nextPageNo >= this.heapFile.numPages()) {
                curp = null;//finished all pages
            }
            else {
                curp = (HeapPage) Database.getBufferPool().getPage(tid,
                        new HeapPageId(this.heapFile.getId(), nextPageNo),
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
