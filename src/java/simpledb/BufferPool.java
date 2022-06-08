package simpledb;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.lang.Thread.currentThread;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    private int numPages;
   // private LinkedList<Page> pages;
    private ConcurrentHashMap<Integer, Page> pages;
    //private ConcurrentHashMap<Integer, lock> locks;

    private LockManager lockmanager;
//    private class lock {
//        private TransactionId transactionId = null;
//        private Permissions permissions = null;
//        private int pageNo;
//        private lock(TransactionId tid, Permissions perm,int pageNo){
//            this.permissions = perm;
//            this.transactionId = tid;
//            this.pageNo=pageNo;
//        }
//        private boolean isLocked(){
//            //return pages.get(pageNo).isDirty()==null;
//            return transactionId != null;
//        }
//        private void addlock(TransactionId tid,Permissions perm){
//            this.permissions = perm;
//            this.transactionId = tid;
//        }
//    }


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        pages = new ConcurrentHashMap<>();
        lockmanager=new LockManager();
        //pageIdIndex = new ConcurrentHashMap<>();
       // locks = new ConcurrentHashMap<>();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */

    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        //锁机制
        /**
         * 下面的是通过依赖图判环来检测死锁的部分，默认情况下这段代码应该是非注释状态。
         * 在使用下面的代码片段的时候，需要把这段代码注释。
         */
      /*
       while(!lockmanager.acquireLock(tid,pid,perm)) {
            try{
                Thread.sleep(10);
            }
            catch(InterruptedException e){
                e.printStackTrace();
            }
            if (lockmanager.isexistCycle(tid)) {
                throw new TransactionAbortedException();
            }
        }

//*/
        /**
          * 下面的是通过超时策略来检测死锁的部分，把上面的找环检测死锁的部分注释，下面的
         * 取消注释就可以编译成功了。
         */
///*
        Long begin=System.currentTimeMillis();
        //System.out.println(System.currentTimeMillis()+"begin"+currentThread().getName());
        while(!lockmanager.acquireLock(tid,pid,perm)) {
            Long end=System.currentTimeMillis();
            //System.out.println(System.currentTimeMillis()+"test"+currentThread().getName());
            if(end-begin>300){
                throw new TransactionAbortedException();
            }
            /*try {
                Thread.sleep(10);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }*/
        }
//*/
        /**
         * 下面的代码是超时策略，但没有加入检测死锁机制，
         * 因为AbortEvictionTest生成的数据包含死锁，但不能自动捕获抛出的异常，
         * 而导致程序会异常终止，故检查AbortEvictionTest和BTreeDeadlockTest应使用这段代码。
         * */
/*
        if(!lockmanager.acquireLock(tid,pid,perm)) {
            try {
                Thread.sleep(10);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
//*/

        //在锁机制后，读page
        if(!pages.containsKey(pid.hashCode())){
            //if the page is not in BufferPool
            DbFile dbfile= Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page tempPage=dbfile.readPage(pid);
            if(pages.size()>=numPages) {
                //pages is full, need to be evict.
                evictPage();
            }
            //page is not full
            pages.put(pid.hashCode(), tempPage);
        }
        //the page is in BufferPool already
        return pages.get(pid.hashCode());
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockmanager.releasePage(tid,pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockmanager.getLock(tid,p)!=null;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    /**
     * Helper class used in transactionComplete function
     * added by Sakura
     * Revert changes made in specific transaction
     * */
    public synchronized void revertchanges(TransactionId tid){

        for(Integer it:pages.keySet()){
            Page now_page=pages.get(it);
            if(now_page.isDirty()==tid){
                int now_tableid=now_page.getId().getTableId();
                DbFile f=Database.getCatalog().getDatabaseFile(now_tableid);
                Page revert_page=f.readPage(now_page.getId());
                pages.put(it,revert_page);
                //page_hashmap.get(it).setBeforeImage();
            }
        }
    }
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        if(commit) flushPages(tid);//写到磁盘上
        else revertchanges(tid);//事务恢复
//        for(Integer it:pages.keySet()){
//            //遍历当前bufferpool中的全部page，找到所有被当前tid修改过的page
//            Page pg = pages.get(it);
//            if (pg.isDirty()==tid) {
//                if (commit) {//完成事务并提交，将page写到磁盘上
//                    flushPage(pg.getId());
//                    pg.setBeforeImage();
//                } else{//中止事务，将bufferpool中的该page删除，磁盘上的page无需改变
//                    discardPage(pg.getId());
//                }
//            }
//        }
        //将与当前事务有关的全部lock释放
        for(Integer it:pages.keySet()){
            if(holdsLock(tid,pages.get(it).getId())){
                releasePage(tid,pages.get(it).getId());
            }
        }
    }
    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile now_dbfile=Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> temp_arraylist=now_dbfile.insertTuple(tid,t);
        for (Page now_page:temp_arraylist) {
            //System.out.println(now_page.getId()+"insert1");
            now_page.markDirty(true,tid);
            if(pages.containsKey(now_page.getId().hashCode()))
                pages.replace(now_page.getId().hashCode(),now_page);
            else{
                //this.getPage(tid,now_page.getId(),Permissions.READ_WRITE);
                pages.put(now_page.getId().hashCode(),now_page);
                //locks.put(now_page.getId().hashCode(), new lock(tid,Permissions.READ_WRITE,now_page.getId().hashCode()));
            }
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile now_dbfile=Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> temp_arraylist=now_dbfile.deleteTuple(tid,t);
        for(Page now_page:temp_arraylist){
            //System.out.println(now_page.getId()+"delete1");
            now_page.markDirty(true,tid);
            if(pages.containsKey(now_page.getId().hashCode()))
                pages.replace(now_page.getId().hashCode(),now_page);
            else{
                //this.getPage(tid,now_page.getId(),Permissions.READ_WRITE);
                // 不能从磁盘中读取，因为磁盘中的page仍为更新前的，要直接在cache中添加此page并标记为dirty，以便未来在磁盘上再更新。
                pages.put(now_page.getId().hashCode(),now_page);
                //locks.put(now_page.getId().hashCode(), new lock(tid,Permissions.READ_WRITE,now_page.getId().hashCode()));
            }
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for(Page now_page:pages.values()){
            flushPage(now_page.getId());
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        //locks.remove(pid.hashCode());
        pages.remove(pid.hashCode());
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page now_page=pages.get(pid.hashCode());
        if(now_page.isDirty()!=null){
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(now_page);
            now_page.markDirty(false,null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for(Integer it:pages.keySet()){
            Page now_page=pages.get(it);
            if(now_page.isDirty()==tid){
                flushPage(now_page.getId());
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        //只有非dirty的page会被evict，若所有page都为dirty，则会抛出DbException
        // not necessary for lab1
        //if(!it.hasNext()) throw new DbException("can not find any page in cache");
        for (Integer integer : pages.keySet()) {
            Page tPage = pages.get(integer);
            if (tPage.isDirty() == null) {//该page为非dirty
                pages.remove(tPage.getId().hashCode());
                return;
            }
        }
//                try {
//                    flushPage(tPage.getId());
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
        throw new DbException("there are all dirty page");
    }

}
