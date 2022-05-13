package simpledb;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

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

    /*
     * Added by lyy
     * Help class describing the type and tid of lock.
     * lockType == READ_WRITE means the lock is a exclusive lock
     * lockType == READ_ONLY means the lock is a shared lock
     * */
    public class Lock{
        private Permissions lockType;
        private TransactionId tid;

        public Lock(Permissions lockType,TransactionId tid){
            this.lockType=lockType;
            this.tid=tid;
        }
        public Lock(Permissions lockType){
            this.lockType=lockType;
            this.tid=null;
        }

        @Override
        public boolean equals(Object obj) {
            if(this==obj) return true;
            if(obj==null||getClass()!=obj.getClass()) return false;
            Lock obj_lock=(Lock) obj;
            return tid.equals(obj_lock.tid)&&lockType.equals(obj_lock.lockType);
        }
    }

    /*
     * Added by lyy
     * Help class to maintain and process series of locks on a specific transaction
     * */
    public class LockManager{
        private ConcurrentHashMap<PageId,List<Lock>> pageid2locklist;
        private ConcurrentHashMap<TransactionId, Set<TransactionId>> dependencylist;
        public LockManager(){
            pageid2locklist=new ConcurrentHashMap<>();
            dependencylist=new ConcurrentHashMap<>();
        }
        public synchronized void addLock(TransactionId tid,PageId pid,Permissions perm){
            /*
             * 对pid页增加一个锁
             */
            Lock lock_to_add=new Lock(perm,tid);
            List<Lock> locklist=pageid2locklist.get(pid);
            if(locklist==null){//如果这个页面上还没有Lock
                locklist=new ArrayList<>();
            }
            locklist.add(lock_to_add);
            pageid2locklist.put(pid,locklist);
            //在依赖集合中删除tid
            removeDependency(tid);
        }
        private synchronized void addDependency(TransactionId from_tid,TransactionId to_tid){
            /*
             * 添加from_tid的所依赖的to_tid
             */
            if(from_tid==to_tid) return;
            Set<TransactionId> lis=dependencylist.get(from_tid);
            if(lis==null||lis.size()==0){
                lis=new HashSet<>();
            }
            lis.add(to_tid);
            dependencylist.put(from_tid,lis);
        }
        private synchronized void removeDependency(TransactionId tid){
            /*
             *返回在依赖集合中删除tid
             */
            dependencylist.remove(tid);
        }
        public synchronized boolean isexistCycle(TransactionId tid){
            // using the logic of topologysort
            Set<TransactionId> diverseid=new HashSet<>();
            Queue<TransactionId> que=new ConcurrentLinkedQueue<>();
            que.add(tid);

            while(que.size()>0){
                TransactionId remove_tid=que.remove();
                if(diverseid.contains(remove_tid)) continue;
                diverseid.add(remove_tid);
                Set<TransactionId> now_set=dependencylist.get(remove_tid);
                if(now_set==null) continue;
                for(TransactionId now_tid:now_set){
                    que.add(now_tid);
                }
            }

            ConcurrentHashMap<TransactionId,Integer> now_rudu=new ConcurrentHashMap<>();
            for(TransactionId now_tid:diverseid){
                now_rudu.put(now_tid,0);
            }
            for(TransactionId now_tid:diverseid){
                Set<TransactionId> now_set=dependencylist.get(now_tid);
                if(now_set==null) continue;
                for(TransactionId now2_tid:now_set){
                    Integer temp = now_rudu.get(now2_tid);
                    temp++;
                    now_rudu.put(now2_tid,temp);
                }
            }

            while(true){
                int cnt=0;
                for(TransactionId now_tid:diverseid){
                    if(now_rudu.get(now_tid)==null) continue;
                    if(now_rudu.get(now_tid)==0){
                        Set<TransactionId> now_set=dependencylist.get(now_tid);
                        if(now_set==null) continue;
                        for(TransactionId now2_tid:now_set){
                            Integer temp = now_rudu.get(now2_tid);
                            if(temp==null) continue;
                            temp--;
                            now_rudu.put(now2_tid,temp);
                        }
                        now_rudu.remove(now_tid);
                        cnt++;
                    }
                }
                if(cnt==0) break;
            }

            if(now_rudu.size()==0) return false;
            return true;
        }

        public synchronized boolean acquireShareLock(TransactionId tid,PageId pid)
                throws DbException{
            /*
             * 返回是否需要被shared（只读）锁锁住，true为不需要被锁住，可继续执行接下来的操作。
             * 遍历该page涉及的全部锁，若其中有一个写锁且不是自己的则拒绝访问，反之可以访问，并做相应更新。
             */
            List<Lock> locklist=pageid2locklist.get(pid);
            if(locklist!=null && locklist.size()!=0)
                //若已经有锁
                for (Lock it:locklist) {
                    //遍历该page涉及的全部锁
                    if (it.lockType == Permissions.READ_WRITE) {
                        //其中有一个写锁
                        if(it.tid.equals(tid)) return true;//是自己的，可以访问
                        else {//不是自己的，添加依赖tid，拒绝访问
                            addDependency(tid,it.tid);
                            return false;
                        }
                    }
                    else if (it.tid.equals(tid)) return true;//若有自己的读锁，无需再加锁，直接返回可以访问
                    //读锁对这个读访问无影响
                }
            //若无锁或其他锁对当前操作无影响，增加这个锁，返回“可以访问”
            addLock(tid,pid,Permissions.READ_ONLY);
            return true;
        }

        public synchronized boolean acquireExclusiveLock(TransactionId tid,PageId pid)
                throws DbException{
            /*
             * 返回是否需要被exclusive（读写）锁锁住，true为不需要被锁住，可继续执行接下来的操作。
             * 若存在其他tid的读锁，拒绝访问；若存在自己的写锁或都为读锁，可以访问。
             */
            List<Lock> locklist=pageid2locklist.get(pid);
            if(locklist!=null&&locklist.size()!=0) {
                //若已经有锁
                for (Lock it:locklist) {
                    //遍历该page涉及的全部锁
                    if (it.lockType == Permissions.READ_WRITE) {
                        //其中有一个写锁
                        if(it.tid.equals(tid)) return true;//是自己的，可以访问
                        else {//不是自己的，添加依赖tid，拒绝访问
                            addDependency(tid,it.tid);
                            return false;
                        }
                    }
                    //读锁对这个写访问无影响
                }
            }
            //若无锁或其他锁对当前操作无影响，增加这个锁，返回“可以访问”
            addLock(tid,pid, Permissions.READ_WRITE);
            return true;
        }

        public synchronized boolean acquireLock(TransactionId tid,PageId pid,Permissions perm)
                throws DbException{
            /*
             * 返回是否需要被锁锁住，true为需要被锁住，根据perm类型调用acquiresharelock或acquireexclusivelock
             */
            if(perm==Permissions.READ_ONLY) return acquireShareLock(tid,pid);
            return acquireExclusiveLock(tid,pid);
        }

        public synchronized void releasePage(TransactionId tid, PageId pid)
        {
            /*
             * 将pid上的锁属于tid释放
             */
            List<Lock> locklist=pageid2locklist.get(pid);
            if(locklist!=null&&locklist.size()!=0) {
                locklist.removeIf(it -> it.tid.equals(tid));
            }
            pageid2locklist.put(pid,locklist);
        }
        public synchronized Lock getLock(TransactionId tid, PageId pid) {
            /*
             * 返回pid页面上tid对应的锁，null为没有这个锁
             **/
            List<Lock> list = pageid2locklist.get(pid);
            if (list==null||list.size()==0) {
                return null;
            }
            for (Lock lk:list) {
                if (lk.tid.equals(tid)) return lk;
            }
            return null;
        }

    }


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        pages = new ConcurrentHashMap<>();
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
        boolean is_acquired=lockmanager.acquireLock(tid,pid,perm);
        /**
         * 下面的是通过依赖图判环来检测死锁的部分，默认情况下这段代码应该是非注释状态。
         * 在使用下面的代码片段的时候，需要把这段代码注释。
         */
 /*       while(!is_acquired) {
            try{
                Thread.sleep(100);
            }
            catch(InterruptedException e){
                e.printStackTrace();
            }
            if (lockprocess.isexistCycle(tid)) {
                throw new TransactionAbortedException();
            }
            is_acquired=lockprocess.acquirelock(tid,pid,perm);
        }

*/
        /**
          * 下面的是通过超时策略来检测死锁的部分，把上面的找环检测死锁的部分注释，下面的
         * 取消注释就可以编译成功了。
         */
/*
        Long begin=System.currentTimeMillis();
        System.out.println(System.currentTimeMillis()+"begin"+currentThread().getName());
        while(!is_acquired) {
            Long end=System.currentTimeMillis();
            System.out.println(System.currentTimeMillis()+"test"+currentThread().getName());
            if(end-begin>3000){
                throw new TransactionAbortedException();
            }
            try {
                Thread.sleep(200);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            is_acquired=lockprocess.acquirelock(tid,pid,perm);
        }
*/
        /**
         * 下面的代码是超时策略，但没有加入检测死锁机制，
         * 因为AbortEvictionTest生成的数据包含死锁，但不能自动捕获抛出的异常，
         * 而导致程序会异常终止，故检查AbortEvictionTest应使用这段代码。
         * */
//*
        while(!is_acquired) {
            try {
                Thread.sleep(200);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            is_acquired=lockmanager.acquireLock(tid,pid,perm);
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
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
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
    public  void deleteTuple(TransactionId tid, Tuple t)
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
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        Iterator<Integer> it = pages.keySet().iterator();
        if(!it.hasNext()) throw new DbException("can not find any page in cache");
        Page tPage=pages.get(it.next());
        if(tPage.isDirty() != null){
            try {
                flushPage(tPage.getId());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        pages.remove(tPage.getId().hashCode());
        //locks.remove(tPage.getId().hashCode());
    }

}
