package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;


/*
 * Added by lyy
 * Help class to maintain and process series of locks on a specific transaction
 * */
public class LockManager{
    private ConcurrentHashMap<PageId, List<Lock>> pageid2locklist;
    private ConcurrentHashMap<TransactionId, Set<TransactionId>> dependencylist;
    public LockManager(){
        pageid2locklist=new ConcurrentHashMap<>();
        dependencylist=new ConcurrentHashMap<>();
    }
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
        //1.构建所有可达的tid的集合diverseid
        Set<TransactionId> diverseid=new HashSet<>();//所有可达的tid的集合
        Queue<TransactionId> que=new ConcurrentLinkedQueue<>();//由当前tid遍历到的尚未更新其他的结点队列
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
        //2.统计所有可达的tid的入度now_rudu
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
        //3.循环找到入度为0的tid并删除该tid和其发出的边直至无法找到。
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
        //4.若还有剩余tid，则有环，反之无环
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
         *
         */
        List<Lock> locklist=pageid2locklist.get(pid);
        if(locklist!=null&&locklist.size()!=0) {
            //若已经有锁
            for (Lock it:locklist) {
                if(!it.tid.equals(tid)){//不是自己的，添加依赖tid，拒绝访问
                    addDependency(tid,it.tid);
                    return false;
                }
                else if(it.lockType == Permissions.READ_WRITE) return true;//若已有自己的读锁，无需再加锁，直接返回“可以访问”
            }
        }
        //若无锁或其他锁对当前操作无影响，增加这个锁，返回“可以访问”
        addLock(tid,pid, Permissions.READ_WRITE);
        return true;
    }

    public synchronized boolean acquireLock(TransactionId tid,PageId pid,Permissions perm)
            throws DbException{
        /*
         * 返回是否需要被锁锁住，true为需要不被锁住，可以继续访问，根据perm类型调用acquiresharelock或acquireexclusivelock
         */
        if(perm==Permissions.READ_ONLY) return acquireShareLock(tid,pid);
        return acquireExclusiveLock(tid,pid);
    }

    public synchronized void releasePage(TransactionId tid, PageId pid)
    {
        /*
         * 将pid上属于tid的锁释放
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

