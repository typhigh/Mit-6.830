package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
	
	enum LockType {
		SLOCK,
		XLOCK 
	}
	
	public class LockManager {
		
		private final int TIME_OUT = 500;
		//private final int TIME_OUT_CNT = 1;
		public class Locker {
			private LockType locktype; 
			private PageId pageId;
			private HashSet<TransactionId> tids;
			private TransactionId xtid;
			public Locker(LockType locktype, PageId pageId) {
				this.locktype = locktype;
				this.pageId = pageId;
				this.tids = new HashSet<TransactionId>(); 
				this.xtid = null;
			}
			public Locker(LockType locktype, PageId pageId, HashSet<TransactionId> tids) {
				this.locktype = locktype;
				this.pageId = pageId;
				this.tids = tids;
				this.xtid = null;
			}
			
			public Locker(LockType locktype, PageId pageId, TransactionId tid) {
				this(locktype, pageId);
				this.xtid = tid;
			}
			public Set<TransactionId> getTids() {
				return this.tids;
			}
			
			public TransactionId getxtid() {
				return this.xtid;
			}
			
			public PageId getPageId() {
				return this.pageId;
			}
			
			public void SetLockType(LockType locktype) {
				this.locktype = locktype;
			}
			
			public void remove(TransactionId tid) {
				if(locktype == LockType.SLOCK) { 
					assert(tids.contains(tid));
					tids.remove(tid);
				}
				else {
					assert(xtid.equals(tid));
					xtid = null;
				}
			}
			
			public void setxtid(TransactionId tid) {
				assert(locktype == LockType.XLOCK);
				this.xtid = tid;
				assert(tids.size() == 0);
			}
			
			public void addholder(TransactionId tid) {
				assert(locktype == LockType.SLOCK);
				tids.add(tid);
			}
			public void updateToXlock() {
				assert(locktype == LockType.SLOCK);
				assert(tids.size() == 1);
				locktype = LockType.XLOCK;
				xtid = tids.iterator().next();
				tids.remove(xtid);
			}
			public int getSRef() {
				if(locktype == LockType.SLOCK)
					return tids.size();
				else 
					return xtid == null ? 0 : 1;
			}
		}
		
		ConcurrentHashMap<PageId, Locker> lockPool;
	    ConcurrentHashMap<TransactionId, HashSet<PageId>> tidMapToLocks; 
	  
	    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
	    	//we use lockPool to get locker, then check whether transaction hold this locker
	    	/*
	    	Locker locker = lockPool.getOrDefault(pid, null);
	    	if(locker == null) 
	    		return false;
	    	if(locker.locktype == LockType.SLOCK) {
	    		//
	    		Set<TransactionId> tids = locker.getTids();
	    		if(tids.contains(tid)) return true;
	    		return false;
	    	}
	    	else {
	    		return locker.xtid == tid;
	    	}
	    	*/
	    	 Set<PageId> pids = this.getLockByTid(tid);
	         return pids != null && pids.contains(pid);
	    }
	    
	    public synchronized Set<PageId> getLockByTid(TransactionId tid) { 
	    	return tidMapToLocks.getOrDefault(tid, null);
	    }
	    
	    public synchronized void releasePage(TransactionId tid, PageId pid) {
	    	//System.out.println(tid.toString() + ":" + pid.toString());
	    	if(tidMapToLocks.containsKey(tid)) {
	    		//HashSet<PageId> pids = tidMapToLocks.get(tid);
	    		tidMapToLocks.get(tid).remove(pid);
	    		//System.out.println("size is: " + pids.size());
	    		if(tidMapToLocks.get(tid).size() == 0) {
	    			tidMapToLocks.remove(tid);
	    		}
	    	}
	    	//else System.out.println("why??");
	    	if(lockPool.containsKey(pid)) {
	    		//Locker locker = lockPool.get(pid);
	    		lockPool.get(pid).remove(tid);
	    		if(lockPool.get(pid).getSRef() == 0) {
	    			lockPool.remove(pid);
	    		}
	    		else {
	    			notifyAll();
	    		}
	    	}
	    	/*
	    	System.out.println(tid.toString() + ":" + pid.toString() + "  done  !");
	    	if(lockPool.containsKey(pid)) {
	    		System.out.println(tid.toString() + ":" + pid.toString() + "  undone  !");
	    	}
	    	
	    	*/
	    }
	    
	    public synchronized void blockRandomTime(long ranTime, long start) 
	    		throws TransactionAbortedException {
	    	if(System.currentTimeMillis() - start > ranTime) {
	    		throw new TransactionAbortedException();
	    	}
	    	try {
				wait(ranTime);
				if(System.currentTimeMillis() - start > ranTime) {
					throw new TransactionAbortedException();
				}
	    	} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
	    
	    private synchronized void addTidMapToLocks(TransactionId tid, PageId pid) {
	    	if(tidMapToLocks.containsKey(tid)) {
	    		if(tidMapToLocks.get(tid).contains(pid)) 
	    			return;
	    		tidMapToLocks.get(tid).add(pid);
	    	}
	    	else {
	    		HashSet<PageId> pids = new HashSet<PageId>();
	    		pids.add(pid);
	    		tidMapToLocks.put(tid, pids);
	    	}
	    }
	    
	    public synchronized void acquire(TransactionId tid, PageId pid, LockType locktype) 
	    		throws TransactionAbortedException {
	    	Random random = new Random();
	    	long start = System.currentTimeMillis();
	    	long ranTime = random.nextInt(TIME_OUT) + 1;
	    	//String printf = tid.toString() + "want" + pid.toString() + "of type: " + locktype.toString();
	    	//System.out.println(printf);
	    	while(true) {
	    		if(locktype == LockType.SLOCK) {
	    			//acquire SLock
	    			if(holdsLock(tid, pid)) 
	    				return;
	    			else if(lockPool.containsKey(pid)) {
	    				//Locker locker = lockPool.get(pid);
	    				if(lockPool.get(pid).locktype == LockType.XLOCK) {
	    					blockRandomTime(ranTime, start);
	    				}
	    				else {
	    					lockPool.get(pid).addholder(tid);
	    					addTidMapToLocks(tid, pid);
	    					return;
	    				}
	    			}
	    			else {
	    				HashSet<TransactionId> tids = new HashSet<TransactionId>();
	    				tids.add(tid);
	    				//Locker locker = new Locker(locktype, pid, tids);
	    				addTidMapToLocks(tid, pid);
	    				lockPool.put(pid, new Locker(locktype, pid, tids));
	    				return;
 	    			}
	    		}
	    		
	    		else if(locktype == LockType.XLOCK){
	    			//acquire XLock
	    			if(holdsLock(tid, pid)) {
	    				//Locker locker = lockPool.get(pid);
	    				if(lockPool.get(pid).locktype == LockType.XLOCK) 
	    					return;
	    				else {
	    					if(lockPool.get(pid).getSRef() == 1) {
	    						lockPool.get(pid).updateToXlock();
	    						return;
	    					}
	    					else {
	    						blockRandomTime(ranTime, start);
	    					}
	    				}
	    			}
	    			else if(lockPool.containsKey(pid)) {
	    				blockRandomTime(ranTime, start);
	    			}
	    			else {
	    				//Locker locker = new Locker(locktype, pid, tid);
	    				//locker.setxtid(tid);
	    				addTidMapToLocks(tid, pid);
	    				lockPool.put(pid, new Locker(locktype, pid, tid));
	    				return;
	    			}
	    		}
	    	}
	    	
	    }
	    public synchronized void releaseTransaction(TransactionId tid) {
	    	if(tidMapToLocks.containsKey(tid)) {
	    		int cnt = tidMapToLocks.get(tid).size();
	    		PageId temp_pids[] = new PageId[cnt];
				PageId pids[] = tidMapToLocks.get(tid).toArray(temp_pids);
	    		for(int i = 0; i < cnt; ++i) {
	    			this.releasePage(tid, pids[i]); 
	    		}
	    		
	    		//notifyAll();
	    	}
	    }
	    
	    
	    public LockManager() {
	    	this.lockPool = new ConcurrentHashMap<PageId, Locker>();
	    	this.tidMapToLocks = new ConcurrentHashMap<TransactionId, HashSet<PageId>>();
	    }
	}
	
	LockManager lockManager;
	
	/** Bytes per page, including header. */
	private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    
    private int numPages;
    ConcurrentHashMap<PageId, Page> buffPool;
    
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
    	this.numPages = numPages;
    	buffPool = new ConcurrentHashMap<PageId, Page>(numPages);
    	lockManager = new LockManager();
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
    	//System.out.println(tid.toString() + "want:" + pid.toString());
    	
    	LockType locktype = (perm == Permissions.READ_ONLY ? 
    			LockType.SLOCK : LockType.XLOCK);  
    	lockManager.acquire(tid, pid, locktype);
    	Page page = buffPool.getOrDefault(pid, null);    	
    	if(page != null) {
    		return page;
    	}
    	if(numPages <= buffPool.size()) {
    		evictPage();
    	}
    	DbFile df = Database.getCatalog().getDatabaseFile(pid.getTableId());
    	page = df.readPage(pid);
    	buffPool.put(pid, page);
        return page;
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
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    	lockManager.releasePage(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	
    	transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
    	return lockManager.holdsLock(tid, p);
    	
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
    	Set<PageId> pids = lockManager.getLockByTid(tid);
    	if(pids != null) {
    		for(PageId pid : pids) {
    			Page pg = buffPool.getOrDefault(pid, null);
    			if(pg == null) continue;
    			if(commit) {
    				flushPage(pid);
    				pg.setBeforeImage();
    			}
    			else if(pg.isDirty() != null){
    				discardPage(pid);
    			}
    		}
    	}
    	lockManager.releaseTransaction(tid);  	
    	//System.out.println(lockManager.getLockByTid(tid).size());
    	assert(lockManager.getLockByTid(tid) == null);
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
    	DbFile f = Database.getCatalog().getDatabaseFile(tableId);
    	ArrayList<Page> pgs = f.insertTuple(tid, t);
    	for(Page pg: pgs) {
    		pg.markDirty(true, tid);
    		buffPool.put(pg.getId(), pg);
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
    	int tableId = t.getRecordId().getPageId().getTableId();
    	DbFile f = Database.getCatalog().getDatabaseFile(tableId);
    	ArrayList<Page> pgs = f.deleteTuple(tid, t);
    	for(Page pg: pgs) {
    		pg.markDirty(true, tid);
    		buffPool.put(pg.getId(), pg);
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
    	Set<PageId> keys = buffPool.keySet();
    	for(PageId key: keys) {
    		flushPage(key);
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
    	buffPool.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
    	if(!buffPool.containsKey(pid)) return;
        Page pg = buffPool.get(pid);
        if(pg.isDirty() != null) {
        	int tableid = pid.getTableId();
        	DbFile df = Database.getCatalog().getDatabaseFile(tableid);
        	df.writePage(pg);
        	pg.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	Set<PageId> pids = lockManager.getLockByTid(tid);
    	if(pids == null) return;
    	for(PageId pid : pids) {
    		flushPage(pid);
    	}
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    
   
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
    	 for (Map.Entry<PageId, Page> entry : buffPool.entrySet()) {
             PageId pid = entry.getKey();
             Page   p   = entry.getValue();
             if (p.isDirty() == null) {
                 // dont need to flushpage since all page evicted are not dirty
                 // flushPage(pid);
                 discardPage(pid);
                 return;
             }
         }
    	throw new DbException("BufferPool: evictPage: all pages are marked as dirty");
    }

}
