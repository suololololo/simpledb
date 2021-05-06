package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.storage.PageId;

import java.io.IOException;

import simpledb.storage.Page;
import simpledb.storage.DbFile;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    private int age;
    private ConcurrentHashMap<PageId, Page> pageStore;
    private ConcurrentHashMap<PageId, Integer> pageAge;
    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;
    private int numPages;
    private PageLockManager pageLockManager;

    private class PageLock {
        public TransactionId transactionId;
        public int lockType;

        public PageLock(TransactionId transactionId, int lockType) {
            this.transactionId = transactionId;
            this.lockType = lockType;
        }
    }

    //inefficient
    private class PageLockManager {
        //        ReentrantReadWriteLock reentrantReadWriteLock;
        public Map<PageId, List<PageLock>> lockMap;

        public PageLockManager() {
            lockMap = new ConcurrentHashMap<>();
        }

        public synchronized boolean acquireLock(TransactionId transactionId, PageId pageId, int lockType) {
            //if no lock
            if (lockMap.get(pageId) == null) {
                PageLock pageLock = new PageLock(transactionId, lockType);
                List<PageLock> pageLockList = new Vector<>();
                pageLockList.add(pageLock);
                lockMap.put(pageId, pageLockList);
                return true;
            }
            List<PageLock> pageLockList = lockMap.get(pageId);
            for (PageLock pageLock : pageLockList) {
                if (pageLock.transactionId == transactionId) {
                    if (pageLock.lockType == lockType) {
                        return true;
                    }
                    if (pageLock.lockType == 1) {
                        return true;
                    }
                    if (pageLockList.size() == 1) {
                        pageLock.lockType = 1;
                        return true;
                    } else {
                        return false;
                    }
                }
            }
            if (pageLockList.get(0).lockType == 1) {
                assert pageLockList.size() == 1 : "exclusive lock can't coexist with other locks";
                return false;
            }
            if (lockType == 0) {
                PageLock pageLock = new PageLock(transactionId, 0);
                pageLockList.add(pageLock);
                lockMap.put(pageId, pageLockList);
                return true;
            }
            return false;
        }

        public synchronized boolean releaseLock(PageId pageId, TransactionId transactionId) {
            assert lockMap.get(pageId) != null : "page not locked!";
            List<PageLock> lockList = lockMap.get(pageId);
            for (int i = lockList.size() - 1; i >= 0; i--) {
                PageLock temp = lockList.get(i);
                if (temp.transactionId == transactionId) {
                    lockList.remove(temp);
                    if (lockList.size() == 0) {
                        lockMap.remove(pageId);
                    }
                    return true;
                }
            }
            return false;
        }

        public synchronized boolean holdsLock(PageId pageId, TransactionId transactionId) {
            List<PageLock> lockList = lockMap.getOrDefault(pageId, null);
            if (lockList == null) {
                return false;
            }
            for (PageLock pageLock : lockList) {
                if (pageLock.transactionId == transactionId) {
                    return true;
                }
            }
            return false;
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
        pageStore = new ConcurrentHashMap<PageId, Page>();
        pageLockManager = new PageLockManager();
        age = 0;
        pageAge = new ConcurrentHashMap<>();
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
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */

    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        int lockType = 1;
        if (perm == Permissions.READ_ONLY) {
            lockType = 0;
        }
        boolean lockAcquire = false;
        long start = System.currentTimeMillis();
        long timeout = new Random().nextInt(2000) + 1000;
        while (!lockAcquire) {
            long now = System.currentTimeMillis();
            if (now - start > timeout) {
                throw new TransactionAbortedException();
            }
            lockAcquire = pageLockManager.acquireLock(tid, pid, lockType);
        }

        if (!pageStore.containsKey(pid)) {
            if (pageStore.size() >= numPages) {
                evictPage();
            }
            DbFile dbfile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            pageStore.put(pid, dbfile.readPage(pid));
            pageAge.put(pid, age++);
        }
        Page page = pageStore.get(pid);


        return page;

//        throw new DbException("can not have page");

        // some code goes here
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
//<<<<<<< HEAD
//    public void releasePage(TransactionId tid, PageId pid) {
//=======
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
//>>>>>>> ddfe6be1ee14e303389f37581daf60574b2ce75f
        // some code goes here
        // not necessary for lab1|lab2
        pageLockManager.releaseLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return pageLockManager.holdsLock(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        try {
            if (commit) {
                flushPages(tid);
            } else {
//                abort  revert any changes made by the transaction by restoring the page to its on-disk state.
                restorePage(tid);
            }
            for (PageId pageId : pageStore.keySet()) {
                if (holdsLock(tid, pageId)) {
                    unsafeReleasePage(tid, pageId);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    private synchronized void restorePage(TransactionId transactionId) {
        for (PageId pageId : pageStore.keySet()) {
            Page page = pageStore.get(pageId);
            if (page.isDirty() == transactionId) {
                int tableId = pageId.getTableId();
                Page newPage = Database.getCatalog().getDatabaseFile(tableId).readPage(pageId);
                pageStore.put(pageId, newPage);
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        updateBufferPool(f.insertTuple(tid, t), tid);

    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile f = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        updateBufferPool(f.deleteTuple(tid, t), tid);
    }

    private void updateBufferPool(List<Page> pageList, TransactionId id) throws DbException {
        for (Page page : pageList) {
            page.markDirty(true, id);
            if (pageStore.size() > numPages) {
                evictPage();
            }
            pageStore.put(page.getId(), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (Page page : pageStore.values()) {
            flushPage(page.getId());
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pageStore.remove(pid);
        pageAge.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = pageStore.get(pid);
        TransactionId transactionId = null;
        if ((transactionId = page.isDirty()) != null) {
            Database.getLogFile().logWrite(transactionId, page.getBeforeImage(), page);
            Database.getLogFile().force();
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            page.markDirty(false, transactionId);
        }

    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (PageId pageId : pageStore.keySet()) {
            if (pageStore.get(pageId).isDirty() == tid) {
                flushPage(pageId);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        //evict the oldest page
        PageId pageId = null;
        int oldestPage = -1;
        for (PageId pid : pageStore.keySet()) {
            Page page = pageStore.get(pid);
            if (page.isDirty() != null) {
                continue;
            }
            if (pageId == null) {
                pageId = pid;
                oldestPage = pageAge.get(pageId);
                continue;
            }
            if (pageAge.get(pid) < oldestPage) {
                pageId = pid;
                oldestPage = pageAge.get(pid);
            }
        }
        if (pageId == null)
            throw new DbException("failed to evict page: all pages are either dirty");
        discardPage(pageId);
    }

}
