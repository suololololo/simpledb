package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.storage.TupleDesc;
import simpledb.storage.BufferPool;
import simpledb.storage.HeapPage;
import simpledb.storage.DbFileIterator;
import simpledb.transaction.TransactionId;
import simpledb.storage.HeapPageId;

import javax.xml.transform.TransformerException;
import java.io.*;
//import java.security.Permissions;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     * the file that stores the on-disk backing store for this heap
     * file.
     */
    private File f;
    private TupleDesc td;
    private PageId pageOffSet;

    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
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
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;

    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
//        // some code goes here
//        try {
//            byte[] bytes = new byte[BufferPool.getPageSize()];
////            RandomAccess
//            pageOffSet = pid;
//            int offset = BufferPool.getPageSize()*pid.getPageNumber();
//            FileInputStream fileInputStream = new FileInputStream(f);
//            fileInputStream.read(bytes,offset, BufferPool.getPageSize());
//            return new HeapPage((HeapPageId) pid,bytes);
//        }
//        catch (Exception e){
////            throw  new DbException("READ PAGE EXCEPTION");
//        }
//        return null;

        int tableId = pid.getTableId();
        int pgNo = pid.getPageNumber();

        RandomAccessFile file = null;
        try {
            file = new RandomAccessFile(this.f, "r");
            if ((pgNo + 1) * BufferPool.getPageSize() > file.length()) {
                file.close();
                throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableId, pgNo));
            }
            byte[] bytes = new byte[BufferPool.getPageSize()];
            file.seek(pgNo * BufferPool.getPageSize());
            // big end
            int read = file.read(bytes, 0, BufferPool.getPageSize());
            if (read != BufferPool.getPageSize()) {
                throw new IllegalArgumentException(String.format("table %d page %d read %d bytes", tableId, pgNo, read));
            }
            HeapPageId id = new HeapPageId(pid.getTableId(), pid.getPageNumber());
            return new HeapPage(id, bytes);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                file.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableId, pgNo));

    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pgNo = page.getId().getPageNumber();
        if (pgNo > numPages()) {
            throw new IllegalArgumentException("page is invalid");
        }
        int pgSize = BufferPool.getPageSize();
        RandomAccessFile randomAccessFile = new RandomAccessFile(f,"rw");
        randomAccessFile.seek(pgNo*pgSize);
        byte[] data = page.getPageData();
        randomAccessFile.write(data);
        randomAccessFile.close();

    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.floor(f.length() * 1.0 / BufferPool.getPageSize());
//        return  (int) f.length()/ BufferPool.getPageSize();

    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        List<Page> pageList = new ArrayList<>();
        for (int i = 0; i < numPages(); i++) {
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(this.getId(), i), Permissions.READ_WRITE);
            if (heapPage.getNumEmptySlots() == 0) {
                continue;
            }
            heapPage.insertTuple(t);
            pageList.add(heapPage);
            return pageList;
        }
        BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(f,true));
        byte[] emptyData = HeapPage.createEmptyPageData();
        bw.write(emptyData);
        bw.close();
        HeapPage heapPage = (HeapPage)Database.getBufferPool().getPage(tid,new HeapPageId(this.getId(),numPages()-1),Permissions.READ_WRITE);
        heapPage.insertTuple(t);
        pageList.add(heapPage);
        return pageList;
        // no new page
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> pageList = new ArrayList<>();
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        heapPage.deleteTuple(t);
        pageList.add(heapPage);
        return pageList;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
//        BufferPool.getPage();
        return new HeapFileIterator(this, tid);
    }

    private static final class HeapFileIterator implements DbFileIterator {
        private HeapFile heapFile;
        private Iterator<Tuple> iterator;
        private int whichPage;
        private TransactionId transactionId;

        public HeapFileIterator(HeapFile heapFile, TransactionId transactionid) {
            this.heapFile = heapFile;
            this.transactionId = transactionid;
        }

        public void open() throws DbException, TransactionAbortedException {
            whichPage = 0;
            iterator = getPageTuples(whichPage);
        }

        private Iterator<Tuple> getPageTuples(int pageNumber) throws TransactionAbortedException, DbException {
            if (pageNumber >= 0 && pageNumber < heapFile.numPages()) {
                HeapPageId headPageId = new HeapPageId(heapFile.getId(), pageNumber);
                HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(transactionId, headPageId, Permissions.READ_ONLY);
                return heapPage.iterator();
            } else {
                throw new DbException(String.format("heapfile %d does not contain page %d!", pageNumber, heapFile.getId()));
            }
        }

        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (iterator == null)
                return false;
            if (!iterator.hasNext()) {
                if (whichPage < (heapFile.numPages() - 1)) {
                    whichPage++;
                    iterator = getPageTuples(whichPage);
//                    return iterator.hasNext();
                    return hasNext();
                } else {
                    return false;
                }
            } else {
                return true;
            }
        }

        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (iterator == null || !iterator.hasNext()) {
                throw new NoSuchElementException();
            }
            return iterator.next();
        }

        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        public void close() {
            iterator = null;
        }
    }
}

