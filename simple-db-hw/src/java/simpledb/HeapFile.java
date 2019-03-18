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

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
	private File f;
	private TupleDesc td;
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
        // some code goes here
    	int tableId = pid.getTableId();
    	int pgno = pid.getPageNumber();
    	int pgsize = Database.getBufferPool().getPageSize();
    	byte data[] = HeapPage.createEmptyPageData();
    	try {
			FileInputStream in = new FileInputStream(f);
			try {
				in.skip(pgno * pgsize);
				in.read(data);
				return new HeapPage(new HeapPageId(tableId, pgno), data);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    	PageId pid = page.getId();
    	int tableId = pid.getTableId();
    	int pgno = pid.getPageNumber();
    	int pgsize = Database.getBufferPool().getPageSize();
    	byte[] data = page.getPageData();
    	RandomAccessFile out = new  RandomAccessFile(f, "rws");
    	out.skipBytes(pgno * pgsize);
    	out.write(data);   	
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    //@SuppressWarnings("static-access")
	public int numPages() {
        // some code goes here
    	return (int) (f.length() / (Database.getBufferPool().getPageSize()));
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        int numpg = numPages();
        
        ArrayList<Page> ret = new ArrayList<Page>(1);
        for(int i = 0; i < numpg; ++i) {
        	HeapPageId pid = new HeapPageId(getId(), i);
        	HeapPage pg = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        	if(pg.getNumEmptySlots() > 0) {
        		pg.insertTuple(t);
        		ret.add(pg);
        		pg.markDirty(true, tid);
        		//writePage(pg);
        		return ret;
        	}
        }
        HeapPageId pid = new HeapPageId(getId(), numpg);
        HeapPage newpg = new HeapPage(pid, HeapPage.createEmptyPageData());
        if(newpg.getNumEmptySlots() > 0) {
    		newpg.insertTuple(t);
    		ret.add(newpg);
    		writePage(newpg);
    		return ret;
    	}
        
        throw new DbException("error on insertTuple: no Tuple can insert");
        // not necessary for lab1
        
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
    	RecordId rid = t.getRecordId();
    	PageId pid = rid.getPageId();
    	if(pid.getTableId() != getId()) {
    		throw new DbException("error on deleteTuple: tableid not match");
    	}
    	HeapPage pg = (HeapPage) Database.getBufferPool()
    			.getPage(tid, pid, Permissions.READ_WRITE);
    	pg.deleteTuple(t);
    	pg.markDirty(true, tid);
        ArrayList<Page> ret = new ArrayList<Page>(1);
        ret.add(pg);
        return ret;
        // not necessary for lab1
    }

    public class HeapFileIterator implements DbFileIterator {
    	private TransactionId tid;
    	private int curPgNo;
    	private int pgNum;
    	private int tableId;
    	Iterator<Tuple> iter;
    	
    	public HeapFileIterator(TransactionId tid) {
    		this.tid = tid;
    		this.pgNum = numPages();
    		this.curPgNo = -1;
    		this.tableId = getId();
    		iter = null;
    	}
    	
    	@Override
    	public boolean hasNext() 
    			throws TransactionAbortedException, DbException {
    		if(curPgNo == -1) return false;
    		if(!iter.hasNext()) {
    			if(curPgNo >= pgNum - 1) return false;
    			else {
    				++curPgNo;
    				iter = getBeginIter(curPgNo);
    			}
    		}
    		return iter.hasNext();
    	}
    	
    	@Override 
    	public Tuple next() throws TransactionAbortedException, DbException {
    		if(!this.hasNext()) {
    			throw new NoSuchElementException();
    		}
    		return iter.next();
    	}
    	
    	public Iterator<Tuple> getBeginIter(int curPgNo) 
    			throws TransactionAbortedException, DbException {
    		PageId pid = new HeapPageId(tableId, curPgNo);
    		return ((HeapPage) Database
    				.getBufferPool()
    				.getPage(tid, pid, Permissions.READ_ONLY))
    				.iterator();
    	}
    	
    	@Override
    	public void open() throws TransactionAbortedException, DbException {
    		curPgNo = 0;
    		iter = getBeginIter(curPgNo);
    	}
    	
    	@Override
    	public void close() {
    		curPgNo = -1;
    		iter = null;
    	}
    	
    	@Override
    	public void rewind() throws TransactionAbortedException, DbException {
    		close();
    		open();
    	}
    	
    	
    }
    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

}

