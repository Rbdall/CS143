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
	
	private File f;
	private TupleDesc td;
	
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
    	this.f = f;
    	this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
    	HeapPage result = null;
    	try {
			RandomAccessFile raf = new RandomAccessFile(f, "r");
			long fileOffput = pid.pageNumber()*BufferPool.PAGE_SIZE;
	    	raf.seek(fileOffput);
	    	byte[] data = new byte[BufferPool.PAGE_SIZE];
	    	raf.read(data);
	    	raf.close();
	    	
	    	result = new HeapPage(new HeapPageId(pid.getTableId(), pid.pageNumber()), data);
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} 
    	
        return result;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        try{
        	RandomAccessFile raf = new RandomAccessFile(f, "rw");
        	long fileOffput = page.getId().pageNumber()*BufferPool.PAGE_SIZE;
        	raf.seek(fileOffput);
        	byte[] data = page.getPageData();
        	raf.write(data);
        	raf.close();
        }
        catch(Exception e){
        	throw new IOException("Failed to write page");
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (f.length()/BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> changedPages = new ArrayList<Page>();
        for(int i = 0; i < numPages(); i++){
        	HeapPage openPage = (HeapPage)(Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE));
        	if(openPage.getNumEmptySlots() != 0){
        		changedPages.add(openPage);
        		openPage.insertTuple(t);
        		return changedPages;
        	}
        }
        HeapPage newPage = new HeapPage(new HeapPageId(getId(), numPages()), HeapPage.createEmptyPageData());
        newPage.insertTuple(t);
        changedPages.add(newPage);
        writePage(newPage);
        return changedPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
    	ArrayList<Page> changedPages = new ArrayList<Page>();
    	HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
    	changedPages.add(page);
    	page.deleteTuple(t);
        return changedPages;
    	
    }
    
    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid, this);
    }

}

