/* ... */

package bufmgr;

import java.awt.*;
import java.io.*;
import java.util.*;
import diskmgr.*;
import global.*;
import chainexception.*;

class FrameDesc
{
	public int index;
	public int pincount;
	public int state;
	public boolean isDirty;
	public PageId pageno;
	public int now;

	public FrameDesc(int index)
	{
		this.index = index;
		pincount = 0;
		state = 0;
		isDirty = false;
		pageno = new PageId(index);
		now = 0;
	}
}

abstract class Replacer implements GlobalConst
{
	protected FrameDesc[] frame;

	protected Replacer(BufMgr bufmgr)
	{
		this.frame = bufmgr.frame;
	}

	public abstract void pinPage(FrameDesc desc);

	public abstract void unpinPage(FrameDesc desc);

	public abstract void newPage(FrameDesc desc);

	public abstract void freePage(FrameDesc desc);

	//Pick best frame to use for pinning a new page.
	public abstract int pickFrame();
}

class Clock extends Replacer
{
	protected static final int FREE = 10;
	protected static final int REFERENCED = 11;
	protected static final int PINNED = 12;
	protected static LinkedList<FrameDesc> LRUCache;

	public Clock(BufMgr bufmgr)
	{
		super(bufmgr);

		for(int i=0; i<frame.length; i++)
			frame[i].state = FREE;

		for(int i=0; i<frame.length; i++)
			frame[i].state = FREE;

        	LRUCache = new LinkedList<>();
	}

	public void pinPage(FrameDesc desc)
	{
		desc.state = PINNED;
		LRUCache.remove(desc);

		if(LRUCache.size() == frame.length)
			LRUCache.removeFirst();

		LRUCache.addLast(desc);
	}

	public void unpinPage(FrameDesc desc)
	{
		desc.state = REFERENCED;
	}

	public void newPage(FrameDesc desc){}

	public void freePage(FrameDesc desc)
	{
		desc.state = FREE;
	}

	//Choose best available frame to pin a new page.
	public int pickFrame()
	{
		int head = 0;
		while(head < frame.length)
		{
			if(frame[head].state != FREE)
				head++;
			else
				return head;
		}

		if(head == frame.length)
		{
			FrameDesc LRUFrame = LRUCache.getFirst();
			for(int i = 0; i < LRUCache.size(); i++)
			{
				if(LRUCache.get(i).state != 12)
		            		return LRUCache.get(i).index;
            		}
        	}
		return -1;
	}
}

public class BufMgr implements GlobalConst
{
	protected Page[] buffer_pool;
	protected FrameDesc[] frame;
	protected HashMap<Integer, Integer> page_map;
	protected Replacer policy;

	int numBuffers;
  /**
   * Create the BufMgr object.
   * Allocate pages (frames) for the buffer pool in main memory and
   * make the buffer manage aware that the replacement policy is
   * specified by replacerArg (i.e. LRU for this project).
   *
   * @param numbufs number of buffers in the buffer pool.
   * @param replacerArg name of the buffer replacement policy.
   */

  public BufMgr(int numbufs, String replacerArg)
  {

    //YOUR CODE HERE
	numBuffers = numbufs;

	buffer_pool = new Page[numbufs];

	frame = new FrameDesc[numbufs];

	int i=0;
	while(i < numbufs)
	{
		buffer_pool[i] = new Page();
		frame[i] = new FrameDesc(i);
		i++;
	}

	page_map = new HashMap<Integer, Integer>();
	policy = new Clock(this);
  }

  /**
   * Pin a page.
   * First check if this page is already in the buffer pool.
   * If it is, increment the pin_count and return a pointer to this
   * page.  If the pin_count was 0 before the call, the page was a
   * replacement candidate, but is no longer a candidate.
   * If the page is not in the pool, choose a frame (from the
   * set of replacement candidates) to hold this page, read the
   * page (using the appropriate method from {diskmgr} package) and pin it.
   * Also, must write out the old page in chosen frame if it is dirty
   * before reading new page.  (You can assume that emptyPage==false for
   * this assignment.)
   *
   * @param pageno page number in the minibase.
   * @param page the pointer poit to the page.
   * @param emptyPage true (empty page); false (non-empty page)
   */

  public void pinPage(PageId pageno, Page page, boolean emptyPage)
	throws ReplacerException,
	HashOperationException,
	PageUnpinnedException,
	InvalidFrameNumberException,
	PageNotReadException,
	BufferPoolExceededException,
	PagePinnedException,
	BufMgrException,
	IOException,
	InvalidPageNumberException,
	FileIOException
  {
    //YOUR CODE HERE
	int head;
	int index = -1;
	if(page_map.get(pageno.pid) != null)
		//get pageno should say that index 2 is already taken
		index = page_map.get(pageno.pid);

	FrameDesc desc;

	//If page is in page_map
	if(index >= 0)
	{
		desc = frame[page_map.get(pageno.pid)];
		page.setpage(buffer_pool[page_map.get(pageno.pid)].getpage());
		policy.pinPage(desc);
		desc.pincount++;
	}

	//If page is not found in page_map
	else
	{
		head = policy.pickFrame();
		if(head != -1)
		{
			if(frame[head].state == 12)
			{
				if(frame[head].isDirty == true)
					if(frame[head].pageno != null)
						flushPage(frame[head].pageno);

				buffer_pool[head].setpage(page);
				page.setpage(buffer_pool[head].getpage());
				page_map.remove(frame[head].pageno.pid);
				frame[head].pincount++;

				frame[head].pageno = new PageId(pageno.pid);
				frame[head].isDirty = false;

				page_map.put(pageno.pid, head);
				policy.pinPage(frame[head]);
			}
			else if(frame[head].state != 12)
			{
				if(frame[head].isDirty == true)
				{
					if(frame[head].pageno != null)
						flushPage(frame[head].pageno);
                    		}
				
				if(frame[head].pageno != null)
					page_map.remove(frame[head].pageno.pid);

				SystemDefs.JavabaseDB.read_page(pageno, buffer_pool[head]);
				page.setpage(buffer_pool[head].getpage());

				frame[head].pincount++;
				frame[head].pageno = new PageId(pageno.pid);
				frame[head].isDirty = false;

				page_map.put(pageno.pid, head);
				policy.pinPage(frame[head]);
			}
			frame[head].now++;
		}
		else
			throw new BufferPoolExceededException(null, "The buffer pool is full. No replacement is possible.");
	}

  }


  /**
   * Unpin a page specified by a pageId.
   * This method should be called with dirty==true if the client has
   * modified the page.  If so, this call should set the dirty bit
   * for this frame.  Further, if pin_count>0, this method should
   * decrement it. If pin_count=0 before this call, throw an exception
   * to report error.  (For testing purposes, we ask you to throw
   * an exception named PageUnpinnedException in case of error.)
   *
   * @param PageId_in_a_DB PageId_in_a_DB page number in the minibase.
   * @param dirty the dirty bit of the frame
   */

  public void unpinPage(PageId PageId_in_a_DB, boolean dirty)
	throws ChainException
  {
      //YOUR CODE HERE
	int index = -1;
	if(page_map.get(PageId_in_a_DB.pid) != null)
		index = page_map.get(PageId_in_a_DB.pid);

	FrameDesc desc;

	if(index == -1)
	    throw new HashEntryNotFoundException(null, "Unpinning Page not in Buffer Pool");

	if(index >= 0)
	{
		desc = frame[page_map.get(PageId_in_a_DB.pid)];
		if(dirty == true)
			desc.isDirty = dirty;

		if(desc.pincount == 1)
		{
			desc.pincount--;
			desc.isDirty = dirty;
			frame[index] = desc;
			page_map.put(PageId_in_a_DB.pid, index);
			policy.unpinPage(desc);
		}
		else if(desc.pincount == 0)
			throw new PageUnpinnedException(null, "BUFMGR: PAGE_NOT_PINNED.");
		else if(desc == null)
			throw new IllegalArgumentException("Page is not there.");
		else
		{
			desc.pincount--;
			desc.isDirty = dirty;
			frame[desc.index] = desc;
		}
	}
  }


  /**
   * Allocate new pages.
   * Call DB object to allocate a run of new pages and
   * find a frame in the buffer pool for the first page
   * and pin it. (This call allows a client of the Buffer Manager
   * to allocate pages on disk.) If buffer is full, i.e., you
   * can't find a frame for the first page, ask DB to deallocate
   * all these pages, and return null.
   *
   * @param firstpage the address of the first page.
   * @param howmany total number of allocated new pages.
   *
   * @return the first page id of the new pages.  null, if error.
   */

  public PageId newPage(Page firstpage, int howmany)
	throws OutOfSpaceException,
	ReplacerException,
	InvalidRunSizeException,
	InvalidPageNumberException,
	HashOperationException,
	FileIOException,
	DiskMgrException,
  	IOException,
	PageUnpinnedException,
	InvalidFrameNumberException,
	PageNotReadException,
	BufferPoolExceededException,
	PagePinnedException,
	BufMgrException
 {

      //YOUR CODE HERE
  	PageId firstPageId = new PageId(0);

	SystemDefs.JavabaseDB.allocate_page(firstPageId, howmany);
	firstPageId.pid = policy.pickFrame();
	pinPage(firstPageId, firstpage, false);
	return firstPageId;
  }


  /**
   * This method should be called to delete a page that is on disk.
   * This routine must call the method in diskmgr package to
   * deallocate the page.
   *
   * @param globalPageId the page number in the data base.
   */

  public void freePage(PageId globalPageId)
	throws ChainException,
	IOException
  {
      //YOUR CODE HERE
	FrameDesc desc = null;

	int index = -1;
	if(page_map.get(globalPageId.pid) != null)
		index = page_map.get(globalPageId.pid);
	if(index >= 0)
		desc = frame[page_map.get(globalPageId.pid)];

	if(desc == null)
    	{
        	SystemDefs.JavabaseDB.deallocate_page(globalPageId);
	        return;
    	}

   	if(desc.pincount <= 1)
    	{
        	if(desc.pincount == 1)
			unpinPage(globalPageId, desc.isDirty);
		desc.isDirty = false;

		SystemDefs.JavabaseDB.deallocate_page(globalPageId);
		policy.freePage(desc);
		page_map.remove(globalPageId.pid);
		desc.pageno = null;
    	}
    	else
        	throw new PagePinnedException(null, "Cannot free a page that is already pinned.");
  }


  /**
   * Used to flush a particular page of the buffer pool to disk.
   * This method calls the write_page method of the diskmgr package.
   *
   * @param pageid the page number in the database.
   */

  public void flushPage(PageId pageid)
	throws InvalidPageNumberException,
	FileIOException,
	IOException
  {

	if(pageid.pid == 0)
		return;

      //YOUR CODE HERE
	FrameDesc desc;
	if(page_map.get(pageid.pid) != null)
	{
		desc = frame[page_map.get(pageid.pid)];

		if(desc == null)
			return;

		Page page_write = new Page();

		page_write = buffer_pool[page_map.get(pageid.pid)];
		
		if(desc.isDirty == true)
		{
			SystemDefs.JavabaseDB.write_page(pageid, page_write);
			desc.isDirty = false;
		}
  	}
  }


  /** Flushes all pages of the buffer pool to disk
   */

  public void flushAllPages()
	throws InvalidPageNumberException,
	FileIOException,
	IOException
  {

      //YOUR CODE HERE
	for(Integer a : page_map.keySet())
	{
		flushPage(new PageId(a));
	}
  }


  /** Gets the total number of buffers.
   *
   * @return total number of buffer frames.
   */

  public int getNumBuffers()
  {

      //YOUR CODE HERE
	return numBuffers;
  }


  /** Gets the total number of unpinned buffer frames.
   *
   * @return total number of unpinned buffer frames.
   */

  public int getNumUnpinnedBuffers() 
  {

	//YOUR CODE HERE
	int numUnpinnedPages = 0;

	int i = 0;

	while(i < frame.length)
	{
		if(frame[i] != null)
		{
			if(frame[i].pincount == 0)
				numUnpinnedPages++;
		}
		i++;
	}
	return numUnpinnedPages;
  }

}
