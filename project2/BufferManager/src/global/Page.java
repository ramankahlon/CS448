/* File Page.java */

package global;

import global.*;

 /**
  * class Page
  */

public class Page implements GlobalConst{
  
  
  /**
   * default constructor
   */
  
  public Page()  
    {
      data = new byte[MAX_SPACE];
      
    }
  
  /**
   * Constructor of class Page
   */
  public Page(byte [] apage)
    {
      data = apage;
    }
  
  /**
   * return the data byte array
   * @return 	the byte array of the page
   */
  public byte [] getpage()
    {
      return data;
      
    }
  
  /**
   * set the page with the given byte array
   * @param 	array   a byte array of page size
   */
  public void setpage(byte [] array)
    {
      data = array;
    }

	public void setpage(Page array)
	{
		data = array.data;
	}
  
  /**
   * private field: An array of bytes 
   * 
   */
  protected byte [] data;
  
}
