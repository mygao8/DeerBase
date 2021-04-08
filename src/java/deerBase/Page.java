package deerBase;


public interface Page {

    /**
     * Return the id of this page.  The id is a unique identifier for a page
     * that can be used to look up the page on disk or determine if the page
     * is resident in the buffer pool.
     *
     * @return the id of this page
     */
    public PageId getId();

    /**
     * Get the id of the transaction that last dirtied this page, or null if the page is clean..
     *
     * @return The id of the transaction that last dirtied this page, or null
     */
    public TransactionId getDirtier();
	
	  /**
	   * Set the dirty state of this page as dirtied by a particular transaction
	   */
	public void markDirty(boolean dirty, TransactionId tid);

	/**
	 * Check the page is dirty or not
	 */
	public boolean isDirty();
    

    public byte[] getPageData();


    public Page getBeforeImage();


    public void setBeforeImage();
}
