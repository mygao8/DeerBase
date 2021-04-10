package deerBase.systemtest;

import org.junit.Before;

import deerBase.Database;

/**
 * Base class for all DeerBase test classes. 
 *
 */
public class DeerBaseTestBase {
	/**
	 * Reset the database before each test is run.
	 */
	@Before	public void setUp() throws Exception {					
		Database.reset();
	}
	
}
