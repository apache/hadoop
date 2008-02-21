package org.apache.hadoop.hbase;

import org.apache.hadoop.hbase.client.TestBatchUpdate;

import junit.framework.TestCase;
import junit.framework.TestSuite;

public class TestSomething extends TestCase {

  protected void setUp() throws Exception {
    super.setUp();
  }

  protected void tearDown() throws Exception {
    super.tearDown();
  }

  public static junit.framework.Test suite() {
    TestSuite suite = new TestSuite();
    suite.addTestSuite(TestBatchUpdate.class);
    suite.addTestSuite(TestHMemcache.class);
    suite.addTestSuite(TestBatchUpdate.class);
    suite.addTestSuite(TestBrokenTest.class);
    suite.addTestSuite(TestToString.class);
    return suite;
  }
}
