package org.apache.hadoop.hbase;

import java.io.IOException;

import junit.framework.TestCase;

public class TestBrokenTest extends TestCase {

  protected void setUp() throws Exception {
    super.setUp();
  }

  protected void tearDown() throws Exception {
    super.tearDown();
  }

  public void testname() throws Exception {
    throw new IOException("I'm broken");
  }
}
