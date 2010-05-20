package org.apache.hadoop.hbase.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test various scanner timeout issues.
 */
public class TestScannerTimeout {

  private final static HBaseTestingUtility
      TEST_UTIL = new HBaseTestingUtility();

  final Log LOG = LogFactory.getLog(getClass());
  private final byte[] someBytes = Bytes.toBytes("f");

   /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration c = TEST_UTIL.getConfiguration();
    c.setInt("hbase.regionserver.lease.period", 1000);
    TEST_UTIL.startMiniCluster(1);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
  }

  /**
   * Test that we do get a ScannerTimeoutException
   * @throws Exception
   */
  @Test
  public void test2481() throws Exception {
    int initialCount = 10;
    HTable t = TEST_UTIL.createTable(Bytes.toBytes("t"), someBytes);
    for (int i = 0; i < initialCount; i++) {
      Put put = new Put(Bytes.toBytes(i));
      put.add(someBytes, someBytes, someBytes);
      t.put(put);
    }
    Scan scan = new Scan();
    ResultScanner r = t.getScanner(scan);
    int count = 0;
    try {
      Result res = r.next();
      while (res != null) {
        count++;
        if (count == 5) {
          Thread.sleep(1500);
        }
        res = r.next();
      }
    } catch (ScannerTimeoutException e) {
      LOG.info("Got the timeout " + e.getMessage(), e);
      return;
    }
    fail("We should be timing out");
  }
}
