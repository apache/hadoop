package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for conditions that should trigger RegionServer aborts when
 * rolling the current HLog fails.
 */
public class TestLogRollAbort {
  private static final Log LOG = LogFactory.getLog(TestLogRolling.class);
  private static MiniDFSCluster dfsCluster;
  private static HBaseAdmin admin;
  private static MiniHBaseCluster cluster;
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  // verbose logging on classes that are touched in these tests
  {
    ((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LeaseManager.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LogFactory.getLog("org.apache.hadoop.hdfs.server.namenode.FSNamesystem"))
        .getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)HRegionServer.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)HRegion.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)HLog.LOG).getLogger().setLevel(Level.ALL);
  }

  // Need to override this setup so we can edit the config before it gets sent
  // to the HDFS & HBase cluster startup.
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Tweak default timeout values down for faster recovery
    TEST_UTIL.getConfiguration().setInt(
        "hbase.regionserver.logroll.errors.tolerated", 2);
    TEST_UTIL.getConfiguration().setInt("ipc.ping.interval", 10 * 1000);
    TEST_UTIL.getConfiguration().setInt("ipc.socket.timeout", 10 * 1000);
    TEST_UTIL.getConfiguration().setInt("hbase.rpc.timeout", 10 * 1000);

    // Increase the amount of time between client retries
    TEST_UTIL.getConfiguration().setLong("hbase.client.pause", 5 * 1000);

    // make sure log.hflush() calls syncFs() to open a pipeline
    TEST_UTIL.getConfiguration().setBoolean("dfs.support.append", true);
    // lower the namenode & datanode heartbeat so the namenode
    // quickly detects datanode failures
    TEST_UTIL.getConfiguration().setInt("heartbeat.recheck.interval", 5000);
    TEST_UTIL.getConfiguration().setInt("dfs.heartbeat.interval", 1);
    // the namenode might still try to choose the recently-dead datanode
    // for a pipeline, so try to a new pipeline multiple times
    TEST_UTIL.getConfiguration().setInt("dfs.client.block.write.retries", 10);
    // set periodic sync to 2 min so it doesn't run during test
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.optionallogflushinterval",
        120 * 1000);
  }

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(2);

    cluster = TEST_UTIL.getHBaseCluster();
    dfsCluster = TEST_UTIL.getDFSCluster();
    admin = TEST_UTIL.getHBaseAdmin();

    // disable region rebalancing (interferes with log watching)
    cluster.getMaster().balanceSwitch(false);
  }

  @After
  public void tearDown() throws IOException {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Tests that RegionServer aborts if we hit an error closing the WAL when
   * there are unsynced WAL edits.  See HBASE-4282.
   */
  @Test
  public void testRSAbortWithUnflushedEdits() throws Exception {
    LOG.info("Starting testRSAbortWithUnflushedEdits()");

    // When the META table can be opened, the region servers are running
    new HTable(TEST_UTIL.getConfiguration(), HConstants.META_TABLE_NAME);

    // Create the test table and open it
    String tableName = this.getClass().getSimpleName();
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    desc.setDeferredLogFlush(true);

    admin.createTable(desc);
    HTable table = new HTable(TEST_UTIL.getConfiguration(), tableName);

    HRegionServer server = TEST_UTIL.getRSForFirstRegionInTable(Bytes.toBytes(tableName));
    HLog log = server.getWAL();

    assertTrue("Need HDFS-826 for this test", log.canGetCurReplicas());
    // don't run this test without append support (HDFS-200 & HDFS-142)
    assertTrue("Need append support for this test",
        FSUtils.isAppendSupported(TEST_UTIL.getConfiguration()));

    Put p = new Put(Bytes.toBytes("row2001"));
    p.add(HConstants.CATALOG_FAMILY, Bytes.toBytes("col"), Bytes.toBytes(2001));
    table.put(p);

    log.sync();
    assertEquals("Last deferred edit should have been cleared by sync()",
        log.getLastDeferredSeq(), 0);

    p = new Put(Bytes.toBytes("row2002"));
    p.add(HConstants.CATALOG_FAMILY, Bytes.toBytes("col"), Bytes.toBytes(2002));
    table.put(p);

    dfsCluster.restartDataNodes();
    LOG.info("Restarted datanodes");

    assertTrue("Should have an outstanding WAL edit",
        log.getLastDeferredSeq() > 0);
    try {
      log.rollWriter(true);
      fail("Log roll should have triggered FailedLogCloseException");
    } catch (FailedLogCloseException flce) {
      assertTrue("Should have deferred flush log edits outstanding",
          log.getLastDeferredSeq() > 0);
    }
  }
}
