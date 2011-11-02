/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.util.hbck;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;

/**
 * This testing base class creates a minicluster and testing table table
 * and shuts down the cluster afterwards. It also provides methods wipes out
 * meta and to inject errors into meta and the file system.
 * 
 * Tests should generally break stuff, then attempt to rebuild the meta table
 * offline, then restart hbase, and finally perform checks.
 * 
 * NOTE: This is a slow set of tests which takes ~30s each needs to run on a
 * relatively beefy machine. It seems necessary to have each test in a new jvm
 * since minicluster startup and tear downs seem to leak file handles and
 * eventually cause out of file handle exceptions.
 */
public class OfflineMetaRebuildTestCore {
  protected final static Log LOG = LogFactory
      .getLog(OfflineMetaRebuildTestCore.class);
  protected HBaseTestingUtility TEST_UTIL;
  protected Configuration conf;
  private final static byte[] FAM = Bytes.toBytes("fam");

  // for the instance, reset every test run
  protected HTable htbl;
  protected final static byte[][] splits = new byte[][] { Bytes.toBytes("A"),
      Bytes.toBytes("B"), Bytes.toBytes("C") };

  private final static String TABLE_BASE = "tableMetaRebuild";
  private static int tableIdx = 0;
  protected String table = "tableMetaRebuild";

  @Before
  public void setUpBefore() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    TEST_UTIL.getConfiguration().setInt("dfs.datanode.max.xceivers", 9192);
    TEST_UTIL.startMiniCluster(3);
    conf = TEST_UTIL.getConfiguration();
    assertEquals(0, TEST_UTIL.getHBaseAdmin().listTables().length);

    // setup the table
    table = TABLE_BASE + "-" + tableIdx;
    tableIdx++;
    htbl = setupTable(table);
    populateTable(htbl);
    assertEquals(4, scanMeta());
    LOG.info("Table " + table + " has " + tableRowCount(conf, table)
        + " entries.");
    assertEquals(16, tableRowCount(conf, table));
    TEST_UTIL.getHBaseAdmin().disableTable(table);
    assertEquals(1, TEST_UTIL.getHBaseAdmin().listTables().length);
  }

  @After
  public void tearDownAfter() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    HConnectionManager.deleteConnection(conf, true);
  }

  /**
   * Setup a clean table before we start mucking with it.
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   */
  private HTable setupTable(String tablename) throws Exception {
    HTableDescriptor desc = new HTableDescriptor(tablename);
    HColumnDescriptor hcd = new HColumnDescriptor(Bytes.toString(FAM));
    desc.addFamily(hcd); // If a table has no CF's it doesn't get checked
    TEST_UTIL.getHBaseAdmin().createTable(desc, splits);
    return new HTable(TEST_UTIL.getConfiguration(), tablename);
  }

  private void dumpMeta(HTableDescriptor htd) throws IOException {
    List<byte[]> metaRows = TEST_UTIL.getMetaTableRows(htd.getName());
    for (byte[] row : metaRows) {
      LOG.info(Bytes.toString(row));
    }
  }

  private void populateTable(HTable tbl) throws IOException {
    byte[] values = { 'A', 'B', 'C', 'D' };
    for (int i = 0; i < values.length; i++) {
      for (int j = 0; j < values.length; j++) {
        Put put = new Put(new byte[] { values[i], values[j] });
        put.add(Bytes.toBytes("fam"), new byte[] {}, new byte[] { values[i],
            values[j] });
        tbl.put(put);
      }
    }
    tbl.flushCommits();
  }

  /**
   * delete table in preparation for next test
   * 
   * @param tablename
   * @throws IOException
   */
  void deleteTable(HBaseAdmin admin, String tablename) throws IOException {
    try {
      byte[] tbytes = Bytes.toBytes(tablename);
      admin.disableTable(tbytes);
      admin.deleteTable(tbytes);
    } catch (Exception e) {
      // Do nothing.
    }
  }

  protected void deleteRegion(Configuration conf, final HTable tbl,
      byte[] startKey, byte[] endKey) throws IOException {

    LOG.info("Before delete:");
    HTableDescriptor htd = tbl.getTableDescriptor();
    dumpMeta(htd);

    Map<HRegionInfo, HServerAddress> hris = tbl.getRegionsInfo();
    for (Entry<HRegionInfo, HServerAddress> e : hris.entrySet()) {
      HRegionInfo hri = e.getKey();
      HServerAddress hsa = e.getValue();
      if (Bytes.compareTo(hri.getStartKey(), startKey) == 0
          && Bytes.compareTo(hri.getEndKey(), endKey) == 0) {

        LOG.info("RegionName: " + hri.getRegionNameAsString());
        byte[] deleteRow = hri.getRegionName();
        TEST_UTIL.getHBaseAdmin().unassign(deleteRow, true);

        LOG.info("deleting hdfs data: " + hri.toString() + hsa.toString());
        Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
        FileSystem fs = rootDir.getFileSystem(conf);
        Path p = new Path(rootDir + "/" + htd.getNameAsString(),
            hri.getEncodedName());
        fs.delete(p, true);

        HTable meta = new HTable(conf, HConstants.META_TABLE_NAME);
        Delete delete = new Delete(deleteRow);
        meta.delete(delete);
      }
      LOG.info(hri.toString() + hsa.toString());
    }

    TEST_UTIL.getMetaTableRows(htd.getName());
    LOG.info("After delete:");
    dumpMeta(htd);
  }

  protected HRegionInfo createRegion(Configuration conf, final HTable htbl,
      byte[] startKey, byte[] endKey) throws IOException {
    HTable meta = new HTable(conf, HConstants.META_TABLE_NAME);
    HTableDescriptor htd = htbl.getTableDescriptor();
    HRegionInfo hri = new HRegionInfo(htbl.getTableName(), startKey, endKey);

    LOG.info("manually adding regioninfo and hdfs data: " + hri.toString());
    Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
    FileSystem fs = rootDir.getFileSystem(conf);
    Path p = new Path(rootDir + "/" + htd.getNameAsString(),
        hri.getEncodedName());
    fs.mkdirs(p);
    Path riPath = new Path(p, HRegion.REGIONINFO_FILE);
    FSDataOutputStream out = fs.create(riPath);
    hri.write(out);
    out.close();

    // add to meta.
    Put put = new Put(hri.getRegionName());
    put.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
        Writables.getBytes(hri));
    meta.put(put);
    meta.flushCommits();
    return hri;
  }

  protected void wipeOutMeta() throws IOException {
    // Mess it up by blowing up meta.
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    Scan s = new Scan();
    HTable meta = new HTable(conf, HConstants.META_TABLE_NAME);
    ResultScanner scanner = meta.getScanner(s);
    List<Delete> dels = new ArrayList<Delete>();
    for (Result r : scanner) {
      Delete d = new Delete(r.getRow());
      dels.add(d);
      admin.unassign(r.getRow(), true);
    }
    meta.delete(dels);
    meta.flushCommits();
  }

  /**
   * Returns the number of rows in a given table. HBase must be up and the table
   * should be present (will wait for timeout for a while otherwise)
   * 
   * @return # of rows in the specified table
   */
  protected int tableRowCount(Configuration conf, String table)
      throws IOException {
    HTable t = new HTable(conf, table);
    Scan st = new Scan();

    ResultScanner rst = t.getScanner(st);
    int count = 0;
    for (@SuppressWarnings("unused")
    Result rt : rst) {
      count++;
    }
    return count;
  }

  /**
   * Dumps .META. table info
   * 
   * @return # of entries in meta.
   */
  protected int scanMeta() throws IOException {
    int count = 0;
    HTable meta = new HTable(conf, HTableDescriptor.META_TABLEDESC.getName());
    ResultScanner scanner = meta.getScanner(new Scan());
    LOG.info("Table: " + Bytes.toString(meta.getTableName()));
    for (Result res : scanner) {
      LOG.info(Bytes.toString(res.getRow()));
      count++;
    }
    return count;
  }
}
