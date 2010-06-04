/**
 * Copyright 2009 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;

public class TestStoreReconstruction {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private Path dir;
  private MiniDFSCluster cluster;
  private static final String TABLE = "testtable";
  private static final int TOTAL_EDITS = 10000;
  private Configuration conf;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception { }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {

    conf = TEST_UTIL.getConfiguration();
    cluster = new MiniDFSCluster(conf, 3, true, (String[])null);
    // Set the hbase.rootdir to be the home directory in mini dfs.
    TEST_UTIL.getConfiguration().set(HConstants.HBASE_DIR,
      this.cluster.getFileSystem().getHomeDirectory().toString());
    this.dir = new Path("/hbase", TABLE);
    conf.setInt("hbase.regionserver.flushlogentries", 1);

    if (cluster.getFileSystem().exists(dir)) {
      cluster.getFileSystem().delete(dir, true);
    }
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {}

  /**
   * Create a Store with the result of a HLog split and test we only
   * see the good edits
   * @throws Exception
   */
  @Test
  public void runReconstructionLog() throws Exception {
    byte[] family = Bytes.toBytes("column");
    HColumnDescriptor hcd = new HColumnDescriptor(family);
    HTableDescriptor htd = new HTableDescriptor(TABLE);
    htd.addFamily(hcd);
    HRegionInfo info = new HRegionInfo(htd, null, null, false);
    Path oldLogDir = new Path(this.dir, HConstants.HREGION_OLDLOGDIR_NAME);
    Path logDir = new Path(this.dir, HConstants.HREGION_LOGDIR_NAME);
    HLog log = new HLog(cluster.getFileSystem(), logDir, oldLogDir, conf, null);
    HRegion region = new HRegion(dir, log,
        cluster.getFileSystem(),conf, info, null);
    List<KeyValue> result = new ArrayList<KeyValue>();

    // Empty set to get all columns
    NavigableSet<byte[]> qualifiers =
      new ConcurrentSkipListSet<byte[]>(Bytes.BYTES_COMPARATOR);

    final byte[] tableName = Bytes.toBytes(TABLE);
    final byte[] rowName = tableName;
    final byte[] regionName = info.getRegionName();

    // Add 10 000 edits to HLog on the good family
    for (int j = 0; j < TOTAL_EDITS; j++) {
      byte[] qualifier = Bytes.toBytes(Integer.toString(j));
      byte[] column = Bytes.toBytes("column:" + Integer.toString(j));
      WALEdit edit = new WALEdit();
      edit.add(new KeyValue(rowName, family, qualifier,
          System.currentTimeMillis(), column));
      log.append(info, tableName, edit,
          System.currentTimeMillis());
    }
    // Add a cache flush, shouldn't have any effect
    long logSeqId = log.startCacheFlush();
    log.completeCacheFlush(regionName, tableName, logSeqId, info.isMetaRegion());

    // Add an edit to another family, should be skipped.
    WALEdit edit = new WALEdit();
    edit.add(new KeyValue(rowName, Bytes.toBytes("another family"), rowName,
          System.currentTimeMillis(), rowName));
    log.append(info, tableName, edit,
          System.currentTimeMillis());
    log.sync();

    // TODO dont close the file here.
    log.close();

    List<Path> splits = HLog.splitLog(new Path(conf.get(HConstants.HBASE_DIR)),
      logDir, oldLogDir, cluster.getFileSystem(), conf);

    // Split should generate only 1 file since there's only 1 region
    assertEquals(1, splits.size());

    // Make sure the file exists
    assertTrue(cluster.getFileSystem().exists(splits.get(0)));

    // This will run the log reconstruction
    Store store = new Store(dir, region, hcd, cluster.getFileSystem(),
        splits.get(0), conf, null);

    Get get = new Get(rowName);
    store.get(get, qualifiers, result);
    // Make sure we only see the good edits
    assertEquals(TOTAL_EDITS, result.size());
  }
}
