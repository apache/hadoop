/*
 * Copyright 2011 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogSplitter;
import org.apache.hadoop.hbase.regionserver.wal.WALCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Tests invocation of the {@link org.apache.hadoop.hbase.coprocessor.MasterObserver}
 * interface hooks at all appropriate times during normal HMaster operations.
 */
public class TestWALObserver {
  private static final Log LOG = LogFactory.getLog(TestWALObserver.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static byte[] TEST_TABLE = Bytes.toBytes("observedTable");
  private static byte[][] TEST_FAMILY = { Bytes.toBytes("fam1"),
    Bytes.toBytes("fam2"),
    Bytes.toBytes("fam3"),
  };
  private static byte[][] TEST_QUALIFIER = { Bytes.toBytes("q1"),
    Bytes.toBytes("q2"),
    Bytes.toBytes("q3"),
  };
  private static byte[][] TEST_VALUE = { Bytes.toBytes("v1"),
    Bytes.toBytes("v2"),
    Bytes.toBytes("v3"),
  };
  private static byte[] TEST_ROW = Bytes.toBytes("testRow");

  private Configuration conf;
  private FileSystem fs;
  private Path dir;
  private MiniDFSCluster cluster;
  private Path hbaseRootDir;
  private Path oldLogDir;
  private Path logDir;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY,
        SampleRegionWALObserver.class.getName());
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        SampleRegionWALObserver.class.getName());
    conf.setBoolean("dfs.support.append", true);
    conf.setInt("dfs.client.block.recovery.retries", 2);

    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.setNameNodeNameSystemLeasePeriod(100, 10000);
    Path hbaseRootDir =
      TEST_UTIL.getDFSCluster().getFileSystem().makeQualified(new Path("/hbase"));
    LOG.info("hbase.rootdir=" + hbaseRootDir);
    conf.set(HConstants.HBASE_DIR, hbaseRootDir.toString());
  }

  @AfterClass
  public static void teardownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    this.conf = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
    //this.cluster = TEST_UTIL.getDFSCluster();
    this.fs = TEST_UTIL.getDFSCluster().getFileSystem();
    this.hbaseRootDir = new Path(conf.get(HConstants.HBASE_DIR));
    this.dir = new Path(this.hbaseRootDir, TestWALObserver.class.getName());
    this.oldLogDir = new Path(this.hbaseRootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    this.logDir = new Path(this.hbaseRootDir, HConstants.HREGION_LOGDIR_NAME);

    if (TEST_UTIL.getDFSCluster().getFileSystem().exists(this.hbaseRootDir)) {
      TEST_UTIL.getDFSCluster().getFileSystem().delete(this.hbaseRootDir, true);
    }
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.getDFSCluster().getFileSystem().delete(this.hbaseRootDir, true);
  }

  /**
   * Test WAL write behavior with WALObserver. The coprocessor monitors
   * a WALEdit written to WAL, and ignore, modify, and add KeyValue's for the
   * WALEdit.
   */
  @Test
  public void testWALObserverWriteToWAL() throws Exception {
    HRegionInfo hri = createBasic3FamilyHRegionInfo(Bytes.toString(TEST_TABLE));
    Path basedir = new Path(this.hbaseRootDir, Bytes.toString(TEST_TABLE));
    deleteDir(basedir);
    fs.mkdirs(new Path(basedir, hri.getEncodedName()));

    HLog log = new HLog(this.fs, this.dir, this.oldLogDir, this.conf);
    SampleRegionWALObserver cp = getCoprocessor(log);

    // TEST_FAMILY[0] shall be removed from WALEdit.
    // TEST_FAMILY[1] value shall be changed.
    // TEST_FAMILY[2] shall be added to WALEdit, although it's not in the put.
    cp.setTestValues(TEST_TABLE, TEST_ROW, TEST_FAMILY[0], TEST_QUALIFIER[0],
        TEST_FAMILY[1], TEST_QUALIFIER[1],
        TEST_FAMILY[2], TEST_QUALIFIER[2]);

    assertFalse(cp.isPreWALWriteCalled());
    assertFalse(cp.isPostWALWriteCalled());

    // TEST_FAMILY[2] is not in the put, however it shall be added by the tested
    // coprocessor.
    // Use a Put to create familyMap.
    Put p = creatPutWith2Families(TEST_ROW);

    Map<byte [], List<KeyValue>> familyMap = p.getFamilyMap();
    WALEdit edit = new WALEdit();
    addFamilyMapToWALEdit(familyMap, edit);

    boolean foundFamily0 = false;
    boolean foundFamily2 = false;
    boolean modifiedFamily1 = false;

    List<KeyValue> kvs = edit.getKeyValues();

    for (KeyValue kv : kvs) {
      if (Arrays.equals(kv.getFamily(), TEST_FAMILY[0])) {
        foundFamily0 = true;
      }
      if (Arrays.equals(kv.getFamily(), TEST_FAMILY[2])) {
        foundFamily2 = true;
      }
      if (Arrays.equals(kv.getFamily(), TEST_FAMILY[1])) {
        if (!Arrays.equals(kv.getValue(), TEST_VALUE[1])) {
          modifiedFamily1 = true;
        }
      }
    }
    assertTrue(foundFamily0);
    assertFalse(foundFamily2);
    assertFalse(modifiedFamily1);

    // it's where WAL write cp should occur.
    long now = EnvironmentEdgeManager.currentTimeMillis();
    log.append(hri, hri.getTableDesc().getName(), edit, now);

    // the edit shall have been change now by the coprocessor.
    foundFamily0 = false;
    foundFamily2 = false;
    modifiedFamily1 = false;
    for (KeyValue kv : kvs) {
      if (Arrays.equals(kv.getFamily(), TEST_FAMILY[0])) {
        foundFamily0 = true;
      }
      if (Arrays.equals(kv.getFamily(), TEST_FAMILY[2])) {
        foundFamily2 = true;
      }
      if (Arrays.equals(kv.getFamily(), TEST_FAMILY[1])) {
        if (!Arrays.equals(kv.getValue(), TEST_VALUE[1])) {
          modifiedFamily1 = true;
        }
      }
    }
    assertFalse(foundFamily0);
    assertTrue(foundFamily2);
    assertTrue(modifiedFamily1);

    assertTrue(cp.isPreWALWriteCalled());
    assertTrue(cp.isPostWALWriteCalled());
  }

  /**
   * Test WAL replay behavior with WALObserver.
   */
  @Test
  public void testWALObserverReplay() throws Exception {
    // WAL replay is handled at HRegion::replayRecoveredEdits(), which is
    // ultimately called by HRegion::initialize()
    byte[] tableName = Bytes.toBytes("testWALCoprocessorReplay");

    final HRegionInfo hri = createBasic3FamilyHRegionInfo(Bytes.toString(tableName));
    final Path basedir = new Path(this.hbaseRootDir, Bytes.toString(tableName));
    deleteDir(basedir);
    fs.mkdirs(new Path(basedir, hri.getEncodedName()));

    //HLog wal = new HLog(this.fs, this.dir, this.oldLogDir, this.conf);
    HLog wal = createWAL(this.conf);
    //Put p = creatPutWith2Families(TEST_ROW);
    WALEdit edit = new WALEdit();
    long now = EnvironmentEdgeManager.currentTimeMillis();
    //addFamilyMapToWALEdit(p.getFamilyMap(), edit);
    final int countPerFamily = 1000;
    for (HColumnDescriptor hcd: hri.getTableDesc().getFamilies()) {
      addWALEdits(tableName, hri, TEST_ROW, hcd.getName(), countPerFamily,
          EnvironmentEdgeManager.getDelegate(), wal);
    }
    wal.append(hri, tableName, edit, now);
    // sync to fs.
    wal.sync();

    final Configuration newConf = HBaseConfiguration.create(this.conf);
    User user = HBaseTestingUtility.getDifferentUser(newConf,
        ".replay.wal.secondtime");
    user.runAs(new PrivilegedExceptionAction() {
      public Object run() throws Exception {
        runWALSplit(newConf);
        FileSystem newFS = FileSystem.get(newConf);
        // Make a new wal for new region open.
        HLog wal2 = createWAL(newConf);
        HRegion region2 = new HRegion(basedir, wal2, FileSystem.get(newConf),
          newConf, hri, TEST_UTIL.getHBaseCluster().getRegionServer(0));
        long seqid2 = region2.initialize();

        SampleRegionWALObserver cp2 =
          (SampleRegionWALObserver)region2.getCoprocessorHost().findCoprocessor(
              SampleRegionWALObserver.class.getName());
        // TODO: asserting here is problematic.
        assertNotNull(cp2);
        assertTrue(cp2.isPreWALRestoreCalled());
        assertTrue(cp2.isPostWALRestoreCalled());
        region2.close();
        wal2.closeAndDelete();
        return null;
      }
    });
  }
  /**
   * Test to see CP loaded successfully or not. There is a duplication
   * at TestHLog, but the purpose of that one is to see whether the loaded
   * CP will impact existing HLog tests or not.
   */
  @Test
  public void testWALObserverLoaded() throws Exception {
    HLog log = new HLog(fs, dir, oldLogDir, conf);
    assertNotNull(getCoprocessor(log));
  }

  private SampleRegionWALObserver getCoprocessor(HLog wal) throws Exception {
    WALCoprocessorHost host = wal.getCoprocessorHost();
    Coprocessor c = host.findCoprocessor(SampleRegionWALObserver.class.getName());
    return (SampleRegionWALObserver)c;
  }

  /*
   * Creates an HRI around an HTD that has <code>tableName</code> and three
   * column families named.
   * @param tableName Name of table to use when we create HTableDescriptor.
   */
  private HRegionInfo createBasic3FamilyHRegionInfo(final String tableName) {
    HTableDescriptor htd = new HTableDescriptor(tableName);

    for (int i = 0; i < TEST_FAMILY.length; i++ ) {
      HColumnDescriptor a = new HColumnDescriptor(TEST_FAMILY[i]);
      htd.addFamily(a);
    }
    return new HRegionInfo(htd, null, null, false);
  }

  /*
   * @param p Directory to cleanup
   */
  private void deleteDir(final Path p) throws IOException {
    if (this.fs.exists(p)) {
      if (!this.fs.delete(p, true)) {
        throw new IOException("Failed remove of " + p);
      }
    }
  }

  private Put creatPutWith2Families(byte[] row) throws IOException {
    Put p = new Put(row);
    for (int i = 0; i < TEST_FAMILY.length-1; i++ ) {
      p.add(TEST_FAMILY[i], TEST_QUALIFIER[i],
          TEST_VALUE[i]);
    }
    return p;
  }

  /**
   * Copied from HRegion.
   *
   * @param familyMap map of family->edits
   * @param walEdit the destination entry to append into
   */
  private void addFamilyMapToWALEdit(Map<byte[], List<KeyValue>> familyMap,
      WALEdit walEdit) {
    for (List<KeyValue> edits : familyMap.values()) {
      for (KeyValue kv : edits) {
        walEdit.add(kv);
      }
    }
  }
  private Path runWALSplit(final Configuration c) throws IOException {
    FileSystem fs = FileSystem.get(c);
    HLogSplitter logSplitter = HLogSplitter.createLogSplitter(c,
        this.hbaseRootDir, this.logDir, this.oldLogDir, fs);
    List<Path> splits = logSplitter.splitLog();
    // Split should generate only 1 file since there's only 1 region
    assertEquals(1, splits.size());
    // Make sure the file exists
    assertTrue(fs.exists(splits.get(0)));
    LOG.info("Split file=" + splits.get(0));
    return splits.get(0);
  }
  private HLog createWAL(final Configuration c) throws IOException {
    HLog wal = new HLog(FileSystem.get(c), logDir, oldLogDir, c);
    return wal;
  }
  private void addWALEdits (final byte [] tableName, final HRegionInfo hri,
      final byte [] rowName, final byte [] family,
      final int count, EnvironmentEdge ee, final HLog wal)
  throws IOException {
    String familyStr = Bytes.toString(family);
    for (int j = 0; j < count; j++) {
      byte[] qualifierBytes = Bytes.toBytes(Integer.toString(j));
      byte[] columnBytes = Bytes.toBytes(familyStr + ":" + Integer.toString(j));
      WALEdit edit = new WALEdit();
      edit.add(new KeyValue(rowName, family, qualifierBytes,
        ee.currentTimeMillis(), columnBytes));
      wal.append(hri, tableName, edit, ee.currentTimeMillis());
    }
  }
}

