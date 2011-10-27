/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;
import junit.framework.AssertionFailedError;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaMigrationRemovingHTD;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.migration.HRegionInfo090x;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test migration that removes HTableDescriptor from HRegionInfo moving the
 * meta version from no version to {@link MetaReader#META_VERSION}.
 */
public class TestMetaMigrationRemovingHTD {
  static final Log LOG = LogFactory.getLog(TestMetaMigrationRemovingHTD.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static String TESTTABLE = "TestTable";
  private final static int ROWCOUNT = 100;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Start up our mini cluster on top of an 0.90 root.dir that has data from
    // a 0.90 hbase run -- it has a table with 100 rows in it  -- and see if
    // we can migrate from 0.90.
    TEST_UTIL.startMiniZKCluster();
    TEST_UTIL.startMiniDFSCluster(1);
    Path testdir = TEST_UTIL.getDataTestDir("TestMetaMigrationRemovingHTD");
    // Untar our test dir.
    File untar = untar(new File(testdir.toString()));
    // Now copy the untar up into hdfs so when we start hbase, we'll run from it.
    Configuration conf = TEST_UTIL.getConfiguration();
    FsShell shell = new FsShell(conf);
    FileSystem fs = FileSystem.get(conf);
    // Minihbase roots itself in user home directory up in minidfs.
    Path homedir = fs.getHomeDirectory();
    doFsCommand(shell,
      new String [] {"-put", untar.toURI().toString(), homedir.toString()});
    // See whats in minihdfs.
    doFsCommand(shell, new String [] {"-lsr", "/"});
    TEST_UTIL.startMiniHBaseCluster(1, 1);
    // Assert we are running against the copied-up filesystem.  The copied-up
    // rootdir should have had a table named 'TestTable' in it.  Assert it
    // present.
    HTable t = new HTable(TEST_UTIL.getConfiguration(), TESTTABLE);
    ResultScanner scanner = t.getScanner(new Scan());
    int count = 0;
    while (scanner.next() != null) {
      count++;
    }
    // Assert that we find all 100 rows that are in the data we loaded.  If
    // so then we must have migrated it from 0.90 to 0.92.
    Assert.assertEquals(ROWCOUNT, count);
  }

  private static File untar(final File testdir) throws IOException {
    // Find the src data under src/test/data
    final String datafile = "hbase-4388-root.dir";
    String srcTarFile =
      System.getProperty("project.build.testSourceDirectory", "src/test") +
      File.separator + "data" + File.separator + datafile + ".tgz";
    File homedir = new File(testdir.toString());
    File tgtUntarDir = new File(homedir, datafile);
    if (tgtUntarDir.exists()) {
      if (!FileUtil.fullyDelete(tgtUntarDir)) {
        throw new IOException("Failed delete of " + tgtUntarDir.toString());
      }
    }
    LOG.info("Untarring " + srcTarFile + " into " + homedir.toString());
    FileUtil.unTar(new File(srcTarFile), homedir);
    Assert.assertTrue(tgtUntarDir.exists());
    return tgtUntarDir;
  }

  private static void doFsCommand(final FsShell shell, final String [] args)
  throws Exception {
    // Run the 'put' command.
    int errcode = shell.run(args);
    if (errcode != 0) throw new IOException("Failed put; errcode=" + errcode);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMetaUpdatedFlagInROOT() throws Exception {
    boolean metaUpdated = MetaMigrationRemovingHTD.
      isMetaHRIUpdated(TEST_UTIL.getMiniHBaseCluster().getMaster());
    assertEquals(true, metaUpdated);
  }

  @Test
  public void testMetaMigration() throws Exception {
    LOG.info("Starting testMetaWithLegacyHRI");
    final byte [] FAMILY = Bytes.toBytes("family");
    HTableDescriptor htd = new HTableDescriptor("testMetaMigration");
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
      htd.addFamily(hcd);
    Configuration conf = TEST_UTIL.getConfiguration();
    createMultiRegionsWithLegacyHRI(conf, htd, FAMILY,
        new byte[][]{
            HConstants.EMPTY_START_ROW,
            Bytes.toBytes("region_a"),
            Bytes.toBytes("region_b")});
    CatalogTracker ct =
      TEST_UTIL.getMiniHBaseCluster().getMaster().getCatalogTracker();
    // Erase the current version of root meta for this test.
    undoVersionInMeta();
    MetaReader.fullScanMetaAndPrint(ct);
    LOG.info("Meta Print completed.testUpdatesOnMetaWithLegacyHRI");

    Set<HTableDescriptor> htds =
      MetaMigrationRemovingHTD.updateMetaWithNewRegionInfo(
        TEST_UTIL.getHBaseCluster().getMaster());
    MetaReader.fullScanMetaAndPrint(ct);
    // Should be one entry only and it should be for the table we just added.
    assertEquals(1, htds.size());
    assertTrue(htds.contains(htd));
    // Assert that the flag in ROOT is updated to reflect the correct status
    boolean metaUpdated =
      MetaMigrationRemovingHTD.isMetaHRIUpdated(
        TEST_UTIL.getMiniHBaseCluster().getMaster());
    assertEquals(true, metaUpdated);
  }

  /**
   * This test assumes a master crash/failure during the meta migration process
   * and attempts to continue the meta migration process when a new master takes over.
   * When a master dies during the meta migration we will have some rows of
   * META.CatalogFamily updated with new HRI, (i.e HRI with out HTD) and some
   * still hanging with legacy HRI. (i.e HRI with HTD). When the backup master/ or
   * fresh start of master attempts the migration it will encouter some rows of META
   * already updated with new HRI and some still legacy. This test will simulate this
   * scenario and validates that the migration process can safely skip the updated
   * rows and migrate any pending rows at startup.
   * @throws Exception
   */
  @Test
  public void testMasterCrashDuringMetaMigration() throws Exception {
    final byte[] FAMILY = Bytes.toBytes("family");
    HTableDescriptor htd = new HTableDescriptor("testMasterCrashDuringMetaMigration");
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
      htd.addFamily(hcd);
    Configuration conf = TEST_UTIL.getConfiguration();
    // Create 10 New regions.
    createMultiRegionsWithNewHRI(conf, htd, FAMILY, 10);
    // Create 10 Legacy regions.
    createMultiRegionsWithLegacyHRI(conf, htd, FAMILY, 10);
    CatalogTracker ct =
      TEST_UTIL.getMiniHBaseCluster().getMaster().getCatalogTracker();
    // Erase the current version of root meta for this test.
    undoVersionInMeta();
    MetaMigrationRemovingHTD.updateRootWithMetaMigrationStatus(ct);
    //MetaReader.fullScanMetaAndPrint(ct);
    LOG.info("Meta Print completed.testUpdatesOnMetaWithLegacyHRI");

    Set<HTableDescriptor> htds =
      MetaMigrationRemovingHTD.updateMetaWithNewRegionInfo(
        TEST_UTIL.getHBaseCluster().getMaster());
    assertEquals(1, htds.size());
    assertTrue(htds.contains(htd));
    // Assert that the flag in ROOT is updated to reflect the correct status
    boolean metaUpdated = MetaMigrationRemovingHTD.
      isMetaHRIUpdated(TEST_UTIL.getMiniHBaseCluster().getMaster());
    assertEquals(true, metaUpdated);
    LOG.info("END testMetaWithLegacyHRI");
  }

  private void undoVersionInMeta() throws IOException {
    Delete d = new Delete(HRegionInfo.ROOT_REGIONINFO.getRegionName());
    // Erase the current version of root meta for this test.
    d.deleteColumn(HConstants.CATALOG_FAMILY, HConstants.META_VERSION_QUALIFIER);
    HTable rootTable =
      new HTable(TEST_UTIL.getConfiguration(), HConstants.ROOT_TABLE_NAME);
    try {
      rootTable.delete(d);
    } finally {
      rootTable.close();
    }
  }

  public static void assertEquals(int expected, int actual) {
    if (expected != actual) {
      throw new AssertionFailedError("expected:<" +
      expected + "> but was:<" +
      actual + ">");
    }
  }

  public static void assertEquals(boolean expected, boolean actual) {
    if (expected != actual) {
      throw new AssertionFailedError("expected:<" +
      expected + "> but was:<" +
      actual + ">");
    }
  }


  /**
   * @param c
   * @param htd
   * @param family
   * @param numRegions
   * @return
   * @throws IOException
   * @deprecated Just for testing migration of meta from 0.90 to 0.92... will be
   * removed thereafter
   */
  public int createMultiRegionsWithLegacyHRI(final Configuration c,
      final HTableDescriptor htd, final byte [] family, int numRegions)
  throws IOException {
    if (numRegions < 3) throw new IOException("Must create at least 3 regions");
    byte [] startKey = Bytes.toBytes("aaaaa");
    byte [] endKey = Bytes.toBytes("zzzzz");
    byte [][] splitKeys = Bytes.split(startKey, endKey, numRegions - 3);
    byte [][] regionStartKeys = new byte[splitKeys.length+1][];
    for (int i=0;i<splitKeys.length;i++) {
      regionStartKeys[i+1] = splitKeys[i];
    }
    regionStartKeys[0] = HConstants.EMPTY_BYTE_ARRAY;
    return createMultiRegionsWithLegacyHRI(c, htd, family, regionStartKeys);
  }

  /**
   * @param c
   * @param htd
   * @param columnFamily
   * @param startKeys
   * @return
   * @throws IOException
   * @deprecated Just for testing migration of meta from 0.90 to 0.92... will be
   * removed thereafter
   */
  public int createMultiRegionsWithLegacyHRI(final Configuration c,
      final HTableDescriptor htd, final byte[] columnFamily, byte [][] startKeys)
  throws IOException {
    Arrays.sort(startKeys, Bytes.BYTES_COMPARATOR);
    HTable meta = new HTable(c, HConstants.META_TABLE_NAME);
    if(!htd.hasFamily(columnFamily)) {
      HColumnDescriptor hcd = new HColumnDescriptor(columnFamily);
      htd.addFamily(hcd);
    }
    List<HRegionInfo090x> newRegions
        = new ArrayList<HRegionInfo090x>(startKeys.length);
    int count = 0;
    for (int i = 0; i < startKeys.length; i++) {
      int j = (i + 1) % startKeys.length;
      HRegionInfo090x hri = new HRegionInfo090x(htd,
        startKeys[i], startKeys[j]);
      Put put = new Put(hri.getRegionName());
      put.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
        Writables.getBytes(hri));
      meta.put(put);
      LOG.info("createMultiRegions: PUT inserted " + hri.toString());

      newRegions.add(hri);
      count++;
    }
    return count;
  }

  int createMultiRegionsWithNewHRI(final Configuration c,
      final HTableDescriptor htd, final byte [] family, int numRegions)
  throws IOException {
    if (numRegions < 3) throw new IOException("Must create at least 3 regions");
    byte [] startKey = Bytes.toBytes("aaaaa");
    byte [] endKey = Bytes.toBytes("zzzzz");
    byte [][] splitKeys = Bytes.split(startKey, endKey, numRegions - 3);
    byte [][] regionStartKeys = new byte[splitKeys.length+1][];
    for (int i=0;i<splitKeys.length;i++) {
      regionStartKeys[i+1] = splitKeys[i];
    }
    regionStartKeys[0] = HConstants.EMPTY_BYTE_ARRAY;
    return createMultiRegionsWithNewHRI(c, htd, family, regionStartKeys);
  }

  int createMultiRegionsWithNewHRI(final Configuration c, final HTableDescriptor htd,
      final byte[] columnFamily, byte [][] startKeys)
  throws IOException {
    Arrays.sort(startKeys, Bytes.BYTES_COMPARATOR);
    HTable meta = new HTable(c, HConstants.META_TABLE_NAME);
    if(!htd.hasFamily(columnFamily)) {
      HColumnDescriptor hcd = new HColumnDescriptor(columnFamily);
      htd.addFamily(hcd);
    }
    List<HRegionInfo> newRegions
        = new ArrayList<HRegionInfo>(startKeys.length);
    int count = 0;
    for (int i = 0; i < startKeys.length; i++) {
      int j = (i + 1) % startKeys.length;
      HRegionInfo hri = new HRegionInfo(htd.getName(),
        startKeys[i], startKeys[j]);
      Put put = new Put(hri.getRegionName());
      put.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
        Writables.getBytes(hri));
      meta.put(put);
      LOG.info("createMultiRegions: PUT inserted " + hri.toString());

      newRegions.add(hri);
      count++;
    }
    return count;
  }
}
