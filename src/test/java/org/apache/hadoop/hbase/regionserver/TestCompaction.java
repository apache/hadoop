/**
 * Copyright 2007 The Apache Software Foundation
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

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.fail;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionProgress;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;


/**
 * Test compactions
 */
public class TestCompaction extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(TestCompaction.class.getName());
  private HRegion r = null;
  private Path compactionDir = null;
  private Path regionCompactionDir = null;
  private static final byte [] COLUMN_FAMILY = fam1;
  private final byte [] STARTROW = Bytes.toBytes(START_KEY);
  private static final byte [] COLUMN_FAMILY_TEXT = COLUMN_FAMILY;
  private int compactionThreshold;
  private byte[] firstRowBytes, secondRowBytes, thirdRowBytes;
  final private byte[] col1, col2;

  private MiniDFSCluster cluster;

  /** constructor */
  public TestCompaction() throws Exception {
    super();

    // Set cache flush size to 1MB
    conf.setInt("hbase.hregion.memstore.flush.size", 1024*1024);
    conf.setInt("hbase.hregion.memstore.block.multiplier", 100);
    this.cluster = null;
    compactionThreshold = conf.getInt("hbase.hstore.compactionThreshold", 3);

    firstRowBytes = START_KEY.getBytes(HConstants.UTF8_ENCODING);
    secondRowBytes = START_KEY.getBytes(HConstants.UTF8_ENCODING);
    // Increment the least significant character so we get to next row.
    secondRowBytes[START_KEY_BYTES.length - 1]++;
    thirdRowBytes = START_KEY.getBytes(HConstants.UTF8_ENCODING);
    thirdRowBytes[START_KEY_BYTES.length - 1]++;
    thirdRowBytes[START_KEY_BYTES.length - 1]++;
    col1 = "column1".getBytes(HConstants.UTF8_ENCODING);
    col2 = "column2".getBytes(HConstants.UTF8_ENCODING);
  }

  @Override
  public void setUp() throws Exception {
    this.cluster = new MiniDFSCluster(conf, 2, true, (String[])null);
    // Make the hbase rootdir match the minidfs we just span up
    this.conf.set(HConstants.HBASE_DIR,
      this.cluster.getFileSystem().getHomeDirectory().toString());
    super.setUp();
    HTableDescriptor htd = createTableDescriptor(getName());
    this.r = createNewHRegion(htd, null, null);
  }

  @Override
  public void tearDown() throws Exception {
    HLog hlog = r.getLog();
    this.r.close();
    hlog.closeAndDelete();
    if (this.cluster != null) {
      shutdownDfs(cluster);
    }
    super.tearDown();
  }

  /**
   * Test that on a major compaction, if all cells are expired or deleted, then
   * we'll end up with no product.  Make sure scanner over region returns
   * right answer in this case - and that it just basically works.
   * @throws IOException
   */
  public void testMajorCompactingToNoOutput() throws IOException {
    createStoreFile(r);
    for (int i = 0; i < compactionThreshold; i++) {
      createStoreFile(r);
    }
    // Now delete everything.
    InternalScanner s = r.getScanner(new Scan());
    do {
      List<KeyValue> results = new ArrayList<KeyValue>();
      boolean result = s.next(results);
      r.delete(new Delete(results.get(0).getRow()), null, false);
      if (!result) break;
    } while(true);
    // Flush
    r.flushcache();
    // Major compact.
    r.compactStores(true);
    s = r.getScanner(new Scan());
    int counter = 0;
    do {
      List<KeyValue> results = new ArrayList<KeyValue>();
      boolean result = s.next(results);
      if (!result) break;
      counter++;
    } while(true);
    assertEquals(0, counter);
  }

  /**
   * Run compaction and flushing memstore
   * Assert deletes get cleaned up.
   * @throws Exception
   */
  public void testMajorCompaction() throws Exception {
    createStoreFile(r);
    for (int i = 0; i < compactionThreshold; i++) {
      createStoreFile(r);
    }
    // Add more content.
    addContent(new HRegionIncommon(r), Bytes.toString(COLUMN_FAMILY));

    // Now there are about 5 versions of each column.
    // Default is that there only 3 (MAXVERSIONS) versions allowed per column.
    //
    // Assert == 3 when we ask for versions.
    Result result = r.get(new Get(STARTROW).addFamily(COLUMN_FAMILY_TEXT).setMaxVersions(100), null);
    assertEquals(compactionThreshold, result.size());

    // see if CompactionProgress is in place but null
    for (Store store: this.r.stores.values()) {
      assertNull(store.getCompactionProgress());
    }

    r.flushcache();
    r.compactStores(true);

    // see if CompactionProgress has done its thing on at least one store
    int storeCount = 0;
    for (Store store: this.r.stores.values()) {
      CompactionProgress progress = store.getCompactionProgress();
      if( progress != null ) {
        ++storeCount;
        assert(progress.currentCompactedKVs > 0);
        assert(progress.totalCompactingKVs > 0);
      }
      assert(storeCount > 0);
    }

    // look at the second row
    // Increment the least significant character so we get to next row.
    byte [] secondRowBytes = START_KEY.getBytes(HConstants.UTF8_ENCODING);
    secondRowBytes[START_KEY_BYTES.length - 1]++;

    // Always 3 versions if that is what max versions is.
    result = r.get(new Get(secondRowBytes).addFamily(COLUMN_FAMILY_TEXT).setMaxVersions(100), null);
    assertEquals(compactionThreshold, result.size());

    // Now add deletes to memstore and then flush it.
    // That will put us over
    // the compaction threshold of 3 store files.  Compacting these store files
    // should result in a compacted store file that has no references to the
    // deleted row.
    Delete delete = new Delete(secondRowBytes, System.currentTimeMillis(), null);
    byte [][] famAndQf = {COLUMN_FAMILY, null};
    delete.deleteFamily(famAndQf[0]);
    r.delete(delete, null, true);

    // Assert deleted.
    result = r.get(new Get(secondRowBytes).addFamily(COLUMN_FAMILY_TEXT).setMaxVersions(100), null );
    assertTrue("Second row should have been deleted", result.isEmpty());

    r.flushcache();

    result = r.get(new Get(secondRowBytes).addFamily(COLUMN_FAMILY_TEXT).setMaxVersions(100), null );
    assertTrue("Second row should have been deleted", result.isEmpty());

    // Add a bit of data and flush.  Start adding at 'bbb'.
    createSmallerStoreFile(this.r);
    r.flushcache();
    // Assert that the second row is still deleted.
    result = r.get(new Get(secondRowBytes).addFamily(COLUMN_FAMILY_TEXT).setMaxVersions(100), null );
    assertTrue("Second row should still be deleted", result.isEmpty());

    // Force major compaction.
    r.compactStores(true);
    assertEquals(r.getStore(COLUMN_FAMILY_TEXT).getStorefiles().size(), 1);

    result = r.get(new Get(secondRowBytes).addFamily(COLUMN_FAMILY_TEXT).setMaxVersions(100), null );
    assertTrue("Second row should still be deleted", result.isEmpty());

    // Make sure the store files do have some 'aaa' keys in them -- exactly 3.
    // Also, that compacted store files do not have any secondRowBytes because
    // they were deleted.
    verifyCounts(3,0);

    // Multiple versions allowed for an entry, so the delete isn't enough
    // Lower TTL and expire to ensure that all our entries have been wiped
    final int ttlInSeconds = 1;
    for (Store store: this.r.stores.values()) {
      store.ttl = ttlInSeconds * 1000;
    }
    Thread.sleep(ttlInSeconds * 1000);

    r.compactStores(true);
    int count = count();
    assertTrue("Should not see anything after TTL has expired", count == 0);
  }

  public void testMinorCompactionWithDeleteRow() throws Exception {
    Delete deleteRow = new Delete(secondRowBytes);
    testMinorCompactionWithDelete(deleteRow);
  }
  public void testMinorCompactionWithDeleteColumn1() throws Exception {
    Delete dc = new Delete(secondRowBytes);
    /* delete all timestamps in the column */
    dc.deleteColumns(fam2, col2);
    testMinorCompactionWithDelete(dc);
  }
  public void testMinorCompactionWithDeleteColumn2() throws Exception {
    Delete dc = new Delete(secondRowBytes);
    dc.deleteColumn(fam2, col2);
    /* compactionThreshold is 3. The table has 4 versions: 0, 1, 2, and 3.
     * we only delete the latest version. One might expect to see only
     * versions 1 and 2. HBase differs, and gives us 0, 1 and 2.
     * This is okay as well. Since there was no compaction done before the
     * delete, version 0 seems to stay on.
     */
    //testMinorCompactionWithDelete(dc, 2);
    testMinorCompactionWithDelete(dc, 3);
  }
  public void testMinorCompactionWithDeleteColumnFamily() throws Exception {
    Delete deleteCF = new Delete(secondRowBytes);
    deleteCF.deleteFamily(fam2);
    testMinorCompactionWithDelete(deleteCF);
  }
  public void testMinorCompactionWithDeleteVersion1() throws Exception {
    Delete deleteVersion = new Delete(secondRowBytes);
    deleteVersion.deleteColumns(fam2, col2, 2);
    /* compactionThreshold is 3. The table has 4 versions: 0, 1, 2, and 3.
     * We delete versions 0 ... 2. So, we still have one remaining.
     */
    testMinorCompactionWithDelete(deleteVersion, 1);
  }
  public void testMinorCompactionWithDeleteVersion2() throws Exception {
    Delete deleteVersion = new Delete(secondRowBytes);
    deleteVersion.deleteColumn(fam2, col2, 1);
    /*
     * the table has 4 versions: 0, 1, 2, and 3.
     * 0 does not count.
     * We delete 1.
     * Should have 2 remaining.
     */
    testMinorCompactionWithDelete(deleteVersion, 2);
  }

  /*
   * A helper function to test the minor compaction algorithm. We check that
   * the delete markers are left behind. Takes delete as an argument, which
   * can be any delete (row, column, columnfamliy etc), that essentially
   * deletes row2 and column2. row1 and column1 should be undeleted
   */
  private void testMinorCompactionWithDelete(Delete delete) throws Exception {
    testMinorCompactionWithDelete(delete, 0);
  }
  private void testMinorCompactionWithDelete(Delete delete, int expectedResultsAfterDelete) throws Exception {
    HRegionIncommon loader = new HRegionIncommon(r);
    for (int i = 0; i < compactionThreshold + 1; i++) {
      addContent(loader, Bytes.toString(fam1), Bytes.toString(col1), firstRowBytes, thirdRowBytes, i);
      addContent(loader, Bytes.toString(fam1), Bytes.toString(col2), firstRowBytes, thirdRowBytes, i);
      addContent(loader, Bytes.toString(fam2), Bytes.toString(col1), firstRowBytes, thirdRowBytes, i);
      addContent(loader, Bytes.toString(fam2), Bytes.toString(col2), firstRowBytes, thirdRowBytes, i);
      r.flushcache();
    }

    Result result = r.get(new Get(firstRowBytes).addColumn(fam1, col1).setMaxVersions(100), null);
    assertEquals(compactionThreshold, result.size());
    result = r.get(new Get(secondRowBytes).addColumn(fam2, col2).setMaxVersions(100), null);
    assertEquals(compactionThreshold, result.size());

    // Now add deletes to memstore and then flush it.  That will put us over
    // the compaction threshold of 3 store files.  Compacting these store files
    // should result in a compacted store file that has no references to the
    // deleted row.
    r.delete(delete, null, true);

    // Make sure that we have only deleted family2 from secondRowBytes
    result = r.get(new Get(secondRowBytes).addColumn(fam2, col2).setMaxVersions(100), null);
    assertEquals(expectedResultsAfterDelete, result.size());
    // but we still have firstrow
    result = r.get(new Get(firstRowBytes).addColumn(fam1, col1).setMaxVersions(100), null);
    assertEquals(compactionThreshold, result.size());

    r.flushcache();
    // should not change anything.
    // Let us check again

    // Make sure that we have only deleted family2 from secondRowBytes
    result = r.get(new Get(secondRowBytes).addColumn(fam2, col2).setMaxVersions(100), null);
    assertEquals(expectedResultsAfterDelete, result.size());
    // but we still have firstrow
    result = r.get(new Get(firstRowBytes).addColumn(fam1, col1).setMaxVersions(100), null);
    assertEquals(compactionThreshold, result.size());

    // do a compaction
    Store store2 = this.r.stores.get(fam2);
    int numFiles1 = store2.getStorefiles().size();
    assertTrue("Was expecting to see 4 store files", numFiles1 > compactionThreshold); // > 3
    store2.compactRecent(compactionThreshold);   // = 3
    int numFiles2 = store2.getStorefiles().size();
    // Check that we did compact
    assertTrue("Number of store files should go down", numFiles1 > numFiles2);
    // Check that it was a minor compaction.
    assertTrue("Was not supposed to be a major compaction", numFiles2 > 1);

    // Make sure that we have only deleted family2 from secondRowBytes
    result = r.get(new Get(secondRowBytes).addColumn(fam2, col2).setMaxVersions(100), null);
    assertEquals(expectedResultsAfterDelete, result.size());
    // but we still have firstrow
    result = r.get(new Get(firstRowBytes).addColumn(fam1, col1).setMaxVersions(100), null);
    assertEquals(compactionThreshold, result.size());
  }

  private void verifyCounts(int countRow1, int countRow2) throws Exception {
    int count1 = 0;
    int count2 = 0;
    for (StoreFile f: this.r.stores.get(COLUMN_FAMILY_TEXT).getStorefiles()) {
      HFileScanner scanner = f.getReader().getScanner(false, false);
      scanner.seekTo();
      do {
        byte [] row = scanner.getKeyValue().getRow();
        if (Bytes.equals(row, STARTROW)) {
          count1++;
        } else if(Bytes.equals(row, secondRowBytes)) {
          count2++;
        }
      } while(scanner.next());
    }
    assertEquals(countRow1,count1);
    assertEquals(countRow2,count2);
  }

  /**
   * Verify that you can stop a long-running compaction
   * (used during RS shutdown)
   * @throws Exception
   */
  public void testInterruptCompaction() throws Exception {
    assertEquals(0, count());

    // lower the polling interval for this test
    int origWI = Store.closeCheckInterval;
    Store.closeCheckInterval = 10*1000; // 10 KB

    try {
      // Create a couple store files w/ 15KB (over 10KB interval)
      int jmax = (int) Math.ceil(15.0/compactionThreshold);
      byte [] pad = new byte[1000]; // 1 KB chunk
      for (int i = 0; i < compactionThreshold; i++) {
        HRegionIncommon loader = new HRegionIncommon(r);
        Put p = new Put(Bytes.add(STARTROW, Bytes.toBytes(i)));
        for (int j = 0; j < jmax; j++) {
          p.add(COLUMN_FAMILY, Bytes.toBytes(j), pad);
        }
        addContent(loader, Bytes.toString(COLUMN_FAMILY));
        loader.put(p);
        loader.flushcache();
      }

      HRegion spyR = spy(r);
      doAnswer(new Answer() {
        public Object answer(InvocationOnMock invocation) throws Throwable {
          r.writestate.writesEnabled = false;
          return invocation.callRealMethod();
        }
      }).when(spyR).doRegionCompactionPrep();

      // force a minor compaction, but not before requesting a stop
      spyR.compactStores();

      // ensure that the compaction stopped, all old files are intact,
      Store s = r.stores.get(COLUMN_FAMILY);
      assertEquals(compactionThreshold, s.getStorefilesCount());
      assertTrue(s.getStorefilesSize() > 15*1000);
      // and no new store files persisted past compactStores()
      FileStatus[] ls = cluster.getFileSystem().listStatus(r.getTmpDir());
      assertEquals(0, ls.length);

    } finally {
      // don't mess up future tests
      r.writestate.writesEnabled = true;
      Store.closeCheckInterval = origWI;

      // Delete all Store information once done using
      for (int i = 0; i < compactionThreshold; i++) {
        Delete delete = new Delete(Bytes.add(STARTROW, Bytes.toBytes(i)));
        byte [][] famAndQf = {COLUMN_FAMILY, null};
        delete.deleteFamily(famAndQf[0]);
        r.delete(delete, null, true);
      }
      r.flushcache();

      // Multiple versions allowed for an entry, so the delete isn't enough
      // Lower TTL and expire to ensure that all our entries have been wiped
      final int ttlInSeconds = 1;
      for (Store store: this.r.stores.values()) {
        store.ttl = ttlInSeconds * 1000;
      }
      Thread.sleep(ttlInSeconds * 1000);

      r.compactStores(true);
      assertEquals(0, count());
    }
  }

  private int count() throws IOException {
    int count = 0;
    for (StoreFile f: this.r.stores.
        get(COLUMN_FAMILY_TEXT).getStorefiles()) {
      HFileScanner scanner = f.getReader().getScanner(false, false);
      if (!scanner.seekTo()) {
        continue;
      }
      do {
        count++;
      } while(scanner.next());
    }
    return count;
  }

  private void createStoreFile(final HRegion region) throws IOException {
    HRegionIncommon loader = new HRegionIncommon(region);
    addContent(loader, Bytes.toString(COLUMN_FAMILY));
    loader.flushcache();
  }

  private void createSmallerStoreFile(final HRegion region) throws IOException {
    HRegionIncommon loader = new HRegionIncommon(region);
    addContent(loader, Bytes.toString(COLUMN_FAMILY), ("" +
    		"bbb").getBytes(), null);
    loader.flushcache();
  }

  public void testCompactionWithCorruptResult() throws Exception {
    int nfiles = 10;
    for (int i = 0; i < nfiles; i++) {
      createStoreFile(r);
    }
    Store store = r.getStore(COLUMN_FAMILY);

    List<StoreFile> storeFiles = store.getStorefiles();
    long maxId = StoreFile.getMaxSequenceIdInList(storeFiles);

    StoreFile.Writer compactedFile = store.compactStore(storeFiles, false, maxId);

    // Now lets corrupt the compacted file.
    FileSystem fs = cluster.getFileSystem();
    Path origPath = compactedFile.getPath();
    Path homedir = store.getHomedir();
    Path dstPath = new Path(homedir, origPath.getName());
    FSDataOutputStream stream = fs.create(origPath, null, true, 512, (short) 3,
        (long) 1024,
        null);
    stream.writeChars("CORRUPT FILE!!!!");
    stream.close();

    try {
      store.completeCompaction(storeFiles, compactedFile);
    } catch (Exception e) {
      // The complete compaction should fail and the corrupt file should remain
      // in the 'tmp' directory;
      assert (fs.exists(origPath));
      assert (!fs.exists(dstPath));
      System.out.println("testCompactionWithCorruptResult Passed");
      return;
    }
    fail("testCompactionWithCorruptResult failed since no exception was" +
        "thrown while completing a corrupt file");
  }
}
