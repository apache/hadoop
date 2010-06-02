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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.MiniDFSCluster;


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
  private static final int COMPACTION_THRESHOLD = MAXVERSIONS;

  private MiniDFSCluster cluster;

  /** constructor */
  public TestCompaction() {
    super();

    // Set cache flush size to 1MB
    conf.setInt("hbase.hregion.memstore.flush.size", 1024*1024);
    conf.setInt("hbase.hregion.memstore.block.multiplier", 10);
    this.cluster = null;
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
    this.compactionDir = HRegion.getCompactionDir(this.r.getBaseDir());
    this.regionCompactionDir = new Path(this.compactionDir,
                        this.r.getRegionInfo().getEncodedName());
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
    for (int i = 0; i < COMPACTION_THRESHOLD; i++) {
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
  public void testCompaction() throws Exception {
    createStoreFile(r);
    for (int i = 0; i < COMPACTION_THRESHOLD; i++) {
      createStoreFile(r);
    }
    // Add more content.  Now there are about 5 versions of each column.
    // Default is that there only 3 (MAXVERSIONS) versions allowed per column.
    // Assert == 3 when we ask for versions.
    addContent(new HRegionIncommon(r), Bytes.toString(COLUMN_FAMILY));


    // FIX!!
//    Cell[] cellValues =
//      Cell.createSingleCellArray(r.get(STARTROW, COLUMN_FAMILY_TEXT, -1, 100 /*Too many*/));
    Result result = r.get(new Get(STARTROW).addFamily(COLUMN_FAMILY_TEXT).setMaxVersions(100), null);

    // Assert that I can get 3 versions since it is the max I should get
    assertEquals(COMPACTION_THRESHOLD, result.size());
//    assertEquals(cellValues.length, 3);
    r.flushcache();
    r.compactStores();
    // check compaction dir is exists
    assertTrue(this.cluster.getFileSystem().exists(this.compactionDir));
    // check Compaction Dir for this Regions is cleaned up
    assertTrue(!this.cluster.getFileSystem().exists(this.regionCompactionDir));
    // Always 3 versions if that is what max versions is.
    byte [] secondRowBytes = START_KEY.getBytes(HConstants.UTF8_ENCODING);
    // Increment the least significant character so we get to next row.
    secondRowBytes[START_KEY_BYTES.length - 1]++;
    // FIX
    result = r.get(new Get(secondRowBytes).addFamily(COLUMN_FAMILY_TEXT).setMaxVersions(100), null);

    // Assert that I can get 3 versions since it is the max I should get
    assertEquals(3, result.size());
//
//    cellValues = Cell.createSingleCellArray(r.get(secondRowBytes, COLUMN_FAMILY_TEXT, -1, 100/*Too many*/));
//    LOG.info("Count of " + Bytes.toString(secondRowBytes) + ": " +
//      cellValues.length);
//    assertTrue(cellValues.length == 3);

    // Now add deletes to memstore and then flush it.  That will put us over
    // the compaction threshold of 3 store files.  Compacting these store files
    // should result in a compacted store file that has no references to the
    // deleted row.
    Delete delete = new Delete(secondRowBytes, System.currentTimeMillis(), null);
    byte [][] famAndQf = {COLUMN_FAMILY, null};
    delete.deleteFamily(famAndQf[0]);
    r.delete(delete, null, true);

    // Assert deleted.

    result = r.get(new Get(secondRowBytes).addFamily(COLUMN_FAMILY_TEXT).setMaxVersions(100), null );
    assertTrue(result.isEmpty());


    r.flushcache();
    result = r.get(new Get(secondRowBytes).addFamily(COLUMN_FAMILY_TEXT).setMaxVersions(100), null );
    assertTrue(result.isEmpty());

    // Add a bit of data and flush.  Start adding at 'bbb'.
    createSmallerStoreFile(this.r);
    r.flushcache();
    // Assert that the second row is still deleted.
    result = r.get(new Get(secondRowBytes).addFamily(COLUMN_FAMILY_TEXT).setMaxVersions(100), null );
    assertTrue(result.isEmpty());

    // Force major compaction.
    r.compactStores(true);
    assertEquals(r.getStore(COLUMN_FAMILY_TEXT).getStorefiles().size(), 1);

    result = r.get(new Get(secondRowBytes).addFamily(COLUMN_FAMILY_TEXT).setMaxVersions(100), null );
    assertTrue(result.isEmpty());

    // Make sure the store files do have some 'aaa' keys in them -- exactly 3.
    // Also, that compacted store files do not have any secondRowBytes because
    // they were deleted.
    int count = 0;
    boolean containsStartRow = false;
    for (StoreFile f: this.r.stores.get(COLUMN_FAMILY_TEXT).getStorefiles()) {
      HFileScanner scanner = f.getReader().getScanner(false, false);
      scanner.seekTo();
      do {
        byte [] row = scanner.getKeyValue().getRow();
        if (Bytes.equals(row, STARTROW)) {
          containsStartRow = true;
          count++;
        } else {
          // After major compaction, should be none of these rows in compacted
          // file.
          assertFalse(Bytes.equals(row, secondRowBytes));
        }
      } while(scanner.next());
    }
    assertTrue(containsStartRow);
    assertTrue(count == 3);
    // Do a simple TTL test.
    final int ttlInSeconds = 1;
    for (Store store: this.r.stores.values()) {
      store.ttl = ttlInSeconds * 1000;
    }
    Thread.sleep(ttlInSeconds * 1000);
    r.compactStores(true);
    count = count();
    assertTrue(count == 0);
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
}
