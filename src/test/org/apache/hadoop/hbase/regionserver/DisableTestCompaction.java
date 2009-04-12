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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.MiniDFSCluster;

/**
 * Test compactions
 */
public class DisableTestCompaction extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(DisableTestCompaction.class.getName());
  private HRegion r = null;
  private static final byte [] COLUMN_FAMILY = COLFAMILY_NAME1;
  private final byte [] STARTROW = Bytes.toBytes(START_KEY);
  private static final byte [] COLUMN_FAMILY_TEXT = COLUMN_FAMILY;
  private static final byte [] COLUMN_FAMILY_TEXT_MINUS_COLON =
    Bytes.toBytes(Bytes.toString(COLUMN_FAMILY).substring(0, COLUMN_FAMILY.length - 1));
  private static final int COMPACTION_THRESHOLD = MAXVERSIONS;

  private MiniDFSCluster cluster;
  
  /** constructor */
  public DisableTestCompaction() {
    super();
    
    // Set cache flush size to 1MB
    conf.setInt("hbase.hregion.memcache.flush.size", 1024*1024);
    conf.setInt("hbase.hregion.memcache.block.multiplier", 10);
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
   * Run compaction and flushing memcache
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
    Cell[] cellValues = 
      Cell.createSingleCellArray(r.get(STARTROW, COLUMN_FAMILY_TEXT, -1, 100 /*Too many*/));
    // Assert that I can get 3 versions since it is the max I should get
    assertEquals(cellValues.length, 3);
    r.flushcache();
    r.compactStores();
    // Always 3 versions if that is what max versions is.
    byte [] secondRowBytes = START_KEY.getBytes(HConstants.UTF8_ENCODING);
    // Increment the least significant character so we get to next row.
    secondRowBytes[START_KEY_BYTES.length - 1]++;
    // FIX
    cellValues = Cell.createSingleCellArray(r.get(secondRowBytes, COLUMN_FAMILY_TEXT, -1, 100/*Too many*/));
    LOG.info("Count of " + Bytes.toString(secondRowBytes) + ": " +
      cellValues.length);
    assertTrue(cellValues.length == 3);

    // Now add deletes to memcache and then flush it.  That will put us over
    // the compaction threshold of 3 store files.  Compacting these store files
    // should result in a compacted store file that has no references to the
    // deleted row.
    r.deleteAll(secondRowBytes, COLUMN_FAMILY_TEXT, System.currentTimeMillis(),
      null);
    // Assert deleted.
    assertNull(r.get(secondRowBytes, COLUMN_FAMILY_TEXT, -1, 100 /*Too many*/));
    r.flushcache();
    assertNull(r.get(secondRowBytes, COLUMN_FAMILY_TEXT, -1, 100 /*Too many*/));
    // Add a bit of data and flush.  Start adding at 'bbb'.
    createSmallerStoreFile(this.r);
    r.flushcache();
    // Assert that the second row is still deleted.
    // FIX
    cellValues = Cell.createSingleCellArray(r.get(secondRowBytes, COLUMN_FAMILY_TEXT, -1, 100 /*Too many*/));
    assertNull(r.get(secondRowBytes, COLUMN_FAMILY_TEXT, -1, 100 /*Too many*/));
    // Force major compaction.
    r.compactStores(true);
    assertEquals(r.getStore(COLUMN_FAMILY_TEXT).getStorefiles().size(), 1);
    assertNull(r.get(secondRowBytes, COLUMN_FAMILY_TEXT, -1, 100 /*Too many*/));
    // Make sure the store files do have some 'aaa' keys in them -- exactly 3.
    // Also, that compacted store files do not have any secondRowBytes because
    // they were deleted.
    int count = 0;
    boolean containsStartRow = false;
    for (StoreFile f: this.r.stores.
        get(Bytes.mapKey(COLUMN_FAMILY_TEXT_MINUS_COLON)).getStorefiles().values()) {
      HFileScanner scanner = f.getReader().getScanner();
      scanner.seekTo();
      do {
        HStoreKey key = HStoreKey.create(scanner.getKey());
        if (Bytes.equals(key.getRow(), STARTROW)) {
          containsStartRow = true;
          count++;
        } else {
          // After major compaction, should be none of these rows in compacted
          // file.
          assertFalse(Bytes.equals(key.getRow(), secondRowBytes));
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
        get(Bytes.mapKey(COLUMN_FAMILY_TEXT_MINUS_COLON)).getStorefiles().values()) {
      HFileScanner scanner = f.getReader().getScanner();
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
    addContent(loader, Bytes.toString(COLUMN_FAMILY),
        ("bbb").getBytes(), null);
    loader.flushcache();
  }
}
