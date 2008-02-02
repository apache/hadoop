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
package org.apache.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.dfs.MiniDFSCluster;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

/**
 * Test compactions
 */
public class TestCompaction extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(TestCompaction.class.getName());
  private HRegion r = null;
  private static final String COLUMN_FAMILY = COLFAMILY_NAME1;
  private final Text STARTROW;
  private static final Text COLUMN_FAMILY_TEXT = new Text(COLUMN_FAMILY);
  private static final Text COLUMN_FAMILY_TEXT_MINUS_COLON =
    new Text(COLUMN_FAMILY.substring(0, COLUMN_FAMILY.length() - 1));
  private static final int COMPACTION_THRESHOLD = MAXVERSIONS;

  private MiniDFSCluster cluster;
  
  /** constructor */
  public TestCompaction() {
    super();
    STARTROW = new Text(START_KEY);
    
    // Set cache flush size to 1MB
    conf.setInt("hbase.hregion.memcache.flush.size", 1024*1024);
    conf.setInt("hbase.hregion.memcache.block.multiplier", 2);
    this.cluster = null;
  }
  
  /** {@inheritDoc} */
  @Override
  public void setUp() throws Exception {
    this.cluster = new MiniDFSCluster(conf, 2, true, (String[])null);
    super.setUp();
    HTableDescriptor htd = createTableDescriptor(getName());
    this.r = createNewHRegion(htd, null, null);
  }
  
  /** {@inheritDoc} */
  @Override
  public void tearDown() throws Exception {
    HLog hlog = r.getLog();
    this.r.close();
    hlog.closeAndDelete();
    if (this.cluster != null) {
      StaticTestEnvironment.shutdownDfs(cluster);
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
    assertFalse(r.compactIfNeeded());
    for (int i = 0; i < COMPACTION_THRESHOLD; i++) {
      createStoreFile(r);
    }
    // Add more content.  Now there are about 5 versions of each column.
    // Default is that there only 3 (MAXVERSIONS) versions allowed per column.
    // Assert > 3 and then after compaction, assert that only 3 versions
    // available.
    addContent(new HRegionIncommon(r), COLUMN_FAMILY);
    byte [][] bytes = this.r.get(STARTROW, COLUMN_FAMILY_TEXT, 100 /*Too many*/);
    // Assert that I can get > 5 versions (Should be at least 5 in there).
    assertTrue(bytes.length >= 5);
    // Try to run compaction concurrent with a thread flush just to see that
    // we can.
    final HRegion region = this.r;
    Thread t1 = new Thread() {
      @Override
      public void run() {
        try {
          region.flushcache();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    };
    Thread t2 = new Thread() {
      @Override
      public void run() {
        try {
          assertTrue(region.compactIfNeeded());
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    };
    t1.setDaemon(true);
    t1.start();
    t2.setDaemon(true);
    t2.start();
    t1.join();
    t2.join();
    // Now assert that there are 4 versions of a record only: thats the
    // 3 versions that should be in the compacted store and then the one more
    // we added when we flushed. But could be 3 only if the flush happened
    // before the compaction started though we tried to have the threads run
    // concurrently (On hudson this happens).
    byte [] secondRowBytes = START_KEY.getBytes(HConstants.UTF8_ENCODING);
    // Increment the least significant character so we get to next row.
    secondRowBytes[START_KEY_BYTES.length - 1]++;
    Text secondRow = new Text(secondRowBytes);
    bytes = this.r.get(secondRow, COLUMN_FAMILY_TEXT, 100/*Too many*/);
    LOG.info("Count of " + secondRow + ": " + bytes.length);
    // Commented out because fails on an hp+ubuntu single-processor w/ 1G and
    // "Intel(R) Pentium(R) 4 CPU 3.20GHz" though passes on all local
    // machines and even on hudson.  On said machine, its reporting in the
    // LOG line above that there are 3 items in row so it should pass the
    // below test.
    assertTrue(bytes.length == 3 || bytes.length == 4);

    // Now add deletes to memcache and then flush it.  That will put us over
    // the compaction threshold of 3 store files.  Compacting these store files
    // should result in a compacted store file that has no references to the
    // deleted row.
    this.r.deleteAll(STARTROW, COLUMN_FAMILY_TEXT, System.currentTimeMillis());
    // Now, before compacting, remove all instances of the first row so can
    // verify that it is removed as we compact.
    // Assert all delted.
    assertNull(this.r.get(STARTROW, COLUMN_FAMILY_TEXT, 100 /*Too many*/));
    this.r.flushcache();
    assertNull(this.r.get(STARTROW, COLUMN_FAMILY_TEXT, 100 /*Too many*/));
    // Add a bit of data and flush it so we for sure have the compaction limit
    // for store files.  Usually by this time we will have but if compaction
    // included the flush that ran 'concurrently', there may be just the
    // compacted store and the flush above when we added deletes.  Add more
    // content to be certain.
    createSmallerStoreFile(this.r);
    assertTrue(this.r.compactIfNeeded());
    // Assert that the first row is still deleted.
    bytes = this.r.get(STARTROW, COLUMN_FAMILY_TEXT, 100 /*Too many*/);
    assertNull(bytes);
    // Assert the store files do not have the first record 'aaa' keys in them.
    for (MapFile.Reader reader:
        this.r.stores.get(COLUMN_FAMILY_TEXT_MINUS_COLON).getReaders()) {
      reader.reset();
      HStoreKey key = new HStoreKey();
      ImmutableBytesWritable val = new ImmutableBytesWritable();
      while(reader.next(key, val)) {
        assertFalse(key.getRow().equals(STARTROW));
      }
    }
  }

  private void createStoreFile(final HRegion region) throws IOException {
    HRegionIncommon loader = new HRegionIncommon(region);
    addContent(loader, COLUMN_FAMILY);
    loader.flushcache();
  }

  private void createSmallerStoreFile(final HRegion region) throws IOException {
    HRegionIncommon loader = new HRegionIncommon(region); 
    addContent(loader, COLUMN_FAMILY,
        ("bbb" + PUNCTUATION).getBytes(), null);
    loader.flushcache();
  }
}
