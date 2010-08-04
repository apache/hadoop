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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the {@link SplitTransaction} class against an HRegion (as opposed to
 * running cluster).
 */
public class TestSplitTransaction {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final Path testdir =
    HBaseTestingUtility.getTestDir(this.getClass().getName());
  private HRegion parent;
  private HLog wal;
  private FileSystem fs;
  private static final byte [] STARTROW = new byte [] {'a', 'a', 'a'};
  // '{' is next ascii after 'z'.
  private static final byte [] ENDROW = new byte [] {'{', '{', '{'};
  private static final byte [] GOOD_SPLIT_ROW = new byte [] {'d', 'd', 'd'};
  private static final byte [] CF = HConstants.CATALOG_FAMILY;

  @Before public void setup() throws IOException {
    this.fs = FileSystem.get(TEST_UTIL.getConfiguration());
    this.fs.delete(this.testdir, true);
    this.wal = new HLog(fs, new Path(this.testdir, "logs"),
      new Path(this.testdir, "archive"),
      TEST_UTIL.getConfiguration(), null);
    this.parent = createRegion(this.testdir, this.wal);
  }

  @After public void teardown() throws IOException {
    if (this.parent != null && !this.parent.isClosed()) this.parent.close();
    if (this.fs.exists(this.parent.getRegionDir()) &&
        !this.fs.delete(this.parent.getRegionDir(), true)) {
      throw new IOException("Failed delete of " + this.parent.getRegionDir());
    }
    if (this.wal != null) this.wal.closeAndDelete();
    this.fs.delete(this.testdir, true);
  }

  /**
   * Test straight prepare works.  Tries to split on {@link #GOOD_SPLIT_ROW}
   * @throws IOException
   */
  @Test public void testPrepare() throws IOException {
    prepareGOOD_SPLIT_ROW();
  }

  private SplitTransaction prepareGOOD_SPLIT_ROW() {
    SplitTransaction st = new SplitTransaction(this.parent, GOOD_SPLIT_ROW);
    assertTrue(st.prepare());
    // Assert the write lock is held on successful prepare as the javadoc asserts.
    assertTrue(this.parent.splitsAndClosesLock.writeLock().isHeldByCurrentThread());
    return st;
  }

  /**
   * Pass an unreasonable split row.
   */
  @Test public void testPrepareWithBadSplitRow() throws IOException {
    // Pass start row as split key.
    SplitTransaction st = new SplitTransaction(this.parent, STARTROW);
    assertFalse(st.prepare());
    st = new SplitTransaction(this.parent, HConstants.EMPTY_BYTE_ARRAY);
    assertFalse(st.prepare());
    st = new SplitTransaction(this.parent, new byte [] {'A', 'A', 'A'});
    assertFalse(st.prepare());
    st = new SplitTransaction(this.parent, ENDROW);
    assertFalse(st.prepare());
  }

  @Test public void testPrepareWithClosedRegion() throws IOException {
    this.parent.close();
    SplitTransaction st = new SplitTransaction(this.parent, GOOD_SPLIT_ROW);
    assertFalse(st.prepare());
  }

  @Test public void testWholesomeSplit() throws IOException {
    final int rowcount = TEST_UTIL.loadRegion(this.parent, CF);
    assertTrue(rowcount > 0);
    int parentRowCount = countRows(this.parent);
    assertEquals(rowcount, parentRowCount);

    // Start transaction.
    SplitTransaction st = prepareGOOD_SPLIT_ROW();

    // Run the execute.  Look at what it returns.
    PairOfSameType<HRegion> daughters = st.execute(null);
    // Do some assertions about execution.
    assertTrue(this.fs.exists(st.getSplitDir()));
    // Assert the parent region is closed.
    assertTrue(this.parent.isClosed());

    // Assert splitdir is empty -- because its content will have been moved out
    // to be under the daughter region dirs.
    assertEquals(0, this.fs.listStatus(st.getSplitDir()).length);
    // Check daughters have correct key span.
    assertTrue(Bytes.equals(this.parent.getStartKey(),
      daughters.getFirst().getStartKey()));
    assertTrue(Bytes.equals(GOOD_SPLIT_ROW,
      daughters.getFirst().getEndKey()));
    assertTrue(Bytes.equals(daughters.getSecond().getStartKey(),
      GOOD_SPLIT_ROW));
    assertTrue(Bytes.equals(this.parent.getEndKey(),
      daughters.getSecond().getEndKey()));
    // Count rows.
    int daughtersRowCount = 0;
    for (HRegion r: daughters) {
      // Open so can count its content.
      HRegion openRegion = HRegion.openHRegion(r.getRegionInfo(), this.testdir,
        r.getLog(), r.getConf());
      try {
        int count = countRows(openRegion);
        assertTrue(count > 0 && count != rowcount);
        daughtersRowCount += count;
      } finally {
        openRegion.close();
      }
    }
    assertEquals(rowcount, daughtersRowCount);
    // Assert the write lock is no longer held on parent
    assertTrue(!this.parent.splitsAndClosesLock.writeLock().isHeldByCurrentThread());
  }

  @Test public void testRollback() throws IOException {
    final int rowcount = TEST_UTIL.loadRegion(this.parent, CF);
    assertTrue(rowcount > 0);
    int parentRowCount = countRows(this.parent);
    assertEquals(rowcount, parentRowCount);

    // Start transaction.
    SplitTransaction st = prepareGOOD_SPLIT_ROW();
    SplitTransaction spiedUponSt = spy(st);
    when(spiedUponSt.createDaughterRegion(spiedUponSt.getSecondDaughter())).
      thenThrow(new MockedFailedDaughterCreation());
    // Run the execute.  Look at what it returns.
    boolean expectedException = false;
    try {
      spiedUponSt.execute(null);
    } catch (MockedFailedDaughterCreation e) {
      expectedException = true;
    }
    assertTrue(expectedException);
    // Run rollback
    spiedUponSt.rollback(null);

    // Assert I can scan parent.
    int parentRowCount2 = countRows(this.parent);
    assertEquals(parentRowCount, parentRowCount2);

    // Assert rollback cleaned up stuff in fs
    assertTrue(!this.fs.exists(HRegion.getRegionDir(this.testdir, st.getFirstDaughter())));
    assertTrue(!this.fs.exists(HRegion.getRegionDir(this.testdir, st.getSecondDaughter())));
    assertTrue(!this.parent.splitsAndClosesLock.writeLock().isHeldByCurrentThread());

    // Now retry the split but do not throw an exception this time.
    assertTrue(st.prepare());
    PairOfSameType<HRegion> daughters = st.execute(null);
    // Count rows.
    int daughtersRowCount = 0;
    for (HRegion r: daughters) {
      // Open so can count its content.
      HRegion openRegion = HRegion.openHRegion(r.getRegionInfo(), this.testdir,
        r.getLog(), r.getConf());
      try {
        int count = countRows(openRegion);
        assertTrue(count > 0 && count != rowcount);
        daughtersRowCount += count;
      } finally {
        openRegion.close();
      }
    }
    assertEquals(rowcount, daughtersRowCount);
    // Assert the write lock is no longer held on parent
    assertTrue(!this.parent.splitsAndClosesLock.writeLock().isHeldByCurrentThread());
  }

  /**
   * Exception used in this class only.
   */
  @SuppressWarnings("serial")
  private class MockedFailedDaughterCreation extends IOException {}

  private int countRows(final HRegion r) throws IOException {
    int rowcount = 0;
    InternalScanner scanner = r.getScanner(new Scan());
    try {
      List<KeyValue> kvs = new ArrayList<KeyValue>();
      boolean hasNext = true;
      while (hasNext) {
        hasNext = scanner.next(kvs);
        if (!kvs.isEmpty()) rowcount++;
      }
    } finally {
      scanner.close();
    }
    return rowcount;
  }

  static HRegion createRegion(final Path testdir, final HLog wal)
  throws IOException {
    // Make a region with start and end keys. Use 'aaa', to 'AAA'.  The load
    // region utility will add rows between 'aaa' and 'zzz'.
    HTableDescriptor htd = new HTableDescriptor("table");
    HColumnDescriptor hcd = new HColumnDescriptor(CF);
    htd.addFamily(hcd);
    HRegionInfo hri = new HRegionInfo(htd, STARTROW, ENDROW);
    return HRegion.openHRegion(hri, testdir, wal, TEST_UTIL.getConfiguration());
  }
}