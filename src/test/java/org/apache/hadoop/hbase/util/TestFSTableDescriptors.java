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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableExistsException;
import org.junit.Test;


/**
 * Tests for {@link FSTableDescriptors}.
 */
public class TestFSTableDescriptors {
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final Log LOG = LogFactory.getLog(TestFSTableDescriptors.class);

  @Test (expected=IllegalArgumentException.class)
  public void testRegexAgainstOldStyleTableInfo() {
    Path p = new Path("/tmp", FSTableDescriptors.TABLEINFO_NAME);
    int i = FSTableDescriptors.getTableInfoSequenceid(p);
    assertEquals(0, i);
    // Assert it won't eat garbage -- that it fails
    p = new Path("/tmp", "abc");
    FSTableDescriptors.getTableInfoSequenceid(p);
  }

  @Test
  public void testCreateAndUpdate() throws IOException {
    Path testdir = UTIL.getDataTestDir();
    HTableDescriptor htd = new HTableDescriptor("testCreate");
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    assertTrue(FSTableDescriptors.createTableDescriptor(fs, testdir, htd));
    assertFalse(FSTableDescriptors.createTableDescriptor(fs, testdir, htd));
    FileStatus [] statuses = fs.listStatus(testdir);
    assertTrue(statuses.length == 1);
    for (int i = 0; i < 10; i++) {
      FSTableDescriptors.updateHTableDescriptor(fs, testdir, htd);
    }
    statuses = fs.listStatus(testdir);
    assertTrue(statuses.length == 1);
    Path tmpTableDir = new Path(FSUtils.getTablePath(testdir, htd.getName()), ".tmp");
    statuses = fs.listStatus(tmpTableDir);
    assertTrue(statuses.length == 0);
  }

  @Test
  public void testSequenceidAdvancesOnTableInfo() throws IOException {
    Path testdir = UTIL.getDataTestDir("testSequenceidAdvancesOnTableInfo");
    HTableDescriptor htd = new HTableDescriptor("testSequenceidAdvancesOnTableInfo");
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    Path p0 = FSTableDescriptors.updateHTableDescriptor(fs, testdir, htd);
    int i0 = FSTableDescriptors.getTableInfoSequenceid(p0);
    Path p1 = FSTableDescriptors.updateHTableDescriptor(fs, testdir, htd);
    // Assert we cleaned up the old file.
    assertTrue(!fs.exists(p0));
    int i1 = FSTableDescriptors.getTableInfoSequenceid(p1);
    assertTrue(i1 == i0 + 1);
    Path p2 = FSTableDescriptors.updateHTableDescriptor(fs, testdir, htd);
    // Assert we cleaned up the old file.
    assertTrue(!fs.exists(p1));
    int i2 = FSTableDescriptors.getTableInfoSequenceid(p2);
    assertTrue(i2 == i1 + 1);
  }

  @Test
  public void testFormatTableInfoSequenceId() {
    Path p0 = assertWriteAndReadSequenceid(0);
    // Assert p0 has format we expect.
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < FSTableDescriptors.WIDTH_OF_SEQUENCE_ID; i++) {
      sb.append("0");
    }
    assertEquals(FSTableDescriptors.TABLEINFO_NAME + "." + sb.toString(),
      p0.getName());
    // Check a few more.
    Path p2 = assertWriteAndReadSequenceid(2);
    Path p10000 = assertWriteAndReadSequenceid(10000);
    // Get a .tablinfo that has no sequenceid suffix.
    Path p = new Path(p0.getParent(), FSTableDescriptors.TABLEINFO_NAME);
    FileStatus fs = new FileStatus(0, false, 0, 0, 0, p);
    FileStatus fs0 = new FileStatus(0, false, 0, 0, 0, p0);
    FileStatus fs2 = new FileStatus(0, false, 0, 0, 0, p2);
    FileStatus fs10000 = new FileStatus(0, false, 0, 0, 0, p10000);
    FSTableDescriptors.FileStatusFileNameComparator comparator =
      new FSTableDescriptors.FileStatusFileNameComparator();
    assertTrue(comparator.compare(fs, fs0) > 0);
    assertTrue(comparator.compare(fs0, fs2) > 0);
    assertTrue(comparator.compare(fs2, fs10000) > 0);
  }

  private Path assertWriteAndReadSequenceid(final int i) {
    Path p = FSTableDescriptors.getTableInfoFileName(new Path("/tmp"), i);
    int ii = FSTableDescriptors.getTableInfoSequenceid(p);
    assertEquals(i, ii);
    return p;
  }

  @Test
  public void testRemoves() throws IOException {
    final String name = "testRemoves";
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    // Cleanup old tests if any detrius laying around.
    Path rootdir = new Path(UTIL.getDataTestDir(), name);
    TableDescriptors htds = new FSTableDescriptors(fs, rootdir);
    HTableDescriptor htd = new HTableDescriptor(name);
    htds.add(htd);
    assertNotNull(htds.remove(htd.getNameAsString()));
    assertNull(htds.remove(htd.getNameAsString()));
  }

  @Test public void testReadingHTDFromFS() throws IOException {
    final String name = "testReadingHTDFromFS";
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    HTableDescriptor htd = new HTableDescriptor(name);
    Path rootdir = UTIL.getDataTestDir(name);
    createHTDInFS(fs, rootdir, htd);
    HTableDescriptor htd2 =
      FSTableDescriptors.getTableDescriptor(fs, rootdir, htd.getNameAsString());
    assertTrue(htd.equals(htd2));
  }

  private void createHTDInFS(final FileSystem fs, Path rootdir,
      final HTableDescriptor htd)
  throws IOException {
    FSTableDescriptors.createTableDescriptor(fs, rootdir, htd);
  }

  @Test public void testHTableDescriptors()
  throws IOException, InterruptedException {
    final String name = "testHTableDescriptors";
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    // Cleanup old tests if any debris laying around.
    Path rootdir = new Path(UTIL.getDataTestDir(), name);
    final int count = 10;
    // Write out table infos.
    for (int i = 0; i < count; i++) {
      HTableDescriptor htd = new HTableDescriptor(name + i);
      createHTDInFS(fs, rootdir, htd);
    }
    FSTableDescriptors htds = new FSTableDescriptors(fs, rootdir) {
      @Override
      public HTableDescriptor get(byte[] tablename)
          throws TableExistsException, FileNotFoundException, IOException {
        LOG.info(Bytes.toString(tablename) + ", cachehits=" + this.cachehits);
        return super.get(tablename);
      }
    };
    for (int i = 0; i < count; i++) {
      assertTrue(htds.get(Bytes.toBytes(name + i)) !=  null);
    }
    for (int i = 0; i < count; i++) {
      assertTrue(htds.get(Bytes.toBytes(name + i)) !=  null);
    }
    // Update the table infos
    for (int i = 0; i < count; i++) {
      HTableDescriptor htd = new HTableDescriptor(name + i);
      htd.addFamily(new HColumnDescriptor("" + i));
      FSTableDescriptors.updateHTableDescriptor(fs, rootdir, htd);
    }
    // Wait a while so mod time we write is for sure different.
    Thread.sleep(100);
    for (int i = 0; i < count; i++) {
      assertTrue(htds.get(Bytes.toBytes(name + i)) !=  null);
    }
    for (int i = 0; i < count; i++) {
      assertTrue(htds.get(Bytes.toBytes(name + i)) !=  null);
    }
    assertEquals(count * 4, htds.invocations);
    assertTrue("expected=" + (count * 2) + ", actual=" + htds.cachehits,
      htds.cachehits >= (count * 2));
    assertTrue(htds.get(HConstants.ROOT_TABLE_NAME) != null);
    assertEquals(htds.invocations, count * 4 + 1);
    assertTrue("expected=" + ((count * 2) + 1) + ", actual=" + htds.cachehits,
      htds.cachehits >= ((count * 2) + 1));
  }

  @Test (expected=org.apache.hadoop.hbase.TableExistsException.class)
  public void testNoSuchTable() throws IOException {
    final String name = "testNoSuchTable";
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    // Cleanup old tests if any detrius laying around.
    Path rootdir = new Path(UTIL.getDataTestDir(), name);
    TableDescriptors htds = new FSTableDescriptors(fs, rootdir);
    htds.get("NoSuchTable");
  }

  @Test
  public void testUpdates() throws IOException {
    final String name = "testUpdates";
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    // Cleanup old tests if any detrius laying around.
    Path rootdir = new Path(UTIL.getDataTestDir(), name);
    TableDescriptors htds = new FSTableDescriptors(fs, rootdir);
    HTableDescriptor htd = new HTableDescriptor(name);
    htds.add(htd);
    htds.add(htd);
    htds.add(htd);
  }

  @Test
  public void testTableInfoFileStatusComparator() {
    FileStatus bare =
      new FileStatus(0, false, 0, 0, -1, new Path("/tmp", FSTableDescriptors.TABLEINFO_NAME));
    FileStatus future =
      new FileStatus(0, false, 0, 0, -1,
        new Path("/tmp/tablinfo." + System.currentTimeMillis()));
    FileStatus farFuture =
      new FileStatus(0, false, 0, 0, -1,
        new Path("/tmp/tablinfo." + System.currentTimeMillis() + 1000));
    FileStatus [] alist = {bare, future, farFuture};
    FileStatus [] blist = {bare, farFuture, future};
    FileStatus [] clist = {farFuture, bare, future};
    FSTableDescriptors.FileStatusFileNameComparator c =
      new FSTableDescriptors.FileStatusFileNameComparator();
    Arrays.sort(alist, c);
    Arrays.sort(blist, c);
    Arrays.sort(clist, c);
    // Now assert all sorted same in way we want.
    for (int i = 0; i < alist.length; i++) {
      assertTrue(alist[i].equals(blist[i]));
      assertTrue(blist[i].equals(clist[i]));
      assertTrue(clist[i].equals(i == 0? farFuture: i == 1? future: bare));
    }
  }
}