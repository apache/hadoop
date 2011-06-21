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

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.Test;


/**
 * Tests for {@link FSTableDescriptors}.
 */
public class TestFSTableDescriptors {
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @Test
  public void testRemoves() throws IOException {
    final String name = "testRemoves";
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    // Cleanup old tests if any detrius laying around.
    Path rootdir = new Path(HBaseTestingUtility.getTestDir(), name);
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
    Path rootdir = HBaseTestingUtility.getTestDir(name);
    createHTDInFS(fs, rootdir, htd);
    HTableDescriptor htd2 =
      FSUtils.getTableDescriptor(fs, rootdir, htd.getNameAsString());
    assertTrue(htd.equals(htd2));
  }

  private void createHTDInFS(final FileSystem fs, Path rootdir,
      final HTableDescriptor htd)
  throws IOException {
    FSUtils.createTableDescriptor(fs, rootdir, htd);
  }

  @Test public void testHTableDescriptors()
  throws IOException, InterruptedException {
    final String name = "testHTableDescriptors";
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    // Cleanup old tests if any detrius laying around.
    Path rootdir = new Path(HBaseTestingUtility.getTestDir(), name);
    final int count = 10;
    // Write out table infos.
    for (int i = 0; i < count; i++) {
      HTableDescriptor htd = new HTableDescriptor(name + i);
      createHTDInFS(fs, rootdir, htd);
    }
    FSTableDescriptors htds = new FSTableDescriptors(fs, rootdir);
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
      FSUtils.updateHTableDescriptor(fs, rootdir, htd);
    }
    for (int i = 0; i < count; i++) {
      assertTrue(htds.get(Bytes.toBytes(name + i)) !=  null);
    }
    for (int i = 0; i < count; i++) {
      assertTrue(htds.get(Bytes.toBytes(name + i)) !=  null);
    }
    assertEquals(htds.invocations, count * 4);
    assertEquals(htds.cachehits, count * 2);
    assertTrue(htds.get(HConstants.ROOT_TABLE_NAME) != null);
    assertEquals(htds.invocations, count * 4 + 1);
    assertEquals(htds.cachehits, count * 2 + 1);
  }

  @Test (expected=java.io.FileNotFoundException.class)
  public void testNoSuchTable() throws IOException {
    final String name = "testNoSuchTable";
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    // Cleanup old tests if any detrius laying around.
    Path rootdir = new Path(HBaseTestingUtility.getTestDir(), name);
    TableDescriptors htds = new FSTableDescriptors(fs, rootdir);
    htds.get("NoSuchTable");
  }

  @Test
  public void testUpdates() throws IOException {
    final String name = "testUpdates";
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    // Cleanup old tests if any detrius laying around.
    Path rootdir = new Path(HBaseTestingUtility.getTestDir(), name);
    TableDescriptors htds = new FSTableDescriptors(fs, rootdir);
    HTableDescriptor htd = new HTableDescriptor(name);
    htds.add(htd);
    htds.add(htd);
    htds.add(htd);
  }
}