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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.NavigableSet;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.Test;

/**
 * Simple testing of a few HLog methods.
 */
public class TestHLogMethods {
  private final HBaseTestingUtility util = new HBaseTestingUtility();

  /**
   * Assert that getSplitEditFilesSorted returns files in expected order and
   * that it skips moved-aside files.
   * @throws IOException
   */
  @Test public void testGetSplitEditFilesSorted() throws IOException {
    FileSystem fs = FileSystem.get(util.getConfiguration());
    Path regiondir = HBaseTestingUtility.getTestDir("regiondir");
    fs.delete(regiondir, true);
    fs.mkdirs(regiondir);
    Path recoverededits = HLog.getRegionDirRecoveredEditsDir(regiondir);
    String first = HLog.formatRecoveredEditsFileName(-1);
    createFile(fs, recoverededits, first);
    createFile(fs, recoverededits, HLog.formatRecoveredEditsFileName(0));
    createFile(fs, recoverededits, HLog.formatRecoveredEditsFileName(1));
    createFile(fs, recoverededits, HLog.formatRecoveredEditsFileName(11));
    createFile(fs, recoverededits, HLog.formatRecoveredEditsFileName(2));
    createFile(fs, recoverededits, HLog.formatRecoveredEditsFileName(50));
    String last = HLog.formatRecoveredEditsFileName(Long.MAX_VALUE);
    createFile(fs, recoverededits, last);
    createFile(fs, recoverededits,
      Long.toString(Long.MAX_VALUE) + "." + System.currentTimeMillis());
    NavigableSet<Path> files = HLog.getSplitEditFilesSorted(fs, regiondir);
    assertEquals(7, files.size());
    assertEquals(files.pollFirst().getName(), first);
    assertEquals(files.pollLast().getName(), last);
    assertEquals(files.pollFirst().getName(),
      HLog.formatRecoveredEditsFileName(0));
    assertEquals(files.pollFirst().getName(),
      HLog.formatRecoveredEditsFileName(1));
    assertEquals(files.pollFirst().getName(),
      HLog.formatRecoveredEditsFileName(2));
    assertEquals(files.pollFirst().getName(),
      HLog.formatRecoveredEditsFileName(11));
  }

  private void createFile(final FileSystem fs, final Path testdir,
      final String name)
  throws IOException {
    FSDataOutputStream fdos = fs.create(new Path(testdir, name), true);
    fdos.close();
  }
}