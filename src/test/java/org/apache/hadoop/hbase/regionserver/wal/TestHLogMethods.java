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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValueTestUtil;
import org.apache.hadoop.hbase.MultithreadedTestUtil;
import org.apache.hadoop.hbase.MultithreadedTestUtil.TestContext;
import org.apache.hadoop.hbase.MultithreadedTestUtil.TestThread;
import org.apache.hadoop.hbase.regionserver.wal.HLogSplitter.EntryBuffers;
import org.apache.hadoop.hbase.regionserver.wal.HLogSplitter.RegionEntryBuffer;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import static org.mockito.Mockito.mock;

/**
 * Simple testing of a few HLog methods.
 */
public class TestHLogMethods {
  private static final byte[] TEST_REGION = Bytes.toBytes("test_region");;
  private static final byte[] TEST_TABLE = Bytes.toBytes("test_table");
  
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
    String first = HLogSplitter.formatRecoveredEditsFileName(-1);
    createFile(fs, recoverededits, first);
    createFile(fs, recoverededits, HLogSplitter.formatRecoveredEditsFileName(0));
    createFile(fs, recoverededits, HLogSplitter.formatRecoveredEditsFileName(1));
    createFile(fs, recoverededits, HLogSplitter
        .formatRecoveredEditsFileName(11));
    createFile(fs, recoverededits, HLogSplitter.formatRecoveredEditsFileName(2));
    createFile(fs, recoverededits, HLogSplitter
        .formatRecoveredEditsFileName(50));
    String last = HLogSplitter.formatRecoveredEditsFileName(Long.MAX_VALUE);
    createFile(fs, recoverededits, last);
    createFile(fs, recoverededits,
      Long.toString(Long.MAX_VALUE) + "." + System.currentTimeMillis());
    NavigableSet<Path> files = HLog.getSplitEditFilesSorted(fs, regiondir);
    assertEquals(7, files.size());
    assertEquals(files.pollFirst().getName(), first);
    assertEquals(files.pollLast().getName(), last);
    assertEquals(files.pollFirst().getName(),
      HLogSplitter
        .formatRecoveredEditsFileName(0));
    assertEquals(files.pollFirst().getName(),
      HLogSplitter
        .formatRecoveredEditsFileName(1));
    assertEquals(files.pollFirst().getName(),
      HLogSplitter
        .formatRecoveredEditsFileName(2));
    assertEquals(files.pollFirst().getName(),
      HLogSplitter
        .formatRecoveredEditsFileName(11));
  }

  private void createFile(final FileSystem fs, final Path testdir,
      final String name)
  throws IOException {
    FSDataOutputStream fdos = fs.create(new Path(testdir, name), true);
    fdos.close();
  }

  @Test
  public void testRegionEntryBuffer() throws Exception {
    HLogSplitter.RegionEntryBuffer reb = new HLogSplitter.RegionEntryBuffer(
        TEST_TABLE, TEST_REGION);
    assertEquals(0, reb.heapSize());

    reb.appendEntry(createTestLogEntry(1));
    assertTrue(reb.heapSize() > 0);
  }
  
  @Test
  public void testEntrySink() throws Exception {
    Configuration conf = new Configuration();
    HLogSplitter splitter = HLogSplitter.createLogSplitter(
        conf, mock(Path.class), mock(Path.class), mock(Path.class),
        mock(FileSystem.class));

    EntryBuffers sink = splitter.new EntryBuffers(1*1024*1024);
    for (int i = 0; i < 1000; i++) {
      HLog.Entry entry = createTestLogEntry(i);
      sink.appendEntry(entry);
    }
    
    assertTrue(sink.totalBuffered > 0);
    long amountInChunk = sink.totalBuffered;
    // Get a chunk
    RegionEntryBuffer chunk = sink.getChunkToWrite();
    assertEquals(chunk.heapSize(), amountInChunk);
    
    // Make sure it got marked that a thread is "working on this"
    assertTrue(sink.isRegionCurrentlyWriting(TEST_REGION));

    // Insert some more entries
    for (int i = 0; i < 500; i++) {
      HLog.Entry entry = createTestLogEntry(i);
      sink.appendEntry(entry);
    }    
    // Asking for another chunk shouldn't work since the first one
    // is still writing
    assertNull(sink.getChunkToWrite());
    
    // If we say we're done writing the first chunk, then we should be able
    // to get the second
    sink.doneWriting(chunk);
    
    RegionEntryBuffer chunk2 = sink.getChunkToWrite();
    assertNotNull(chunk2);
    assertNotSame(chunk, chunk2);
    long amountInChunk2 = sink.totalBuffered;
    // The second chunk had fewer rows than the first
    assertTrue(amountInChunk2 < amountInChunk);
    
    sink.doneWriting(chunk2);
    assertEquals(0, sink.totalBuffered);
  }
  
  private HLog.Entry createTestLogEntry(int i) {
    long seq = i;
    long now = i * 1000;

    WALEdit edit = new WALEdit();
    edit.add(KeyValueTestUtil.create("row", "fam", "qual", 1234, "val"));
    HLogKey key = new HLogKey(TEST_REGION, TEST_TABLE, seq, now,
        HConstants.DEFAULT_CLUSTER_ID);
    HLog.Entry entry = new HLog.Entry(key, edit);
    return entry;
  }
}
