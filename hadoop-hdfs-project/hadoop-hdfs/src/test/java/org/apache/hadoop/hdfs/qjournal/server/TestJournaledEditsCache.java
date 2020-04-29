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
package org.apache.hadoop.hdfs.qjournal.server;

import com.google.common.primitives.Bytes;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLayoutVersion;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import org.apache.hadoop.test.PathUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.hdfs.qjournal.QJMTestUtil.createGabageTxns;
import static org.apache.hadoop.hdfs.qjournal.QJMTestUtil.createTxnData;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * Test the {@link JournaledEditsCache} used for caching edits in-memory on the
 * {@link Journal}.
 */
public class TestJournaledEditsCache {

  private static final int EDITS_CAPACITY = 100;

  private static final File TEST_DIR =
      PathUtils.getTestDir(TestJournaledEditsCache.class, false);
  private JournaledEditsCache cache;

  @Before
  public void setup() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_JOURNALNODE_EDIT_CACHE_SIZE_KEY,
        createTxnData(1, 1).length * EDITS_CAPACITY);
    cache = new JournaledEditsCache(conf);
    TEST_DIR.mkdirs();
  }

  @After
  public void cleanup() throws Exception {
    FileUtils.deleteQuietly(TEST_DIR);
  }

  @Test
  public void testCacheSingleSegment() throws Exception {
    storeEdits(1, 20);
    // Leading part of the segment
    assertTxnCountAndContents(1, 5, 5);
    // All of the segment
    assertTxnCountAndContents(1, 20, 20);
    // Past the segment
    assertTxnCountAndContents(1, 40, 20);
    // Trailing part of the segment
    assertTxnCountAndContents(10, 11, 20);
    // Trailing part of the segment, past the end
    assertTxnCountAndContents(10, 20, 20);
  }

  @Test
  public void testCacheBelowCapacityRequestOnBoundary() throws Exception {
    storeEdits(1, 5);
    storeEdits(6, 20);
    storeEdits(21, 30);

    // First segment only
    assertTxnCountAndContents(1, 3, 3);
    // Second segment only
    assertTxnCountAndContents(6, 10, 15);
    // First and second segment
    assertTxnCountAndContents(1, 7, 7);
    // All three segments
    assertTxnCountAndContents(1, 25, 25);
    // Second and third segment
    assertTxnCountAndContents(6, 20, 25);
    // Second and third segment; request past the end
    assertTxnCountAndContents(6, 50, 30);
    // Third segment only; request past the end
    assertTxnCountAndContents(21, 20, 30);
  }

  @Test
  public void testCacheBelowCapacityRequestOffBoundary() throws Exception {
    storeEdits(1, 5);
    storeEdits(6, 20);
    storeEdits(21, 30);

    // First segment only
    assertTxnCountAndContents(3, 1, 3);
    // First and second segment
    assertTxnCountAndContents(3, 6, 8);
    // Second and third segment
    assertTxnCountAndContents(15, 10, 24);
    // Second and third segment; request past the end
    assertTxnCountAndContents(15, 50, 30);
    // Start read past the end
    List<ByteBuffer> buffers = new ArrayList<>();
    assertEquals(0, cache.retrieveEdits(31, 10, buffers));
    assertTrue(buffers.isEmpty());
  }

  @Test
  public void testCacheAboveCapacity() throws Exception {
    int thirdCapacity = EDITS_CAPACITY / 3;
    storeEdits(1, thirdCapacity);
    storeEdits(thirdCapacity + 1, thirdCapacity * 2);
    storeEdits(thirdCapacity * 2 + 1, EDITS_CAPACITY);
    storeEdits(EDITS_CAPACITY + 1, thirdCapacity * 4);
    storeEdits(thirdCapacity * 4 + 1, thirdCapacity * 5);

    try {
      cache.retrieveEdits(1, 10, new ArrayList<>());
      fail();
    } catch (IOException ioe) {
      // expected
    }
    assertTxnCountAndContents(EDITS_CAPACITY + 1, EDITS_CAPACITY,
        thirdCapacity * 5);
  }

  @Test
  public void testCacheSingleAdditionAboveCapacity() throws Exception {
    LogCapturer logs = LogCapturer.captureLogs(Journal.LOG);
    storeEdits(1, EDITS_CAPACITY * 2);
    logs.stopCapturing();
    assertTrue(logs.getOutput().contains("batch of edits was too large"));
    try {
      cache.retrieveEdits(1, 1, new ArrayList<>());
      fail();
    } catch (IOException ioe) {
      // expected
    }
    storeEdits(EDITS_CAPACITY * 2 + 1, EDITS_CAPACITY * 2 + 5);
    assertTxnCountAndContents(EDITS_CAPACITY * 2 + 1, 5,
        EDITS_CAPACITY * 2 + 5);
  }

  @Test
  public void testCacheWithFutureLayoutVersion() throws Exception {
    byte[] firstHalf = createGabageTxns(1, 5);
    byte[] secondHalf = createGabageTxns(6, 5);
    int futureVersion = NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION - 1;
    cache.storeEdits(Bytes.concat(firstHalf, secondHalf), 1, 10,
        futureVersion);
    List<ByteBuffer> buffers = new ArrayList<>();
    assertEquals(5, cache.retrieveEdits(6, 5, buffers));
    assertArrayEquals(getHeaderForLayoutVersion(futureVersion),
        buffers.get(0).array());
    byte[] retBytes = new byte[buffers.get(1).remaining()];
    System.arraycopy(buffers.get(1).array(), buffers.get(1).position(),
        retBytes, 0, buffers.get(1).remaining());
    assertArrayEquals(secondHalf, retBytes);
  }

  @Test
  public void testCacheWithMultipleLayoutVersions() throws Exception {
    int oldLayout = NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION + 1;
    cache.storeEdits(createTxnData(1, 5), 1, 5, oldLayout);
    storeEdits(6, 10);
    // Ensure the cache will only return edits from a single
    // layout version at a time
    try {
      cache.retrieveEdits(1, 50, new ArrayList<>());
      fail("Expected a cache miss");
    } catch (JournaledEditsCache.CacheMissException cme) {
      // expected
    }
    assertTxnCountAndContents(6, 50, 10);
  }

  @Test
  public void testCacheEditsWithGaps() throws Exception {
    storeEdits(1, 5);
    storeEdits(10, 15);

    try {
      cache.retrieveEdits(1, 20, new ArrayList<>());
      fail();
    } catch (JournaledEditsCache.CacheMissException cme) {
      assertEquals(9, cme.getCacheMissAmount());
    }
    assertTxnCountAndContents(10, 10, 15);
  }

  @Test(expected = JournaledEditsCache.CacheMissException.class)
  public void testReadUninitializedCache() throws Exception {
    cache.retrieveEdits(1, 10, new ArrayList<>());
  }

  @Test(expected = JournaledEditsCache.CacheMissException.class)
  public void testCacheMalformedInput() throws Exception {
    storeEdits(1, 1);
    cache.retrieveEdits(-1, 10, new ArrayList<>());
  }

  private void storeEdits(int startTxn, int endTxn) throws Exception {
    cache.storeEdits(createTxnData(startTxn, endTxn - startTxn + 1), startTxn,
        endTxn, NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
  }

  private void assertTxnCountAndContents(int startTxn, int requestedMaxTxns,
      int expectedEndTxn) throws Exception {
    List<ByteBuffer> buffers = new ArrayList<>();
    int expectedTxnCount = expectedEndTxn - startTxn + 1;
    assertEquals(expectedTxnCount,
        cache.retrieveEdits(startTxn, requestedMaxTxns, buffers));

    byte[] expectedBytes = Bytes.concat(
        getHeaderForLayoutVersion(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION),
        createTxnData(startTxn, expectedTxnCount));
    byte[] actualBytes =
        new byte[buffers.stream().mapToInt(ByteBuffer::remaining).sum()];
    int pos = 0;
    for (ByteBuffer buf : buffers) {
      System.arraycopy(buf.array(), buf.position(), actualBytes, pos,
          buf.remaining());
      pos += buf.remaining();
    }
    assertArrayEquals(expectedBytes, actualBytes);
  }

  private static byte[] getHeaderForLayoutVersion(int version)
      throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    EditLogFileOutputStream.writeHeader(version, new DataOutputStream(baos));
    return baos.toByteArray();
  }

}
