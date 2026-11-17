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
package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Test;

/**
 * Tests for dfs.namenode.listing.max-lock-hold-ms,
 * dfs.content-summary.max-lock-hold-ms and dfs.content-summary.max-time-ms.
 * Slowness is injected with test hooks, so that the tests do not depend on
 * the speed of the machine.
 */
public class TestListingAndContentSummaryTimeLimits {
  private static final int FILES = 60;
  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;

  private void start(Configuration conf) throws IOException {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
  }

  @After
  public void tearDown() {
    FSDirStatAndListingOp.listingEntryHookForTests = null;
    ContentSummaryComputationContext.yieldHookForTests = null;
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private static Runnable sleep(final long ms) {
    return () -> {
      try {
        Thread.sleep(ms);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };
  }

  private Path createFlatDir(String name, int n) throws IOException {
    final Path dir = new Path("/" + name);
    fs.mkdirs(dir);
    for (int i = 0; i < n; i++) {
      fs.create(new Path(dir, String.format("f%05d", i))).close();
    }
    return dir;
  }

  /** root with dirs subdirectories, each holding filesPerDir files. */
  private Path createTree(String name, int dirs, int filesPerDir)
      throws IOException {
    final Path root = new Path("/" + name);
    for (int d = 0; d < dirs; d++) {
      final Path sub = new Path(root, String.format("d%04d", d));
      fs.mkdirs(sub);
      for (int f = 0; f < filesPerDir; f++) {
        fs.create(new Path(sub, "f" + f)).close();
      }
    }
    return root;
  }

  private DirectoryListing firstPage(String path) throws IOException {
    return cluster.getNameNodeRpc().getListing(path, HdfsFileStatus.EMPTY_NAME,
        false);
  }

  private static List<String> names(FileStatus[] st) {
    final List<String> l = new ArrayList<>();
    for (FileStatus s : st) {
      l.add(s.getPath().getName());
    }
    return l;
  }

  private static List<String> expectedNames(int n) {
    final List<String> l = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      l.add(String.format("f%05d", i));
    }
    return l;
  }

  // ------------------------------------------------------------- listing

  @Test(timeout = 60000)
  public void testListingDefaultIsUnchanged() throws Exception {
    start(new HdfsConfiguration());
    final Path dir = createFlatDir("plain", FILES);
    FSDirStatAndListingOp.listingEntryHookForTests = sleep(1);
    final DirectoryListing page = firstPage(dir.toString());
    assertEquals(FILES, page.getPartialListing().length);
    assertFalse(page.hasMore());
  }

  @Test(timeout = 60000)
  public void testListingPageCutByTime() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_LISTING_MAX_LOCK_HOLD_MS_KEY, 20);
    start(conf);
    final Path dir = createFlatDir("cut", FILES);
    FSDirStatAndListingOp.listingEntryHookForTests = sleep(5);

    final DirectoryListing page = firstPage(dir.toString());
    final int n = page.getPartialListing().length;
    assertTrue("page should be cut short, got " + n, n >= 1 && n < FILES);
    assertTrue(page.hasMore());

    // the client continues from the last name and gets everything, in order
    assertEquals(expectedNames(FILES), names(fs.listStatus(dir)));
  }

  @Test(timeout = 60000)
  public void testListingCompleteWithConcurrentCreates() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_LISTING_MAX_LOCK_HOLD_MS_KEY, 5);
    start(conf);
    final Path dir = createFlatDir("concurrent", FILES);
    FSDirStatAndListingOp.listingEntryHookForTests = sleep(2);

    final Thread writer = new Thread(() -> {
      try {
        for (int i = 0; i < 50; i++) {
          fs.create(new Path(dir, String.format("e%05d", i))).close();
          fs.create(new Path(dir, String.format("g%05d", i))).close();
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
    writer.start();
    final List<String> listed = names(fs.listStatus(dir));
    writer.join();

    // every file that existed for the whole listing is there exactly once
    final Set<String> seen = new HashSet<>();
    for (String s : listed) {
      assertTrue("duplicate " + s, seen.add(s));
    }
    assertTrue(seen.containsAll(expectedNames(FILES)));
  }

  @Test(timeout = 60000)
  public void testListingInsideSnapshotIsNotCut() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_LISTING_MAX_LOCK_HOLD_MS_KEY, 20);
    start(conf);
    final Path dir = createFlatDir("snap", FILES);
    fs.allowSnapshot(dir);
    fs.createSnapshot(dir, "s1");
    FSDirStatAndListingOp.listingEntryHookForTests = sleep(5);

    final DirectoryListing page = firstPage(dir + "/.snapshot/s1");
    assertEquals(FILES, page.getPartialListing().length);
    assertFalse(page.hasMore());
  }

  // ------------------------------------------------------- content summary

  @Test(timeout = 60000)
  public void testContentSummaryAbortedAfterMaxTime() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_CONTENT_SUMMARY_MAX_TIME_MS_KEY, 100);
    start(conf);
    final Path root = createTree("abort", 40, 2);

    ContentSummaryComputationContext.yieldHookForTests = sleep(10);
    LambdaTestUtils.intercept(IOException.class,
        "dfs.content-summary.max-time-ms", () -> fs.getContentSummary(root));

    // nothing changed and the NameNode still answers; without the injected
    // slowness the same request completes within the limit
    ContentSummaryComputationContext.yieldHookForTests = null;
    final ContentSummary cs = fs.getContentSummary(root);
    assertEquals(41, cs.getDirectoryCount());
    assertEquals(80, cs.getFileCount());
  }

  @Test(timeout = 60000)
  public void testContentSummaryYieldsOnTime() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    // count-based yield effectively off, so only the time limit can yield
    conf.setInt(DFSConfigKeys.DFS_CONTENT_SUMMARY_LIMIT_KEY, 1000000);
    conf.setLong(DFSConfigKeys.DFS_CONTENT_SUMMARY_MAX_LOCK_HOLD_MS_KEY, 5);
    start(conf);
    final Path root = createTree("yield", 20, 3);
    final FSDirectory fsd = cluster.getNamesystem().getFSDirectory();

    ContentSummaryComputationContext.yieldHookForTests = sleep(2);
    final long before = fsd.getYieldCount();
    final ContentSummary cs = fs.getContentSummary(root);
    assertTrue("expected time-based yields", fsd.getYieldCount() > before);
    assertEquals(21, cs.getDirectoryCount());
    assertEquals(60, cs.getFileCount());
  }

  @Test(timeout = 60000)
  public void testContentSummaryDefaultIsUnchanged() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_CONTENT_SUMMARY_LIMIT_KEY, 1000000);
    start(conf);
    final Path root = createTree("default", 20, 3);
    final FSDirectory fsd = cluster.getNamesystem().getFSDirectory();

    ContentSummaryComputationContext.yieldHookForTests = sleep(2);
    final long before = fsd.getYieldCount();
    final ContentSummary cs = fs.getContentSummary(root);
    assertEquals("no time limit: no yield", before, fsd.getYieldCount());
    assertEquals(21, cs.getDirectoryCount());
    assertEquals(60, cs.getFileCount());
  }
}
