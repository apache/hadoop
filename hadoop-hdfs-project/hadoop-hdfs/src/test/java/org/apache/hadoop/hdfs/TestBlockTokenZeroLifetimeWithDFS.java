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

package org.apache.hadoop.hdfs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests validation of {@code dfs.block.access.token.lifetime} (HDFS-17947).
 *
 * A non-positive lifetime makes the NameNode mint block access tokens that are
 * already expired when a DataNode verifies them, so every write pipeline fails
 * with {@code InvalidBlockTokenException} out of
 * {@code DataStreamer.createBlockOutputStream}. Rather than surfacing that as a
 * confusing runtime failure, the NameNode now rejects the value at startup.
 */
public class TestBlockTokenZeroLifetimeWithDFS {

  private static final int BLOCK_SIZE = 1024;
  private static final int FILE_SIZE = 2 * BLOCK_SIZE;
  private static final short REPLICATION = 1;
  private static final Path FILE = new Path("/hdfs-17947.dat");

  private MiniDFSCluster cluster;

  @AfterEach
  public void shutdownCluster() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private static Configuration getConf(long tokenLifetimeMinutes) {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY,
        tokenLifetimeMinutes);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, REPLICATION);
    conf.setInt("io.bytes.per.checksum", 512);
    // Fail fast rather than grinding through the default client retry budget.
    conf.setInt(HdfsClientConfigKeys.BlockWrite.RETRIES_KEY, 0);
    conf.setInt(
        HdfsClientConfigKeys.BlockWrite.LOCATEFOLLOWINGBLOCK_RETRIES_KEY, 1);
    conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 10);
    return conf;
  }

  /**
   * The startup failure may be wrapped by the NameNode/MiniDFSCluster startup
   * path, so match against the whole cause chain rather than the top-level
   * message.
   */
  private static String causeChain(Throwable t) {
    StringBuilder sb = new StringBuilder();
    for (Throwable c = t; c != null; c = c.getCause()) {
      sb.append(c).append(System.lineSeparator());
    }
    return sb.toString();
  }

  private static void assertRejected(Exception e) {
    String chain = causeChain(e);
    assertTrue(
        chain.contains(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY),
        "the failure must name the offending config key, but was:\n" + chain);
    assertTrue(chain.contains("must be a positive number of minutes"),
        "the failure must explain the constraint, but was:\n" + chain);
  }

  /**
   * Formatting builds an FSNamesystem and therefore a BlockManager, so the
   * value is rejected before a NameNode is ever started.
   */
  private void assertFormatRejects(long tokenLifetimeMinutes) {
    Exception e = assertThrows(Exception.class,
        () -> {
          cluster = new MiniDFSCluster.Builder(getConf(tokenLifetimeMinutes))
              .numDataNodes(REPLICATION).build();
        },
        "formatting must be refused with "
            + DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY + " = "
            + tokenLifetimeMinutes);
    assertRejected(e);
  }

  @Test
  public void testFormatRejectsZeroBlockTokenLifetime() {
    assertFormatRejects(0);
  }

  @Test
  public void testFormatRejectsNegativeBlockTokenLifetime() {
    assertFormatRejects(-1);
  }

  /**
   * An already-formatted NameNode also refuses to start up if the value is
   * changed to a non-positive one, which is the upgrade path an operator would
   * actually hit.
   */
  @Test
  public void testNameNodeStartupRejectsZeroBlockTokenLifetime()
      throws Exception {
    cluster = new MiniDFSCluster.Builder(getConf(600))
        .numDataNodes(REPLICATION).build();
    cluster.waitActive();

    cluster.shutdownNameNode(0);
    cluster.getConfiguration(0).setLong(
        DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY, 0);

    Exception e = assertThrows(Exception.class,
        () -> cluster.restartNameNode(0),
        "an already-formatted NameNode must refuse to start with "
            + DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY + " = 0");
    assertRejected(e);
  }

  /**
   * Control: the documented default of 600 minutes starts and writes normally.
   */
  @Test
  public void testWriteSucceedsWithDefaultBlockTokenLifetime()
      throws Exception {
    cluster = new MiniDFSCluster.Builder(getConf(600))
        .numDataNodes(REPLICATION).build();
    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();

    try (FSDataOutputStream out = fs.create(FILE, REPLICATION)) {
      out.write(new byte[FILE_SIZE]);
    }

    assertEquals(FILE_SIZE, fs.getFileStatus(FILE).getLen(),
        "control case with dfs.block.access.token.lifetime=600 must succeed");
  }

  /**
   * Block access tokens are only created when they are enabled, so a
   * non-positive lifetime is irrelevant (and must not be rejected) when
   * {@code dfs.block.access.token.enable} is false.
   */
  @Test
  public void testZeroLifetimeIgnoredWhenBlockTokensDisabled()
      throws Exception {
    Configuration conf = getConf(0);
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, false);

    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(REPLICATION).build();
    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();

    try (FSDataOutputStream out = fs.create(FILE, REPLICATION)) {
      out.write(new byte[FILE_SIZE]);
    }

    assertEquals(FILE_SIZE, fs.getFileStatus(FILE).getLen(),
        "a cluster with block tokens disabled must ignore the lifetime value");
  }
}
