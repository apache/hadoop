/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.Whitebox;
import org.apache.hadoop.util.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.fail;

public class TestBlockRecoveryCauseStandbyNameNodeCrash {
  public static final Logger LOG = LoggerFactory
      .getLogger(TestBlockRecoveryCauseStandbyNameNodeCrash.class);
  private final ErasureCodingPolicy ecPolicy =
      StripedFileTestUtil.getDefaultECPolicy();
  private final int dataBlocks = ecPolicy.getNumDataUnits();
  private final int parityBlocks = ecPolicy.getNumParityUnits();
  private final int cellSize = ecPolicy.getCellSize();
  private final int stripesPerBlock = 4;
  private final int blockSize = cellSize * stripesPerBlock;

  private final String fakeUsername = "fakeUser1";
  private final String fakeGroup = "supergroup";

  private MiniDFSCluster cluster;
  private DistributedFileSystem dfs;
  private Configuration conf;
  private Configuration newConf;
  private final Path dir = new Path("/" + this.getClass().getSimpleName());
  private Path p = new Path(dir, "testfile");

  @BeforeEach
  public void setup() throws IOException {
    conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setLong(HdfsClientConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 60000L);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 0);
    conf.setInt(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
    final int numDNs = dataBlocks + parityBlocks;
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(numDNs)
        .nnTopology(MiniDFSNNTopology.simpleHATopology(2, 50070))
        .build();
    cluster.waitActive();
    cluster.transitionToActive(0);
    newConf = cluster.getConfiguration(0);
    dfs = cluster.getFileSystem(0);
    dfs.enableErasureCodingPolicy(ecPolicy.getName());
    dfs.mkdirs(dir);
    dfs.setErasureCodingPolicy(dir, ecPolicy.getName());
  }

  @AfterEach
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * 1. PauseIBR on some datanodes and write 25MB data (two block groups).
   * 2. Mock client quiet exceptionally.
   * 3. Trigger lease recovery.
   * 4. Standby NameNode crashed.
   */
  @Test
  public void testCommitBlockSynchronizationWithDeleteECBlockgroupCommitted() {
    int curCellSize = (int) 1024 * 1024;
    try {
      for (int i = 0; i < parityBlocks + 1; i++) {
        DataNodeTestUtils.pauseIBR(cluster.getDataNodes().get(i));
      }
      final FSDataOutputStream out = dfs.create(p);
      final DFSStripedOutputStream stripedOut = (DFSStripedOutputStream) out
          .getWrappedStream();
      for (int pos = 0; pos < (stripesPerBlock * dataBlocks + 1) * curCellSize; pos++) {
        out.write(StripedFileTestUtil.getByte(pos));
      }
      for (int i = 0; i < dataBlocks + parityBlocks; i++) {
        StripedDataStreamer s = stripedOut.getStripedDataStreamer(i);
        waitStreamerAllAcked(s);
        stopBlockStream(s);
      }
      recoverLease();
      LOG.info("Trigger recover lease manually successfully.");
    } catch (Throwable e) {
      String msg = "failed testCase" + StringUtils.stringifyException(e);
      fail(msg);
    } finally {
      for (int i = 0; i < parityBlocks + 1; i++) {
        DataNodeTestUtils.resumeIBR(cluster.getDataNodes().get(i));
      }
    }
  }

  /**
   * Stop the block stream without immediately inducing a hard failure.
   * Packets can continue to be queued until the streamer hits a socket timeout.
   *
   * @param s the streamer to stop.
   * @throws Exception
   */
  private void stopBlockStream(StripedDataStreamer s) throws Exception {
    IOUtils.NullOutputStream nullOutputStream = new IOUtils.NullOutputStream();
    Whitebox.setInternalState(s, "blockStream",
        new DataOutputStream(nullOutputStream));
  }

  private void recoverLease() throws Exception {
    final DistributedFileSystem dfs2 =
        (DistributedFileSystem) getFSAsAnotherUser(newConf);
    try {
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          try {
            return dfs2.recoverLease(p);
          } catch (IOException e) {
            LOG.info("BZL#Test. recoverLease() failed: " + e.getMessage());
            return false;
          }
        }
      }, 5000, 24000);
    } catch (TimeoutException e) {
      throw new IOException("Timeout waiting for recoverLease()");
    }
  }

  private FileSystem getFSAsAnotherUser(final Configuration c)
      throws IOException, InterruptedException {
    return FileSystem.get(FileSystem.getDefaultUri(c), c,
        UserGroupInformation
            .createUserForTesting(fakeUsername, new String[]{fakeGroup})
            .getUserName());
  }

  public static void waitStreamerAllAcked(DataStreamer s) throws IOException {
    long toWaitFor = s.getLastQueuedSeqno();
    s.waitForAckedSeqno(toWaitFor);
  }
}
