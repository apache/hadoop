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
package org.apache.hadoop.hdfs.server.namenode.ha;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.io.retry.RetryInvocationHandler;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.MultithreadedTestUtil.RepeatingTestThread;
import org.apache.hadoop.test.MultithreadedTestUtil.TestContext;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.event.Level;

import java.util.function.Supplier;


/**
 * Stress-test for potential bugs when replication is changing
 * on blocks during a failover.
 */
public class TestDNFencingWithReplication {
  static {
    GenericTestUtils.setLogLevel(FSNamesystem.AUDIT_LOG, Level.WARN);
    GenericTestUtils.setLogLevel(Server.LOG, Level.ERROR);
    GenericTestUtils.setLogLevel(RetryInvocationHandler.LOG, Level.ERROR);
  }

  private static final int NUM_THREADS = 20;
  // How long should the test try to run for. In practice
  // it runs for ~20-30s longer than this constant due to startup/
  // shutdown time.
  private static final long RUNTIME = 35000;
  private static final int BLOCK_SIZE = 1024;
  
  private static class ReplicationToggler extends RepeatingTestThread {
    private final FileSystem fs;
    private final Path path;
    private final MiniDFSCluster cluster;

    ReplicationToggler(TestContext ctx, FileSystem fs, Path p,
                       MiniDFSCluster cluster) {
      super(ctx);
      this.fs = fs;
      this.path = p;
      this.cluster = cluster;
    }

    @Override
    public void doAnAction() throws Exception {
      fs.setReplication(path, (short)1);
      waitForReplicas(1);
      fs.setReplication(path, (short)2);
      waitForReplicas(2);
    }
    
    private void waitForReplicas(final int replicas) throws Exception {
      try {
        GenericTestUtils.waitFor(new Supplier<Boolean>() {
          @Override
          public Boolean get() {
            try {
              cluster.waitActive();
              BlockLocation[] blocks = fs.getFileBlockLocations(path, 0, 10);
              Assert.assertEquals(1, blocks.length);
              return blocks[0].getHosts().length == replicas;
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }, 100, 60000);
      } catch (TimeoutException te) {
        throw new IOException("Timed out waiting for " + replicas +
                " replicas on path " + path);
      }
    }
    
    @Override
    public String toString() {
      return "Toggler for " + path;
    }
  }
  
  @Test
  public void testFencingStress() throws Exception {
    HAStressTestHarness harness = new HAStressTestHarness();
    harness.setNumberOfNameNodes(3);
    harness.conf.setInt(
        DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 1000);
    harness.conf.setInt(
        DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1);
    harness.conf.setInt(
        DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY, 1);

    final MiniDFSCluster cluster = harness.startCluster();
    try {
      cluster.waitActive();
      cluster.transitionToActive(0);
      
      FileSystem fs = harness.getFailoverFs();
      TestContext togglers = new TestContext();
      for (int i = 0; i < NUM_THREADS; i++) {
        Path p = new Path("/test-" + i);
        DFSTestUtil.createFile(fs, p, BLOCK_SIZE*10, (short)3, (long)i);
        togglers.addThread(new ReplicationToggler(togglers, fs, p, cluster));
      }
      
      // Start a separate thread which will make sure that replication
      // happens quickly by triggering deletion reports and replication
      // work calculation frequently.
      harness.addReplicationTriggerThread(500);
      harness.addFailoverThread(5000);
      harness.startThreads();
      togglers.startThreads();
      
      togglers.waitFor(RUNTIME);
      togglers.stop();
      harness.stopThreads();

      // CHeck that the files can be read without throwing
      for (int i = 0; i < NUM_THREADS; i++) {
        Path p = new Path("/test-" + i);
        DFSTestUtil.readFile(fs, p);
      }
    } finally {
      System.err.println("===========================\n\n\n\n");
      harness.shutdown();
    }

  }
}
