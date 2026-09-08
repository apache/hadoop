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
package org.apache.hadoop.hdfs.server.datanode;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests the DataNode-side wiring of the block-transfer inactivity timeout: the
 * shared scheduler is created only when the timeout is enabled, and is
 * configured with the correct lifecycle policies (remove-on-cancel so the
 * per-block checks cancelled on completion do not accumulate, and no execution
 * of delayed tasks after shutdown). This exercises the actual feature behavior
 * rather than only round-tripping the configuration value.
 */
public class TestBlockReceiverLastPacketTimeout {

  private MiniDFSCluster cluster;

  @AfterEach
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  private ScheduledExecutorService startAndGetService(Long timeoutMs)
      throws IOException {
    Configuration conf = new HdfsConfiguration();
    if (timeoutMs != null) {
      conf.setLong(DFSConfigKeys.DFS_DATANODE_LAST_PACKET_RECEIVE_TIMEOUT_MS,
          timeoutMs);
    }
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    return cluster.getDataNodes().get(0).getBlockTransferTimeoutService();
  }

  /**
   * When the timeout is enabled the DataNode must create the scheduler and
   * configure it so cancelled per-block checks are removed from the queue
   * immediately and no delayed tasks run after shutdown.
   */
  @Test
  @Timeout(60)
  public void testSchedulerCreatedAndConfiguredWhenEnabled() throws Exception {
    ScheduledExecutorService svc = startAndGetService(600_000L);
    assertNotNull(svc, "scheduler should be created when timeout > 0");
    assertTrue(svc instanceof ScheduledThreadPoolExecutor,
        "scheduler should be a ScheduledThreadPoolExecutor");
    ScheduledThreadPoolExecutor exec = (ScheduledThreadPoolExecutor) svc;
    assertTrue(exec.getRemoveOnCancelPolicy(),
        "cancelled timeout checks must be removed from the queue "
        + "immediately");
    assertFalse(exec.getExecuteExistingDelayedTasksAfterShutdownPolicy(),
        "delayed checks must not run after shutdown");
  }

  /** With the default (unset) config the scheduler must not be created. */
  @Test
  @Timeout(60)
  public void testSchedulerAbsentByDefault() throws Exception {
    assertNull(startAndGetService(null), "scheduler should not be created by default (disabled)");
  }

  /** A zero timeout disables the feature, so no scheduler is created. */
  @Test
  @Timeout(60)
  public void testSchedulerAbsentWhenZero() throws Exception {
    assertNull(startAndGetService(0L), "scheduler should not be created when timeout == 0");
  }

  /** A negative timeout disables the feature, so no scheduler is created. */
  @Test
  @Timeout(60)
  public void testSchedulerAbsentWhenNegative() throws Exception {
    assertNull(startAndGetService(-1L), "scheduler should not be created when timeout < 0");
  }
}
