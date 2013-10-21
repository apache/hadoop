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

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.ClientBaseWithFixes;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStoreTestBase.TestDispatcher;
import org.apache.hadoop.util.ZKUtil;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CyclicBarrier;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestZKRMStateStoreZKClientConnections extends
    ClientBaseWithFixes {

  private static final int ZK_OP_WAIT_TIME = 3000;
  private Log LOG =
      LogFactory.getLog(TestZKRMStateStoreZKClientConnections.class);

  class TestZKClient {

    ZKRMStateStore store;
    boolean forExpire = false;
    TestForwardingWatcher watcher;
    CyclicBarrier syncBarrier = new CyclicBarrier(2);

    protected class TestZKRMStateStore extends ZKRMStateStore {

      public TestZKRMStateStore(Configuration conf, String workingZnode)
          throws Exception {
        init(conf);
        start();
        assertTrue(znodeWorkingPath.equals(workingZnode));
      }

      @Override
      public ZooKeeper getNewZooKeeper()
          throws IOException, InterruptedException {
        return createClient(watcher, hostPort, 100);
      }

      @Override
      public synchronized void processWatchEvent(WatchedEvent event)
          throws Exception {

        if (forExpire) {
          // a hack... couldn't find a way to trigger expired event.
          WatchedEvent expriredEvent = new WatchedEvent(
              Watcher.Event.EventType.None,
              Watcher.Event.KeeperState.Expired, null);
          super.processWatchEvent(expriredEvent);
          forExpire = false;
          syncBarrier.await();
        } else {
          super.processWatchEvent(event);
        }
      }
    }

    private class TestForwardingWatcher extends
        ClientBaseWithFixes.CountdownWatcher {

      public void process(WatchedEvent event) {
        super.process(event);
        try {
          if (store != null) {
            store.processWatchEvent(event);
          }
        } catch (Throwable t) {
          LOG.error("Failed to process watcher event " + event + ": "
              + StringUtils.stringifyException(t));
        }
      }
    }

    public RMStateStore getRMStateStore(Configuration conf) throws Exception {
      String workingZnode = "/Test";
      conf.set(YarnConfiguration.ZK_RM_STATE_STORE_ADDRESS, hostPort);
      conf.set(YarnConfiguration.ZK_RM_STATE_STORE_PARENT_PATH, workingZnode);
      watcher = new TestForwardingWatcher();
      this.store = new TestZKRMStateStore(conf, workingZnode);
      return this.store;
    }
  }

  @Test(timeout = 20000)
  public void testZKClientDisconnectAndReconnect()
      throws Exception {

    TestZKClient zkClientTester = new TestZKClient();
    String path = "/test";
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.ZK_RM_STATE_STORE_TIMEOUT_MS, 100);
    ZKRMStateStore store =
        (ZKRMStateStore) zkClientTester.getRMStateStore(conf);
    TestDispatcher dispatcher = new TestDispatcher();
    store.setRMDispatcher(dispatcher);

    // trigger watch
    store.createWithRetries(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);
    store.getDataWithRetries(path, true);
    store.setDataWithRetries(path, "newBytes".getBytes(), 0);

    stopServer();
    zkClientTester.watcher.waitForDisconnected(ZK_OP_WAIT_TIME);
    try {
      store.getDataWithRetries(path, true);
      fail("Expected ZKClient time out exception");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains(
          "Wait for ZKClient creation timed out"));
    }

    // ZKRMStateStore Session restored
    startServer();
    zkClientTester.watcher.waitForConnected(ZK_OP_WAIT_TIME);
    byte[] ret = null;
    try {
      ret = store.getDataWithRetries(path, true);
    } catch (Exception e) {
      String error = "ZKRMStateStore Session restore failed";
      LOG.error(error, e);
      fail(error);
    }
    Assert.assertEquals("newBytes", new String(ret));
  }

  @Test(timeout = 20000)
  public void testZKSessionTimeout() throws Exception {

    TestZKClient zkClientTester = new TestZKClient();
    String path = "/test";
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.ZK_RM_STATE_STORE_TIMEOUT_MS, 100);
    ZKRMStateStore store =
        (ZKRMStateStore) zkClientTester.getRMStateStore(conf);
    TestDispatcher dispatcher = new TestDispatcher();
    store.setRMDispatcher(dispatcher);

    // a hack to trigger expired event
    zkClientTester.forExpire = true;

    // trigger watch
    store.createWithRetries(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);
    store.getDataWithRetries(path, true);
    store.setDataWithRetries(path, "bytes".getBytes(), 0);

    zkClientTester.syncBarrier.await();
    // after this point, expired event has already been processed.

    try {
      byte[] ret = store.getDataWithRetries(path, false);
      Assert.assertEquals("bytes", new String(ret));
    } catch (Exception e) {
      String error = "New session creation failed";
      LOG.error(error, e);
      fail(error);
    }
  }

  @Test(timeout = 20000)
  public void testSetZKAcl() {
    TestZKClient zkClientTester = new TestZKClient();
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.ZK_RM_STATE_STORE_ACL, "world:anyone:rwca");
    try {
      zkClientTester.store.zkClient.delete(zkClientTester.store
          .znodeWorkingPath, -1);
      fail("Shouldn't be able to delete path");
    } catch (Exception e) {/* expected behavior */
    }
  }

  @Test(timeout = 20000)
  public void testInvalidZKAclConfiguration() {
    TestZKClient zkClientTester = new TestZKClient();
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.ZK_RM_STATE_STORE_ACL, "randomstring&*");
    try {
      zkClientTester.getRMStateStore(conf);
      fail("ZKRMStateStore created with bad ACL");
    } catch (ZKUtil.BadAclFormatException bafe) {
      // expected behavior
    } catch (Exception e) {
      String error = "Incorrect exception on BadAclFormat";
      LOG.error(error, e);
      fail(error);
    }
  }
}
