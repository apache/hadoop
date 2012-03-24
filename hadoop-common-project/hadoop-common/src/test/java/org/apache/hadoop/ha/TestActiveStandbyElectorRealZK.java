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

package org.apache.hadoop.ha;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.ha.ActiveStandbyElector.ActiveStandbyElectorCallback;
import org.apache.hadoop.test.MultithreadedTestUtil.TestContext;
import org.apache.hadoop.test.MultithreadedTestUtil.TestingThread;
import org.apache.log4j.Level;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.test.ClientBase;

/**
 * Test for {@link ActiveStandbyElector} using real zookeeper.
 */
public class TestActiveStandbyElectorRealZK extends ClientBase {
  static final int NUM_ELECTORS = 2;
  static ZooKeeper[] zkClient = new ZooKeeper[NUM_ELECTORS];
  
  static {
    ((Log4JLogger)ActiveStandbyElector.LOG).getLogger().setLevel(
        Level.ALL);
  }
  
  int activeIndex = -1;
  int standbyIndex = -1;
  static final String PARENT_DIR = "/" + UUID.randomUUID();

  ActiveStandbyElector[] electors = new ActiveStandbyElector[NUM_ELECTORS];
  
  @Override
  public void setUp() throws Exception {
    // build.test.dir is used by zookeeper
    new File(System.getProperty("build.test.dir", "build")).mkdirs();
    super.setUp();
  }

  /**
   * The class object runs on a thread and waits for a signal to start from the 
   * test object. On getting the signal it joins the election and thus by doing 
   * this on multiple threads we can test simultaneous attempts at leader lock 
   * creation. after joining the election, the object waits on a signal to exit.
   * this signal comes when the object's elector has become a leader or there is 
   * an unexpected fatal error. this lets another thread object to become a 
   * leader.
   */
  class ThreadRunner extends TestingThread
      implements  ActiveStandbyElectorCallback {
    int index;
    
    CountDownLatch hasBecomeActive = new CountDownLatch(1);

    ThreadRunner(TestContext ctx,
        int idx) {
      super(ctx);
      index = idx;
    }

    @Override
    public void doWork() throws Exception {
      LOG.info("starting " + index);
      // join election
      byte[] data = new byte[1];
      data[0] = (byte)index;
      
      ActiveStandbyElector elector = electors[index];
      LOG.info("joining " + index);
      elector.joinElection(data);

      hasBecomeActive.await(30, TimeUnit.SECONDS);
      Thread.sleep(1000);

      // quit election to allow other elector to become active
      elector.quitElection(true);

      LOG.info("ending " + index);
    }

    @Override
    public synchronized void becomeActive() {
      reportActive(index);
      LOG.info("active " + index);
      hasBecomeActive.countDown();
    }

    @Override
    public synchronized void becomeStandby() {
      reportStandby(index);
      LOG.info("standby " + index);
    }

    @Override
    public synchronized void enterNeutralMode() {
      LOG.info("neutral " + index);
    }

    @Override
    public synchronized void notifyFatalError(String errorMessage) {
      LOG.info("fatal " + index + " .Error message:" + errorMessage);
      this.interrupt();
    }

    @Override
    public void fenceOldActive(byte[] data) {
      LOG.info("fenceOldActive " + index);
      // should not fence itself
      Assert.assertTrue(index != data[0]);
    }
  }

  synchronized void reportActive(int index) {
    if (activeIndex == -1) {
      activeIndex = index;
    } else {
      // standby should become active
      Assert.assertEquals(standbyIndex, index);
      // old active should not become active
      Assert.assertFalse(activeIndex == index);
    }
    activeIndex = index;
  }

  synchronized void reportStandby(int index) {
    // only 1 standby should be reported and it should not be the same as active
    Assert.assertEquals(-1, standbyIndex);
    standbyIndex = index;
    Assert.assertFalse(activeIndex == standbyIndex);
  }

  /**
   * the test creates 2 electors which try to become active using a real
   * zookeeper server. It verifies that 1 becomes active and 1 becomes standby.
   * Upon becoming active the leader quits election and the test verifies that
   * the standby now becomes active. these electors run on different threads and 
   * callback to the test class to report active and standby where the outcome 
   * is verified
   * @throws Exception 
   */
  @Test
  public void testActiveStandbyTransition() throws Exception {
    LOG.info("starting test with parentDir:" + PARENT_DIR);

    TestContext ctx = new TestContext();
    
    for(int i = 0; i < NUM_ELECTORS; i++) {
      LOG.info("creating " + i);
      final ZooKeeper zk = createClient();
      assert zk != null;
      
      ThreadRunner tr = new ThreadRunner(ctx, i);
      electors[i] = new ActiveStandbyElector(
          "hostPort", 1000, PARENT_DIR, Ids.OPEN_ACL_UNSAFE,
          tr) {
        @Override
        protected synchronized ZooKeeper getNewZooKeeper()
            throws IOException {
          return zk;
        }
      };
      ctx.addThread(tr);
    }

    assertFalse(electors[0].parentZNodeExists());
    electors[0].ensureParentZNode();
    assertTrue(electors[0].parentZNodeExists());

    ctx.startThreads();
    ctx.stop();
  }
}
