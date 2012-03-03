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

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.ha.ActiveStandbyElector.ActiveStandbyElectorCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.test.ClientBase;

/**
 * Test for {@link ActiveStandbyElector} using real zookeeper.
 */
public class TestActiveStandbyElectorRealZK extends ClientBase {
  static final int NUM_ELECTORS = 2;
  static ZooKeeper[] zkClient = new ZooKeeper[NUM_ELECTORS];
  static int currentClientIndex = 0;
  
  @Override
  public void setUp() throws Exception {
    // build.test.dir is used by zookeeper
    new File(System.getProperty("build.test.dir", "build")).mkdirs();
    super.setUp();
  }

  class ActiveStandbyElectorTesterRealZK extends ActiveStandbyElector {
    ActiveStandbyElectorTesterRealZK(String hostPort, int timeout,
        String parent, List<ACL> acl, ActiveStandbyElectorCallback app)
        throws IOException {
      super(hostPort, timeout, parent, acl, app);
    }

    @Override
    public ZooKeeper getNewZooKeeper() {
      return TestActiveStandbyElectorRealZK.zkClient[
                             TestActiveStandbyElectorRealZK.currentClientIndex];
    }
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
  class ThreadRunner implements Runnable, ActiveStandbyElectorCallback {
    int index;
    TestActiveStandbyElectorRealZK test;
    boolean wait = true;

    ThreadRunner(int i, TestActiveStandbyElectorRealZK s) {
      index = i;
      test = s;
    }

    @Override
    public void run() {
      LOG.info("starting " + index);
      while(true) {
        synchronized (test) {
          // wait for test start signal to come
          if (!test.start) {
            try {
              test.wait();
            } catch(InterruptedException e) {
              Assert.fail(e.getMessage());
            }
          } else {
            break;
          }
        }
      }
      // join election
      byte[] data = new byte[8];
      ActiveStandbyElector elector = test.elector[index];
      LOG.info("joining " + index);
      elector.joinElection(data);
      try {
        while(true) {
          synchronized (this) {
            // wait for elector to become active/fatal error
            if (wait) {
              // wait to become active
              // wait capped at 30s to prevent hung test
              wait(30000);
            } else {
              break;
            }
          }
        }
        Thread.sleep(1000);
        // quit election to allow other elector to become active
        elector.quitElection();
      } catch(InterruptedException e) {
        Assert.fail(e.getMessage());
      }
      LOG.info("ending " + index);
    }

    @Override
    public synchronized void becomeActive() {
      test.reportActive(index);
      LOG.info("active " + index);
      wait = false;
      notifyAll();
    }

    @Override
    public synchronized void becomeStandby() {
      test.reportStandby(index);
      LOG.info("standby " + index);
    }

    @Override
    public synchronized void enterNeutralMode() {
      LOG.info("neutral " + index);
    }

    @Override
    public synchronized void notifyFatalError(String errorMessage) {
      LOG.info("fatal " + index + " .Error message:" + errorMessage);
      wait = false;
      notifyAll();
    }
  }

  boolean start = false;
  int activeIndex = -1;
  int standbyIndex = -1;
  String parentDir = "/" + java.util.UUID.randomUUID().toString();

  ActiveStandbyElector[] elector = new ActiveStandbyElector[NUM_ELECTORS];
  ThreadRunner[] threadRunner = new ThreadRunner[NUM_ELECTORS];
  Thread[] thread = new Thread[NUM_ELECTORS];

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
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   */
  @Test
  public void testActiveStandbyTransition() throws IOException,
      InterruptedException, KeeperException {
    LOG.info("starting test with parentDir:" + parentDir);
    start = false;
    byte[] data = new byte[8];
    // create random working directory
    createClient().create(parentDir, data, Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);

    for(currentClientIndex = 0; 
        currentClientIndex < NUM_ELECTORS; 
        ++currentClientIndex) {
      LOG.info("creating " + currentClientIndex);
      zkClient[currentClientIndex] = createClient();
      threadRunner[currentClientIndex] = new ThreadRunner(currentClientIndex,
          this);
      elector[currentClientIndex] = new ActiveStandbyElectorTesterRealZK(
          "hostPort", 1000, parentDir, Ids.OPEN_ACL_UNSAFE,
          threadRunner[currentClientIndex]);
      zkClient[currentClientIndex].register(elector[currentClientIndex]);
      thread[currentClientIndex] = new Thread(threadRunner[currentClientIndex]);
      thread[currentClientIndex].start();
    }

    synchronized (this) {
      // signal threads to start
      LOG.info("signaling threads");
      start = true;
      notifyAll();
    }

    for(int i = 0; i < thread.length; i++) {
      thread[i].join();
    }
  }
}
