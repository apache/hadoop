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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_STATE_CONTEXT_ENABLED_KEY;
import static org.apache.hadoop.hdfs.server.namenode.ha.ObserverReadProxyProvider.OBSERVER_PROBE_RETRY_PERIOD_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestStandbyRollEditsLogOnly {
  private static Configuration conf;
  private static MiniQJMHACluster qjmhaCluster;
  private static MiniDFSCluster dfsCluster;
  @BeforeClass
  public static void startUpCluster() throws Exception {
    conf = new Configuration();
    conf.setBoolean(DFS_NAMENODE_STATE_CONTEXT_ENABLED_KEY, true);
    conf.set(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY, "5");
    conf.setTimeDuration(
        OBSERVER_PROBE_RETRY_PERIOD_KEY, 0, TimeUnit.MILLISECONDS);
    qjmhaCluster = HATestUtil.setUpObserverCluster(conf, 1, 1, true);
    dfsCluster = qjmhaCluster.getDfsCluster();
  }

  @Test(timeout=60000)
  public void testOnlyStandbyRollEditlog() throws IOException,
      InterruptedException {
    dfsCluster.transitionToActive(0);
    dfsCluster.transitionToStandby(1);
    dfsCluster.transitionToObserver(2);
    dfsCluster.waitActive();
    NameNode standbyNameNode = dfsCluster.getNameNode(1);
    NameNode observerNameNode = dfsCluster.getNameNode(2);
    Assert.assertEquals("transitionToStandby failed !",
        "standby",standbyNameNode.getNamesystem().getHAState() );
    Assert.assertEquals("transitionToObserver failed !",
        "observer",observerNameNode.getNamesystem().getHAState());

    long standbyInitialRollTime =
        standbyNameNode.getNamesystem().getEditLogTailer().getLastRollTimeMs();
    long observerInitialRollTime =
        observerNameNode.getNamesystem().getEditLogTailer().getLastRollTimeMs();
    //wait for roll edits log
    Thread.sleep(6000);
    long standbyLastRollTime  =
        standbyNameNode.getNamesystem().getEditLogTailer().getLastRollTimeMs();
    long observerLastRollTime =
        observerNameNode.getNamesystem().getEditLogTailer().getLastRollTimeMs();
    assertTrue("Standby should roll the log!",
        standbyLastRollTime > standbyInitialRollTime);
    assertEquals("The observer is not expected to roll the log.",
        observerInitialRollTime , observerLastRollTime);
  }

  @Test(timeout=60000)
  public void testTransObToStandbyThenRollLog() throws IOException,
      InterruptedException {

    int standbyNameNodeIndex = getStandbyNameNode();
    int observerNameNodeIndex = getObserverNameNode();
    assert standbyNameNodeIndex > 0;
    assert observerNameNodeIndex > 0;
    dfsCluster.transitionToObserver(standbyNameNodeIndex);
    dfsCluster.transitionToStandby(observerNameNodeIndex);
    NameNode standbyNameNode = dfsCluster.getNameNode(observerNameNodeIndex);
    NameNode observerNameNode = dfsCluster.getNameNode(standbyNameNodeIndex);
    Assert.assertEquals("transitionToStandby failed !",
        "standby",standbyNameNode.getNamesystem().getHAState() );
    Assert.assertEquals("transitionToObserver failed !",
        "observer",observerNameNode.getNamesystem().getHAState());

    long standbyInitialRollTime =
        standbyNameNode.getNamesystem().getEditLogTailer().getLastRollTimeMs();
    long observerInitialRollTime =
        observerNameNode.getNamesystem().getEditLogTailer().getLastRollTimeMs();
    //wait for roll edits log
    Thread.sleep(6000);
    long standbyLastRollTime =
        standbyNameNode.getNamesystem().getEditLogTailer().getLastRollTimeMs();
    long observerLastRollTime =
        observerNameNode.getNamesystem().getEditLogTailer().getLastRollTimeMs();
    assertTrue("Standby should roll the log",
        standbyLastRollTime > standbyInitialRollTime);
    Assert.assertEquals("The observer is not expected to roll the log.",
        observerInitialRollTime , observerLastRollTime);
  }

  private int getObserverNameNode(){
    for (int i = 0; i < dfsCluster.getNumNameNodes(); i++) {
      if(dfsCluster.getNameNode(i).isObserverState()){
        return i;
      }
    }
    return -1;
  }

  private int getStandbyNameNode(){
    for (int i = 0; i < dfsCluster.getNumNameNodes(); i++) {
      if(dfsCluster.getNameNode(i).isStandbyState()){
        return i;
      }
    }
    return -1;
  }

  @AfterClass
  public static void shutDownCluster() throws IOException {
    if (qjmhaCluster != null) {
      qjmhaCluster.shutdown();
    }
  }
}
