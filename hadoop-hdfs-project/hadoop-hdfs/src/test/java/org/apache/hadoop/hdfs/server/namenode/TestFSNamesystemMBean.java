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

import static org.junit.Assert.*;

import java.lang.management.ManagementFactory;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Test;
import org.mortbay.util.ajax.JSON;

/**
 * Class for testing {@link NameNodeMXBean} implementation
 */
public class TestFSNamesystemMBean {

  /**
   * MBeanClient tries to access FSNamesystem/FSNamesystemState/NameNodeInfo
   * JMX properties. If it can access all the properties, the test is
   * considered successful.
   */
  private static class MBeanClient extends Thread {
    private boolean succeeded = false;
    @Override
    public void run() {
      try {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

        // Metrics that belong to "FSNamesystem", these are metrics that
        // come from hadoop metrics framework for the class FSNamesystem.
        ObjectName mxbeanNamefsn = new ObjectName(
            "Hadoop:service=NameNode,name=FSNamesystem");
        Integer blockCapacity = (Integer) (mbs.getAttribute(mxbeanNamefsn,
            "BlockCapacity"));

        // Metrics that belong to "FSNamesystemState".
        // These are metrics that FSNamesystem registers directly with MBeanServer.
        ObjectName mxbeanNameFsns = new ObjectName(
            "Hadoop:service=NameNode,name=FSNamesystemState");
        String FSState = (String) (mbs.getAttribute(mxbeanNameFsns,
            "FSState"));
        Long blocksTotal = (Long) (mbs.getAttribute(mxbeanNameFsns,
            "BlocksTotal"));
        Long capacityTotal = (Long) (mbs.getAttribute(mxbeanNameFsns,
            "CapacityTotal"));
        Long capacityRemaining = (Long) (mbs.getAttribute(mxbeanNameFsns,
            "CapacityRemaining"));
        Long capacityUsed = (Long) (mbs.getAttribute(mxbeanNameFsns,
            "CapacityUsed"));
        Long filesTotal = (Long) (mbs.getAttribute(mxbeanNameFsns,
            "FilesTotal"));
        Long pendingReplicationBlocks = (Long) (mbs.getAttribute(mxbeanNameFsns,
            "PendingReplicationBlocks"));
        Long underReplicatedBlocks = (Long) (mbs.getAttribute(mxbeanNameFsns,
            "UnderReplicatedBlocks"));
        Long scheduledReplicationBlocks = (Long) (mbs.getAttribute(mxbeanNameFsns,
            "ScheduledReplicationBlocks"));
        Integer totalLoad = (Integer) (mbs.getAttribute(mxbeanNameFsns,
            "TotalLoad"));
        Integer numLiveDataNodes = (Integer) (mbs.getAttribute(mxbeanNameFsns,
            "NumLiveDataNodes"));
        Integer numDeadDataNodes = (Integer) (mbs.getAttribute(mxbeanNameFsns,
           "NumDeadDataNodes"));
        Integer numStaleDataNodes = (Integer) (mbs.getAttribute(mxbeanNameFsns,
            "NumStaleDataNodes"));
        Integer numDecomLiveDataNodes = (Integer) (mbs.getAttribute(mxbeanNameFsns,
            "NumDecomLiveDataNodes"));
        Integer numDecomDeadDataNodes = (Integer) (mbs.getAttribute(mxbeanNameFsns,
            "NumDecomDeadDataNodes"));
        Integer numDecommissioningDataNodes = (Integer) (mbs.getAttribute(mxbeanNameFsns,
            "NumDecommissioningDataNodes"));
        String snapshotStats = (String) (mbs.getAttribute(mxbeanNameFsns,
            "SnapshotStats"));
        Long MaxObjects = (Long) (mbs.getAttribute(mxbeanNameFsns,
            "MaxObjects"));
        Integer numStaleStorages = (Integer) (mbs.getAttribute(
            mxbeanNameFsns, "NumStaleStorages"));

        // Metrics that belong to "NameNodeInfo".
        // These are metrics that FSNamesystem registers directly with MBeanServer.
        ObjectName mxbeanNameNni = new ObjectName(
            "Hadoop:service=NameNode,name=NameNodeInfo");
        String safemode = (String) (mbs.getAttribute(mxbeanNameNni,
            "Safemode"));
        String liveNodes = (String) (mbs.getAttribute(mxbeanNameNni,
            "LiveNodes"));
        String deadNodes = (String) (mbs.getAttribute(mxbeanNameNni,
            "DeadNodes"));
        String decomNodes = (String) (mbs.getAttribute(mxbeanNameNni,
            "DecomNodes"));
        String corruptFiles = (String) (mbs.getAttribute(mxbeanNameNni,
            "CorruptFiles"));

        succeeded = true;
      } catch (Exception e) {
      }
    }
  }

  @Test
  public void test() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();

      FSNamesystem fsn = cluster.getNameNode().namesystem;

      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName mxbeanName = new ObjectName(
          "Hadoop:service=NameNode,name=FSNamesystemState");

      String snapshotStats = (String) (mbs.getAttribute(mxbeanName,
          "SnapshotStats"));

      @SuppressWarnings("unchecked")
      Map<String, Object> stat = (Map<String, Object>) JSON
          .parse(snapshotStats);

      assertTrue(stat.containsKey("SnapshottableDirectories")
          && (Long) stat.get("SnapshottableDirectories") == fsn
              .getNumSnapshottableDirs());
      assertTrue(stat.containsKey("Snapshots")
          && (Long) stat.get("Snapshots") == fsn.getNumSnapshots());

      Object pendingDeletionBlocks = mbs.getAttribute(mxbeanName,
        "PendingDeletionBlocks");
      assertNotNull(pendingDeletionBlocks);
      assertTrue(pendingDeletionBlocks instanceof Long);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  // The test makes sure JMX request can be processed even if namesystem's
  // writeLock is owned by another thread.
  @Test
  public void testWithFSNamesystemWriteLock() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    FSNamesystem fsn = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();

      fsn = cluster.getNameNode().namesystem;
      fsn.writeLock();

      MBeanClient client = new MBeanClient();
      client.start();
      client.join(20000);
      assertTrue("JMX calls are blocked when FSNamesystem's writerlock" +
          "is owned by another thread", client.succeeded);
      client.interrupt();
    } finally {
      if (fsn != null && fsn.hasWriteLock()) {
        fsn.writeUnlock();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}