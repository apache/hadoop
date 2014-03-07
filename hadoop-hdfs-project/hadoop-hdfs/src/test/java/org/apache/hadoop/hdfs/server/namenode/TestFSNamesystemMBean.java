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
}
