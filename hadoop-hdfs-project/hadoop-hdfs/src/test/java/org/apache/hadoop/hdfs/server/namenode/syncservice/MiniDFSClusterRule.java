/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.syncservice;

import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.net.NetUtils;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class MiniDFSClusterRule implements TestRule {

  private MiniDFSCluster cluster;
  final private boolean isSyncServiceActive;

  MiniDFSClusterRule() {
    this(false);
  }

  MiniDFSClusterRule(boolean isSyncServiceActive) {
    this.isSyncServiceActive = isSyncServiceActive;
  }

  @Override
  public Statement apply(Statement statement, Description description) {
    return new Statement() {

      @Override
      public void evaluate() throws Throwable {
        // set stuff up
        int aliasMapPort = NetUtils.getFreeSocketPort();
        File tempDir = Files.createTempDir();
        Configuration conf = getTestConfiguration();
        conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, false);
        conf.set(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS,
            "localhost:" + aliasMapPort);
        conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_PROVIDED_ENABLED, isSyncServiceActive);
        conf.setBoolean(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_ENABLED, true);
        conf.set(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_LEVELDB_DIR, tempDir.getAbsolutePath());
        try {
          cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
          statement.evaluate();
        } finally {
          if (cluster != null) {
            cluster.shutdown();
          }
        }
      }
    };
  }

  private HdfsConfiguration getTestConfiguration() {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 0);

    return conf;
  }

  public MiniDFSCluster getCluster() {
    return cluster;
  }
}


