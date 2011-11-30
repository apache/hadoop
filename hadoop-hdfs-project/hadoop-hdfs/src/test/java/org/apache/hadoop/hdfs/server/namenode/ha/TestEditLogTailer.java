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

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.log4j.Level;
import org.junit.Test;

public class TestEditLogTailer {
  
  private static final String DIR_PREFIX = "/dir";
  private static final int DIRS_TO_MAKE = 20;
  private static final long SLEEP_TIME = 1000;
  private static final long NN_LAG_TIMEOUT = 10 * 1000;
  
  static {
    ((Log4JLogger)FSImage.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)EditLogTailer.LOG).getLogger().setLevel(Level.ALL);
  }
  
  @Test
  public void testTailer() throws IOException, InterruptedException,
      ServiceFailedException {
    Configuration conf = new HdfsConfiguration();
    
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleHATopology())
      .numDataNodes(0)
      .build();
    cluster.waitActive();
    
    cluster.transitionToActive(0);
    
    NameNode nn1 = cluster.getNameNode(0);
    NameNode nn2 = cluster.getNameNode(1);
    nn2.getNamesystem().getEditLogTailer().setSleepTime(250);
    nn2.getNamesystem().getEditLogTailer().interrupt();
    try {
      for (int i = 0; i < DIRS_TO_MAKE / 2; i++) {
        NameNodeAdapter.mkdirs(nn1, getDirPath(i),
            new PermissionStatus("test","test", new FsPermission((short)00755)),
            true);
      }
      
      waitForStandbyToCatchUp(nn1, nn2);
      
      for (int i = 0; i < DIRS_TO_MAKE / 2; i++) {
        assertTrue(NameNodeAdapter.getFileInfo(nn2,
            getDirPath(i), false).isDir());
      }
      
      for (int i = DIRS_TO_MAKE / 2; i < DIRS_TO_MAKE; i++) {
        NameNodeAdapter.mkdirs(nn1, getDirPath(i),
            new PermissionStatus("test","test", new FsPermission((short)00755)),
            true);
      }
      
      waitForStandbyToCatchUp(nn1, nn2);
      
      for (int i = DIRS_TO_MAKE / 2; i < DIRS_TO_MAKE; i++) {
        assertTrue(NameNodeAdapter.getFileInfo(nn2,
            getDirPath(i), false).isDir());
      }
    } finally {
      cluster.shutdown();
    }
  }
  
  private static String getDirPath(int suffix) {
    return DIR_PREFIX + suffix;
  }
  
  private static void waitForStandbyToCatchUp(NameNode active,
      NameNode standby) throws InterruptedException, IOException {
    
    long activeTxId = active.getNamesystem().getFSImage().getEditLog()
      .getLastWrittenTxId();
    
    doSaveNamespace(active);
    
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < NN_LAG_TIMEOUT) {
      long nn2HighestTxId = standby.getNamesystem().getFSImage()
        .getLastAppliedTxId();
      if (nn2HighestTxId >= activeTxId) {
        break;
      }
      Thread.sleep(SLEEP_TIME);
    }
  }
  
  private static void doSaveNamespace(NameNode nn)
      throws IOException {
    NameNodeAdapter.enterSafeMode(nn, false);
    NameNodeAdapter.saveNamespace(nn);
    NameNodeAdapter.leaveSafeMode(nn, false);
  }
  
}
