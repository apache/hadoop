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
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Tests for upgrading with HA enabled.
 */
public class TestDFSUpgradeWithHA {
  
  private static final Log LOG = LogFactory.getLog(TestDFSUpgradeWithHA.class);

  /**
   * Make sure that an HA NN refuses to start if given an upgrade-related
   * startup option.
   */
  @Test
  public void testStartingWithUpgradeOptionsFails() throws IOException {
    for (StartupOption startOpt : Lists.newArrayList(new StartupOption[] {
        StartupOption.UPGRADE, StartupOption.FINALIZE,
        StartupOption.ROLLBACK })) {
      MiniDFSCluster cluster = null;
      try {
        cluster = new MiniDFSCluster.Builder(new Configuration())
            .nnTopology(MiniDFSNNTopology.simpleHATopology())
            .startupOption(startOpt)
            .numDataNodes(0)
            .build();
        fail("Should not have been able to start an HA NN in upgrade mode");
      } catch (IllegalArgumentException iae) {
        GenericTestUtils.assertExceptionContains(
            "Cannot perform DFS upgrade with HA enabled.", iae);
        LOG.info("Got expected exception", iae);
      } finally {
        if (cluster != null) {
          cluster.shutdown();
        }
      }
    }
  }
  
  /**
   * Make sure that an HA NN won't start if a previous upgrade was in progress.
   */
  @Test
  public void testStartingWithUpgradeInProgressFails() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(new Configuration())
          .nnTopology(MiniDFSNNTopology.simpleHATopology())
          .numDataNodes(0)
          .build();
      
      // Simulate an upgrade having started.
      for (int i = 0; i < 2; i++) {
        for (URI uri : cluster.getNameDirs(i)) {
          File prevTmp = new File(new File(uri), Storage.STORAGE_TMP_PREVIOUS);
          LOG.info("creating previous tmp dir: " + prevTmp);
          assertTrue(prevTmp.mkdirs());
        }
      }
      
      cluster.restartNameNodes();
      fail("Should not have been able to start an HA NN with an in-progress upgrade");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains(
          "Cannot start an HA namenode with name dirs that need recovery.",
          ioe);
      LOG.info("Got expected exception", ioe);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
