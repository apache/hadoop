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

import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileUtil;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import java.util.Properties;
import java.io.FileReader;
import java.io.FileWriter;
import org.junit.Assert;
import org.apache.hadoop.test.GenericTestUtils;

/**
 * Regression test for HDFS-3597, SecondaryNameNode upgrade -- when a 2NN
 * starts up with an existing directory structure with an old VERSION file, it
 * should delete the snapshot and download a new one from the NN.
 */
public class TestSecondaryNameNodeUpgrade {

  @Before
  public void cleanupCluster() throws IOException {
    File hdfsDir = new File(MiniDFSCluster.getBaseDirectory()).getCanonicalFile();
    System.out.println("cleanupCluster deleting " + hdfsDir);
    if (hdfsDir.exists() && !FileUtil.fullyDelete(hdfsDir)) {
      throw new IOException("Could not delete hdfs directory '" + hdfsDir + "'");
    }
  }

  private void doIt(Map<String, String> paramsToCorrupt) throws IOException {
    MiniDFSCluster cluster = null;
    FileSystem fs = null;
    SecondaryNameNode snn = null;

    try {
      Configuration conf = new HdfsConfiguration();

      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();

      conf.set(DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY, "0.0.0.0:0");
      snn = new SecondaryNameNode(conf);

      fs = cluster.getFileSystem();

      fs.mkdirs(new Path("/test/foo"));

      snn.doCheckpoint();

      List<File> versionFiles = snn.getFSImage().getStorage().getFiles(null, "VERSION");

      snn.shutdown();

      for (File versionFile : versionFiles) {
        for (Map.Entry<String, String> paramToCorrupt : paramsToCorrupt.entrySet()) {
          String param = paramToCorrupt.getKey();
          String val = paramToCorrupt.getValue();
          System.out.println("Changing '" + param + "' to '" + val + "' in " + versionFile);
          FSImageTestUtil.corruptVersionFile(versionFile, param, val);
        }
      }

      snn = new SecondaryNameNode(conf);

      fs.mkdirs(new Path("/test/bar"));

      snn.doCheckpoint();
    } finally {
      if (fs != null) fs.close();
      if (cluster != null) cluster.shutdown();
      if (snn != null) snn.shutdown();
    }
  }

  @Test
  public void testUpgradeLayoutVersionSucceeds() throws IOException {
    doIt(ImmutableMap.of("layoutVersion", "-39"));
  }

  @Test
  public void testUpgradePreFedSucceeds() throws IOException {
    doIt(ImmutableMap.of("layoutVersion", "-19", "clusterID", "",
          "blockpoolID", ""));
  }

  @Test
  public void testChangeNsIDFails() throws IOException {
    try {
      doIt(ImmutableMap.of("namespaceID", "2"));
      Assert.fail("Should throw InconsistentFSStateException");
    } catch(IOException e) {
      GenericTestUtils.assertExceptionContains("Inconsistent checkpoint fields", e);
      System.out.println("Correctly failed with inconsistent namespaceID: " + e);
    }
  }
}
