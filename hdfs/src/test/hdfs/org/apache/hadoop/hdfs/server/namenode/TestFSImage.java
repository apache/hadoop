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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFSImage {

  private static final String OUT_DIR = System.getProperty("test.build.data",
      "build/test/fsimage");

  private MiniDFSCluster miniDFSCluster = null;

  private static Configuration nnConf = new Configuration();

  private File current = new File(OUT_DIR);

  @Before
  public void setUpCluster() throws Exception {
    clearDirs();
  }

  @After
  public void clusterShutdown() throws Exception {
    if (null != miniDFSCluster) {
      miniDFSCluster.shutdown();
    }
  }

  @Test
  public void testLoadFsEditsShouldReturnTrueWhenEditsNewExists()
      throws Exception {
    nnConf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, OUT_DIR + "/BNN1");
    NameNode.format(nnConf);
    miniDFSCluster = new MiniDFSCluster.Builder(nnConf).numDataNodes(1).build();
    FSImage image = miniDFSCluster.getNameNode().getFSImage();
    URI next = FSNamesystem
        .getNamespaceDirs(miniDFSCluster.getConfiguration(0)).iterator().next();
    File editsNew = new File(next.getRawPath() , "/current/edits.new");
    createEditsNew(editsNew, image);
    int loadFSEdits = image.loadFSEdits(image.getStorage().getStorageDir(0));
    assertEquals("The numEdits should not be zero.", 1, loadFSEdits);
  }

  private void createEditsNew(File editsNew, FSImage image) throws Exception {
    FileOutputStream fileOutputStream = null;
    if (!editsNew.exists()) {
      try {
        editsNew.createNewFile();
        image.editLog.createEditLogFile(editsNew);
      } finally {
        IOUtils.closeStream(fileOutputStream);
      }
    }
  }

  private void clearDirs() throws IOException {
    if (current.exists()) {
      FileUtil.fullyDelete(current);
    }
  }
}
