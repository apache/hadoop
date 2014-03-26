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
package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.io.MD5Hash;
import org.junit.Test;

public class TestFetchImage {
  
  private static final File FETCHED_IMAGE_FILE = new File(
      System.getProperty("build.test.dir"), "fetched-image-dir");
  // Shamelessly stolen from NNStorage.
  private static final Pattern IMAGE_REGEX = Pattern.compile("fsimage_(\\d+)");

  /**
   * Download a few fsimages using `hdfs dfsadmin -fetchImage ...' and verify
   * the results.
   */
  @Test
  public void testFetchImage() throws Exception {
    FETCHED_IMAGE_FILE.mkdirs();
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    FileSystem fs = null;
    try {
      DFSAdmin dfsAdmin = new DFSAdmin();
      dfsAdmin.setConf(conf);
      
      runFetchImage(dfsAdmin, cluster);
      
      fs = cluster.getFileSystem();
      fs.mkdirs(new Path("/foo"));
      fs.mkdirs(new Path("/foo2"));
      fs.mkdirs(new Path("/foo3"));
      
      cluster.getNameNodeRpc()
          .setSafeMode(SafeModeAction.SAFEMODE_ENTER, false);
      cluster.getNameNodeRpc().saveNamespace();
      cluster.getNameNodeRpc()
          .setSafeMode(SafeModeAction.SAFEMODE_LEAVE, false);
      
      runFetchImage(dfsAdmin, cluster);
    } finally {
      if (fs != null) {
        fs.close();
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Run `hdfs dfsadmin -fetchImage ...' and verify that the downloaded image is
   * correct.
   */
  private static void runFetchImage(DFSAdmin dfsAdmin, MiniDFSCluster cluster)
      throws Exception {
    int retVal = dfsAdmin.run(new String[]{"-fetchImage",
        FETCHED_IMAGE_FILE.getPath() });
    
    assertEquals(0, retVal);
    
    File highestImageOnNn = getHighestFsImageOnCluster(cluster);
    MD5Hash expected = MD5FileUtils.computeMd5ForFile(highestImageOnNn);
    MD5Hash actual = MD5FileUtils.computeMd5ForFile(
        new File(FETCHED_IMAGE_FILE, highestImageOnNn.getName()));
    
    assertEquals(expected, actual);
  }
  
  /**
   * @return the fsimage with highest transaction ID in the cluster.
   */
  private static File getHighestFsImageOnCluster(MiniDFSCluster cluster) {
    long highestImageTxId = -1;
    File highestImageOnNn = null;
    for (URI nameDir : cluster.getNameDirs(0)) {
      for (File imageFile : new File(new File(nameDir), "current").listFiles()) {
        Matcher imageMatch = IMAGE_REGEX.matcher(imageFile.getName());
        if (imageMatch.matches()) {
          long imageTxId = Long.parseLong(imageMatch.group(1));
          if (imageTxId > highestImageTxId) {
            highestImageTxId = imageTxId;
            highestImageOnNn = imageFile;
          }
        }
      }
    }
    return highestImageOnNn;
  }
}
