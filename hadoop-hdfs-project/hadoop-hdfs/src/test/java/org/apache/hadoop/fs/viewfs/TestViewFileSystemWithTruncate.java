/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.viewfs;

import java.io.IOException;

import com.google.common.base.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Verify truncate through ViewFileSystem functionality.
 *
 */
public class TestViewFileSystemWithTruncate {
  private static MiniDFSCluster cluster;
  private static Configuration clusterConf = new Configuration();
  private static FileSystem fHdfs;
  private FileSystem fsView;
  private Configuration fsViewConf;
  private FileSystem fsTarget;
  private Path targetTestRoot, mountOnNn1;
  private FileSystemTestHelper fileSystemTestHelper =
      new FileSystemTestHelper("/tmp/TestViewFileSystemWithXAttrs");

  @BeforeClass
  public static void clusterSetupAtBeginning() throws IOException {
    cluster = new MiniDFSCluster.Builder(clusterConf)
        .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(2))
        .numDataNodes(2).build();
    cluster.waitClusterUp();

    fHdfs = cluster.getFileSystem(0);
  }

  @AfterClass
  public static void clusterShutdownAtEnd() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Before
  public void setUp() throws Exception {
    fsTarget = fHdfs;
    targetTestRoot = fileSystemTestHelper.getAbsoluteTestRootPath(fsTarget);

    fsTarget.delete(targetTestRoot, true);
    fsTarget.mkdirs(targetTestRoot);

    fsViewConf = ViewFileSystemTestSetup.createConfig();
    setupMountPoints();
    fsView = FileSystem.get(FsConstants.VIEWFS_URI, fsViewConf);
  }

  private void setupMountPoints() {
    mountOnNn1 = new Path("/mountOnNn1");
    ConfigUtil
        .addLink(fsViewConf, mountOnNn1.toString(), targetTestRoot.toUri());
  }

  @After
  public void tearDown() throws Exception {
    fsTarget.delete(fileSystemTestHelper.getTestRootPath(fsTarget), true);
  }

  @Test(timeout = 30000)
  public void testTruncateWithViewFileSystem()
      throws Exception {
    Path filePath = new Path(mountOnNn1 + "/ttest");
    final Path hdfFilepath = new Path(
        "/tmp/TestViewFileSystemWithXAttrs/ttest");
    FSDataOutputStream out = fsView.create(filePath);
    out.writeBytes("drtatedasfdasfgdfas");
    out.close();
    int newLength = 10;
    boolean isReady = fsView.truncate(filePath, newLength);
    if (!isReady) {
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          try {
            return cluster.getFileSystem(0).isFileClosed(hdfFilepath);
          } catch (IOException e) {
            return false;
          }
        }
      }, 100, 60 * 1000);
    }
    // file length should be 10 after truncate
    assertEquals(newLength, fsView.getFileStatus(filePath).getLen());
  }

}
