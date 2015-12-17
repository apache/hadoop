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
package org.apache.hadoop.fs.viewfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Verify XAttrs through ViewFileSystem functionality.
 */
public class TestViewFileSystemWithXAttrs {

  private static MiniDFSCluster cluster;
  private static Configuration clusterConf = new Configuration();
  private static FileSystem fHdfs;
  private static FileSystem fHdfs2;
  private FileSystem fsView;
  private Configuration fsViewConf;
  private FileSystem fsTarget, fsTarget2;
  private Path targetTestRoot, targetTestRoot2, mountOnNn1, mountOnNn2;
  private FileSystemTestHelper fileSystemTestHelper =
      new FileSystemTestHelper("/tmp/TestViewFileSystemWithXAttrs");

  // XAttrs
  protected static final String name1 = "user.a1";
  protected static final byte[] value1 = {0x31, 0x32, 0x33};
  protected static final String name2 = "user.a2";
  protected static final byte[] value2 = {0x37, 0x38, 0x39};

  @BeforeClass
  public static void clusterSetupAtBeginning() throws IOException {
    cluster = new MiniDFSCluster.Builder(clusterConf)
        .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(2))
        .numDataNodes(2)
        .build();
    cluster.waitClusterUp();

    fHdfs = cluster.getFileSystem(0);
    fHdfs2 = cluster.getFileSystem(1);
  }

  @AfterClass
  public static void ClusterShutdownAtEnd() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Before
  public void setUp() throws Exception {
    fsTarget = fHdfs;
    fsTarget2 = fHdfs2;
    targetTestRoot = fileSystemTestHelper.getAbsoluteTestRootPath(fsTarget);
    targetTestRoot2 = fileSystemTestHelper.getAbsoluteTestRootPath(fsTarget2);

    fsTarget.delete(targetTestRoot, true);
    fsTarget2.delete(targetTestRoot2, true);
    fsTarget.mkdirs(targetTestRoot);
    fsTarget2.mkdirs(targetTestRoot2);

    fsViewConf = ViewFileSystemTestSetup.createConfig();
    setupMountPoints();
    fsView = FileSystem.get(FsConstants.VIEWFS_URI, fsViewConf);
  }

  private void setupMountPoints() {
    mountOnNn1 = new Path("/mountOnNn1");
    mountOnNn2 = new Path("/mountOnNn2");
    ConfigUtil.addLink(fsViewConf, mountOnNn1.toString(),
        targetTestRoot.toUri());
    ConfigUtil.addLink(fsViewConf, mountOnNn2.toString(),
        targetTestRoot2.toUri());
  }

  @After
  public void tearDown() throws Exception {
    fsTarget.delete(fileSystemTestHelper.getTestRootPath(fsTarget), true);
    fsTarget2.delete(fileSystemTestHelper.getTestRootPath(fsTarget2), true);
  }

  /**
   * Verify a ViewFileSystem wrapped over multiple federated NameNodes will
   * dispatch the XAttr operations to the correct NameNode.
   */
  @Test
  public void testXAttrOnMountEntry() throws Exception {
    // Set XAttrs on the first namespace and verify they are correct
    fsView.setXAttr(mountOnNn1, name1, value1);
    fsView.setXAttr(mountOnNn1, name2, value2);
    assertEquals(2, fsView.getXAttrs(mountOnNn1).size());
    assertArrayEquals(value1, fsView.getXAttr(mountOnNn1, name1));
    assertArrayEquals(value2, fsView.getXAttr(mountOnNn1, name2));
    // Double-check by getting the XAttrs using FileSystem
    // instead of ViewFileSystem
    assertArrayEquals(value1, fHdfs.getXAttr(targetTestRoot, name1));
    assertArrayEquals(value2, fHdfs.getXAttr(targetTestRoot, name2));

    // Paranoid check: verify the other namespace does not
    // have XAttrs set on the same path.
    assertEquals(0, fsView.getXAttrs(mountOnNn2).size());
    assertEquals(0, fHdfs2.getXAttrs(targetTestRoot2).size());

    // Remove the XAttr entries on the first namespace
    fsView.removeXAttr(mountOnNn1, name1);
    fsView.removeXAttr(mountOnNn1, name2);
    assertEquals(0, fsView.getXAttrs(mountOnNn1).size());
    assertEquals(0, fHdfs.getXAttrs(targetTestRoot).size());

    // Now set XAttrs on the second namespace
    fsView.setXAttr(mountOnNn2, name1, value1);
    fsView.setXAttr(mountOnNn2, name2, value2);
    assertEquals(2, fsView.getXAttrs(mountOnNn2).size());
    assertArrayEquals(value1, fsView.getXAttr(mountOnNn2, name1));
    assertArrayEquals(value2, fsView.getXAttr(mountOnNn2, name2));
    assertArrayEquals(value1, fHdfs2.getXAttr(targetTestRoot2, name1));
    assertArrayEquals(value2, fHdfs2.getXAttr(targetTestRoot2, name2));

    fsView.removeXAttr(mountOnNn2, name1);
    fsView.removeXAttr(mountOnNn2, name2);
    assertEquals(0, fsView.getXAttrs(mountOnNn2).size());
    assertEquals(0, fHdfs2.getXAttrs(targetTestRoot2).size());
  }
}
