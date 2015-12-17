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
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileContextTestHelper;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
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
 * Verify XAttrs through ViewFs functionality.
 */
public class TestViewFsWithXAttrs {

  private static MiniDFSCluster cluster;
  private static Configuration clusterConf = new Configuration();
  private static FileContext fc, fc2;
  private FileContext fcView, fcTarget, fcTarget2;
  private Configuration fsViewConf;
  private Path targetTestRoot, targetTestRoot2, mountOnNn1, mountOnNn2;
  private FileContextTestHelper fileContextTestHelper =
      new FileContextTestHelper("/tmp/TestViewFsWithXAttrs");

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

    fc = FileContext.getFileContext(cluster.getURI(0), clusterConf);
    fc2 = FileContext.getFileContext(cluster.getURI(1), clusterConf);
  }

  @AfterClass
  public static void ClusterShutdownAtEnd() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Before
  public void setUp() throws Exception {
    fcTarget = fc;
    fcTarget2 = fc2;
    targetTestRoot = fileContextTestHelper.getAbsoluteTestRootPath(fc);
    targetTestRoot2 = fileContextTestHelper.getAbsoluteTestRootPath(fc2);

    fcTarget.delete(targetTestRoot, true);
    fcTarget2.delete(targetTestRoot2, true);
    fcTarget.mkdir(targetTestRoot, new FsPermission((short) 0750), true);
    fcTarget2.mkdir(targetTestRoot2, new FsPermission((short) 0750), true);

    fsViewConf = ViewFileSystemTestSetup.createConfig();
    setupMountPoints();
    fcView = FileContext.getFileContext(FsConstants.VIEWFS_URI, fsViewConf);
  }

  private void setupMountPoints() {
    mountOnNn1 = new Path("/mountOnNn1");
    mountOnNn2 = new Path("/mountOnNn2");
    ConfigUtil.addLink(fsViewConf, mountOnNn1.toString(), targetTestRoot.toUri());
    ConfigUtil.addLink(fsViewConf, mountOnNn2.toString(), targetTestRoot2.toUri());
  }

  @After
  public void tearDown() throws Exception {
    fcTarget.delete(fileContextTestHelper.getTestRootPath(fcTarget), true);
    fcTarget2.delete(fileContextTestHelper.getTestRootPath(fcTarget2), true);
  }

  /**
   * Verify a ViewFs wrapped over multiple federated NameNodes will
   * dispatch the XAttr operations to the correct NameNode.
   */
  @Test
  public void testXAttrOnMountEntry() throws Exception {
    // Set XAttrs on the first namespace and verify they are correct
    fcView.setXAttr(mountOnNn1, name1, value1);
    fcView.setXAttr(mountOnNn1, name2, value2);
    assertEquals(2, fcView.getXAttrs(mountOnNn1).size());
    assertArrayEquals(value1, fcView.getXAttr(mountOnNn1, name1));
    assertArrayEquals(value2, fcView.getXAttr(mountOnNn1, name2));
    // Double-check by getting the XAttrs using FileSystem
    // instead of ViewFs
    assertArrayEquals(value1, fc.getXAttr(targetTestRoot, name1));
    assertArrayEquals(value2, fc.getXAttr(targetTestRoot, name2));

    // Paranoid check: verify the other namespace does not
    // have XAttrs set on the same path.
    assertEquals(0, fcView.getXAttrs(mountOnNn2).size());
    assertEquals(0, fc2.getXAttrs(targetTestRoot2).size());

    // Remove the XAttr entries on the first namespace
    fcView.removeXAttr(mountOnNn1, name1);
    fcView.removeXAttr(mountOnNn1, name2);
    assertEquals(0, fcView.getXAttrs(mountOnNn1).size());
    assertEquals(0, fc.getXAttrs(targetTestRoot).size());

    // Now set XAttrs on the second namespace
    fcView.setXAttr(mountOnNn2, name1, value1);
    fcView.setXAttr(mountOnNn2, name2, value2);
    assertEquals(2, fcView.getXAttrs(mountOnNn2).size());
    assertArrayEquals(value1, fcView.getXAttr(mountOnNn2, name1));
    assertArrayEquals(value2, fcView.getXAttr(mountOnNn2, name2));
    assertArrayEquals(value1, fc2.getXAttr(targetTestRoot2, name1));
    assertArrayEquals(value2, fc2.getXAttr(targetTestRoot2, name2));

    fcView.removeXAttr(mountOnNn2, name1);
    fcView.removeXAttr(mountOnNn2, name2);
    assertEquals(0, fcView.getXAttrs(mountOnNn2).size());
    assertEquals(0, fc2.getXAttrs(targetTestRoot2).size());
  }
}
