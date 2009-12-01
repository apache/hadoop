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

import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NamenodeRole;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestNNLeaseRecovery {
  public static final Log LOG = LogFactory.getLog(TestNNLeaseRecovery.class);
  FSNamesystem fsn;
  Configuration conf;
  
  static {
    ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LOG).getLogger().setLevel(Level.ALL);
  }

  /**
   * Initiates and sets a spied on FSNamesystem so tests can hook its methods
   * @throws IOException if an error occurred
   */
  @Before
  public void startUp() throws IOException {
    conf = new HdfsConfiguration();
    // avoid stubbing access control
    conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, false); 
    NameNode.initMetrics(conf, NamenodeRole.ACTIVE);

    FileSystem.setDefaultUri(conf, "hdfs://localhost:0");
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    NameNode.format(conf);
    fsn = spy(new FSNamesystem(conf));
  }

  /**
   * Cleans the resources and closes the instance of FSNamesystem
   * @throws IOException if an error occurred
   */
  @After
  public void tearDown() throws IOException {
    if (fsn != null) {
      try {
        if (fsn.getFSImage() != null) fsn.getFSImage().close();
        fsn.close();
      } finally {
        File dir = new File(conf.get("hadoop.tmp.dir"));
        if (dir != null) deleteDir(dir);
      }
    }
  }

  /**
   * Mocks FSNamesystem instance, adds an empty file and invokes lease recovery
   * method. 
   * @throws IOException in case of an error
   */
  @Test
  public void testInternalReleaseLease_allCOMPLETE () throws IOException {
    LOG.debug("Running " + GenericTestUtils.getMethodName());    
    LeaseManager.Lease lm = mock(LeaseManager.Lease.class);
    Path file = spy(new Path("/test.dat"));
    DatanodeDescriptor dnd = mock(DatanodeDescriptor.class);
    PermissionStatus ps =
      new PermissionStatus("test", "test", new FsPermission((short)0777));
    
    fsn.dir.addFile(file.toString(), ps, (short)3, 1l, 
      "test", "test-machine", dnd, 1001l);
    assertTrue("True has to be returned in this case",
      fsn.internalReleaseLease(lm, file.toString(), null));
  }
  
  /**
   * Mocks FSNamesystem instance, adds an empty file, sets status of last two
   * blocks to non-defined and UNDER_CONSTRUCTION and invokes lease recovery
   * method. IOException is expected for releasing a create lock on a 
   * closed file. 
   * @throws IOException as the result
   */
  @Test(expected=IOException.class)
  public void testInternalReleaseLease_UNKNOWN_COMM () throws IOException {
    LOG.debug("Running " + GenericTestUtils.getMethodName());
    LeaseManager.Lease lm = mock(LeaseManager.Lease.class);
    Path file = 
      spy(new Path("/" + GenericTestUtils.getMethodName() + "_test.dat"));    
    DatanodeDescriptor dnd = mock(DatanodeDescriptor.class);
    PermissionStatus ps =
      new PermissionStatus("test", "test", new FsPermission((short)0777));
    
    mockFileBlocks(null, 
      HdfsConstants.BlockUCState.UNDER_CONSTRUCTION, file, dnd, ps);
    
    fsn.internalReleaseLease(lm, file.toString(), null);
    assertTrue("FSNamesystem.internalReleaseLease suppose to throw " +
      "IOException here", false);
  }  

  /**
   * Mocks FSNamesystem instance, adds an empty file, sets status of last two
   * blocks to COMMITTED and COMMITTED and invokes lease recovery
   * method. AlreadyBeingCreatedException is expected.
   * @throws AlreadyBeingCreatedException as the result
   */
  @Test(expected=AlreadyBeingCreatedException.class)
  public void testInternalReleaseLease_COMM_COMM () throws IOException {
    LOG.debug("Running " + GenericTestUtils.getMethodName());
    LeaseManager.Lease lm = mock(LeaseManager.Lease.class);
    Path file = 
      spy(new Path("/" + GenericTestUtils.getMethodName() + "_test.dat"));
    DatanodeDescriptor dnd = mock(DatanodeDescriptor.class);
    PermissionStatus ps =
      new PermissionStatus("test", "test", new FsPermission((short)0777));

    mockFileBlocks(HdfsConstants.BlockUCState.COMMITTED, 
      HdfsConstants.BlockUCState.COMMITTED, file, dnd, ps);

    fsn.internalReleaseLease(lm, file.toString(), null);
    assertTrue("FSNamesystem.internalReleaseLease suppose to throw " +
      "AlreadyBeingCreatedException here", false);
  }

  /**
   * Mocks FSNamesystem instance, adds an empty file, sets status of last two
   * blocks to COMMITTED and UNDER_CONSTRUCTION and invokes lease recovery
   * method. <code>false</code> is expected as the result
   * @throws IOException in case of an error
   */
  @Test
  public void testInternalReleaseLease_COMM_CONSTRUCTION () throws IOException {
    LOG.debug("Running " + GenericTestUtils.getMethodName());
    LeaseManager.Lease lm = mock(LeaseManager.Lease.class);
    Path file = 
      spy(new Path("/" + GenericTestUtils.getMethodName() + "_test.dat"));
    DatanodeDescriptor dnd = mock(DatanodeDescriptor.class);
    PermissionStatus ps =
      new PermissionStatus("test", "test", new FsPermission((short)0777));
    
    mockFileBlocks(HdfsConstants.BlockUCState.COMMITTED, 
      HdfsConstants.BlockUCState.UNDER_CONSTRUCTION, file, dnd, ps);
        
    assertFalse("False is expected in return in this case",
      fsn.internalReleaseLease(lm, file.toString(), null));
  }

  private void mockFileBlocks(HdfsConstants.BlockUCState penUltState,
                           HdfsConstants.BlockUCState lastState,
                           Path file, DatanodeDescriptor dnd, 
                           PermissionStatus ps) throws IOException {
    BlockInfo b = mock(BlockInfo.class);
    BlockInfoUnderConstruction b1 = mock(BlockInfoUnderConstruction.class);
    when(b.getBlockUCState()).thenReturn(penUltState);
    when(b1.getBlockUCState()).thenReturn(lastState);
    BlockInfo[] blocks = new BlockInfo[]{b, b1};

    FSDirectory fsDir = mock(FSDirectory.class);
    INodeFileUnderConstruction iNFmock = mock(INodeFileUnderConstruction.class);
    fsn.dir = fsDir;
    FSImage fsImage = mock(FSImage.class);
    FSEditLog editLog = mock(FSEditLog.class);
                            
    when(fsn.getFSImage()).thenReturn(fsImage);
    when(fsn.getFSImage().getEditLog()).thenReturn(editLog);
    fsn.getFSImage().setFSNamesystem(fsn);
    
    when(iNFmock.getBlocks()).thenReturn(blocks);
    when(iNFmock.numBlocks()).thenReturn(2);
    when(iNFmock.getPenultimateBlock()).thenReturn(b);
    when(iNFmock.getLastBlock()).thenReturn(b1);
    when(iNFmock.isUnderConstruction()).thenReturn(true);
    fsDir.addFile(file.toString(), ps, (short)3, 1l, "test", 
      "test-machine", dnd, 1001l);

    when(fsDir.getFileINode(anyString())).thenReturn(iNFmock);
  }

  private static boolean deleteDir(File dir) {
    if (dir == null) return false;
    
    if (dir.isDirectory()) {
      String[] children = dir.list();
      for (String aChildren : children) {
        boolean success = deleteDir(new File(dir, aChildren));
        if (!success) {
          return false;
        }
      }
    }

    // The directory is now empty so delete it
    return dir.delete();
  }
}
