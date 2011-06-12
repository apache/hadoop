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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NamenodeRole;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.After;
import static org.junit.Assert.assertFalse;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.IOException;

public class TestNNLeaseRecovery {
  private static final Log LOG = LogFactory.getLog(TestNNLeaseRecovery.class);
  private static final String NAME_DIR =
    MiniDFSCluster.getBaseDirectory() + "name";

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
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, NAME_DIR);
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, NAME_DIR);
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
        fsn.close();
      } catch(Exception e) {
        LOG.error("Cannot close: ", e);
      } finally {
        File dir = new File(NAME_DIR);
        if (dir != null)
          assertTrue("Cannot delete name-node dirs", FileUtil.fullyDelete(dir));
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
    
    mockFileBlocks(2, null, 
      HdfsConstants.BlockUCState.UNDER_CONSTRUCTION, file, dnd, ps, false);
    
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

    mockFileBlocks(2, HdfsConstants.BlockUCState.COMMITTED, 
      HdfsConstants.BlockUCState.COMMITTED, file, dnd, ps, false);

    fsn.internalReleaseLease(lm, file.toString(), null);
    assertTrue("FSNamesystem.internalReleaseLease suppose to throw " +
      "AlreadyBeingCreatedException here", false);
  }

  /**
   * Mocks FSNamesystem instance, adds an empty file with 0 blocks
   * and invokes lease recovery method. 
   * 
   */
  @Test
  public void testInternalReleaseLease_0blocks () throws IOException {
    LOG.debug("Running " + GenericTestUtils.getMethodName());
    LeaseManager.Lease lm = mock(LeaseManager.Lease.class);
    Path file = 
      spy(new Path("/" + GenericTestUtils.getMethodName() + "_test.dat"));
    DatanodeDescriptor dnd = mock(DatanodeDescriptor.class);
    PermissionStatus ps =
      new PermissionStatus("test", "test", new FsPermission((short)0777));

    mockFileBlocks(0, null, null, file, dnd, ps, false);

    assertTrue("True has to be returned in this case",
      fsn.internalReleaseLease(lm, file.toString(), null));
  }
  
  /**
   * Mocks FSNamesystem instance, adds an empty file with 1 block
   * and invokes lease recovery method. 
   * AlreadyBeingCreatedException is expected.
   * @throws AlreadyBeingCreatedException as the result
   */
  @Test(expected=AlreadyBeingCreatedException.class)
  public void testInternalReleaseLease_1blocks () throws IOException {
    LOG.debug("Running " + GenericTestUtils.getMethodName());
    LeaseManager.Lease lm = mock(LeaseManager.Lease.class);
    Path file = 
      spy(new Path("/" + GenericTestUtils.getMethodName() + "_test.dat"));
    DatanodeDescriptor dnd = mock(DatanodeDescriptor.class);
    PermissionStatus ps =
      new PermissionStatus("test", "test", new FsPermission((short)0777));

    mockFileBlocks(1, null, HdfsConstants.BlockUCState.COMMITTED, file, dnd, ps, false);

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
    
    mockFileBlocks(2, HdfsConstants.BlockUCState.COMMITTED, 
      HdfsConstants.BlockUCState.UNDER_CONSTRUCTION, file, dnd, ps, false);
        
    assertFalse("False is expected in return in this case",
      fsn.internalReleaseLease(lm, file.toString(), null));
  }

  @Test
  public void testCommitBlockSynchronization_BlockNotFound () 
    throws IOException {
    LOG.debug("Running " + GenericTestUtils.getMethodName());
    long recoveryId = 2002;
    long newSize = 273487234;
    Path file = 
      spy(new Path("/" + GenericTestUtils.getMethodName() + "_test.dat"));
    DatanodeDescriptor dnd = mock(DatanodeDescriptor.class);
    PermissionStatus ps =
      new PermissionStatus("test", "test", new FsPermission((short)0777));
    
    mockFileBlocks(2, HdfsConstants.BlockUCState.COMMITTED, 
      HdfsConstants.BlockUCState.UNDER_CONSTRUCTION, file, dnd, ps, false);
    
    BlockInfo lastBlock = fsn.dir.getFileINode(anyString()).getLastBlock(); 
    try {
      fsn.commitBlockSynchronization(lastBlock,
        recoveryId, newSize, true, false, new DatanodeID[1]);
    } catch (IOException ioe) {
      assertTrue(ioe.getMessage().startsWith("Block (="));
    }
  }
  
  @Test
  public void testCommitBlockSynchronization_notUR () 
    throws IOException {
    LOG.debug("Running " + GenericTestUtils.getMethodName());
    long recoveryId = 2002;
    long newSize = 273487234;
    Path file = 
      spy(new Path("/" + GenericTestUtils.getMethodName() + "_test.dat"));
    DatanodeDescriptor dnd = mock(DatanodeDescriptor.class);
    PermissionStatus ps =
      new PermissionStatus("test", "test", new FsPermission((short)0777));
    
    mockFileBlocks(2, HdfsConstants.BlockUCState.COMMITTED, 
      HdfsConstants.BlockUCState.COMPLETE, file, dnd, ps, true);
    
    BlockInfo lastBlock = fsn.dir.getFileINode(anyString()).getLastBlock();
    when(lastBlock.isComplete()).thenReturn(true);
    
    try {
      fsn.commitBlockSynchronization(lastBlock,
        recoveryId, newSize, true, false, new DatanodeID[1]);
    } catch (IOException ioe) {
      assertTrue(ioe.getMessage().startsWith("Unexpected block (="));
    }
  }
  
  @Test
  public void testCommitBlockSynchronization_WrongGreaterRecoveryID() 
    throws IOException {
    LOG.debug("Running " + GenericTestUtils.getMethodName());
    long recoveryId = 2002;
    long newSize = 273487234;
    Path file = 
      spy(new Path("/" + GenericTestUtils.getMethodName() + "_test.dat"));
    DatanodeDescriptor dnd = mock(DatanodeDescriptor.class);
    PermissionStatus ps =
      new PermissionStatus("test", "test", new FsPermission((short)0777));
    
    mockFileBlocks(2, HdfsConstants.BlockUCState.COMMITTED, 
      HdfsConstants.BlockUCState.UNDER_CONSTRUCTION, file, dnd, ps, true);
    
    BlockInfo lastBlock = fsn.dir.getFileINode(anyString()).getLastBlock();
    when(((BlockInfoUnderConstruction)lastBlock).getBlockRecoveryId()).thenReturn(recoveryId-100);
    
    try {
      fsn.commitBlockSynchronization(lastBlock,
        recoveryId, newSize, true, false, new DatanodeID[1]);
    } catch (IOException ioe) {
      assertTrue(ioe.getMessage().startsWith("The recovery id " + recoveryId + " does not match current recovery id " + (recoveryId-100)));
    }
  }  
  
  @Test
  public void testCommitBlockSynchronization_WrongLesserRecoveryID() 
    throws IOException {
    LOG.debug("Running " + GenericTestUtils.getMethodName());
    long recoveryId = 2002;
    long newSize = 273487234;
    Path file = 
      spy(new Path("/" + GenericTestUtils.getMethodName() + "_test.dat"));
    DatanodeDescriptor dnd = mock(DatanodeDescriptor.class);
    PermissionStatus ps =
      new PermissionStatus("test", "test", new FsPermission((short)0777));
    
    mockFileBlocks(2, HdfsConstants.BlockUCState.COMMITTED, 
      HdfsConstants.BlockUCState.UNDER_CONSTRUCTION, file, dnd, ps, true);
    
    BlockInfo lastBlock = fsn.dir.getFileINode(anyString()).getLastBlock();
    when(((BlockInfoUnderConstruction)lastBlock).getBlockRecoveryId()).thenReturn(recoveryId+100);
    
    try {           
      fsn.commitBlockSynchronization(lastBlock,
        recoveryId, newSize, true, false, new DatanodeID[1]);
    } catch (IOException ioe) {
      assertTrue(ioe.getMessage().startsWith("The recovery id " + recoveryId + " does not match current recovery id " + (recoveryId+100)));
    }
  }

  @Test
  public void testCommitBlockSynchronization_EqualRecoveryID() 
    throws IOException {
    LOG.debug("Running " + GenericTestUtils.getMethodName());
    long recoveryId = 2002;
    long newSize = 273487234;
    Path file = 
      spy(new Path("/" + GenericTestUtils.getMethodName() + "_test.dat"));
    DatanodeDescriptor dnd = mock(DatanodeDescriptor.class);
    PermissionStatus ps =
      new PermissionStatus("test", "test", new FsPermission((short)0777));
    
    mockFileBlocks(2, HdfsConstants.BlockUCState.COMMITTED, 
      HdfsConstants.BlockUCState.UNDER_CONSTRUCTION, file, dnd, ps, true);
    
    BlockInfo lastBlock = fsn.dir.getFileINode(anyString()).getLastBlock();
    when(((BlockInfoUnderConstruction)lastBlock).getBlockRecoveryId()).thenReturn(recoveryId);
    
    boolean recoveryChecked = false;
    try {
      fsn.commitBlockSynchronization(lastBlock,
        recoveryId, newSize, true, false, new DatanodeID[1]);
    } catch (NullPointerException ioe) {
      // It is fine to get NPE here because the datanodes array is empty
      recoveryChecked = true;
    }
    assertTrue("commitBlockSynchronization had to throw NPE here", recoveryChecked);
  }

  private void mockFileBlocks(int fileBlocksNumber,
                              HdfsConstants.BlockUCState penUltState,
                              HdfsConstants.BlockUCState lastState,
                              Path file, DatanodeDescriptor dnd,
                              PermissionStatus ps,
                              boolean setStoredBlock) throws IOException {
    BlockInfo b = mock(BlockInfo.class);
    BlockInfoUnderConstruction b1 = mock(BlockInfoUnderConstruction.class);
    when(b.getBlockUCState()).thenReturn(penUltState);
    when(b1.getBlockUCState()).thenReturn(lastState);
    BlockInfo[] blocks;

    FSDirectory fsDir = mock(FSDirectory.class);
    INodeFileUnderConstruction iNFmock = mock(INodeFileUnderConstruction.class);

    fsn.dir.close();
    fsn.dir = fsDir;
    FSImage fsImage = mock(FSImage.class);
    FSEditLog editLog = mock(FSEditLog.class);
                            
    when(fsn.getFSImage()).thenReturn(fsImage);
    when(fsn.getFSImage().getEditLog()).thenReturn(editLog);
    fsn.getFSImage().setFSNamesystem(fsn);
    
    switch (fileBlocksNumber) {
      case 0:
        blocks = new BlockInfo[0];
        break;
      case 1:
        blocks = new BlockInfo[]{b1};
        when(iNFmock.getLastBlock()).thenReturn(b1);
        break;
      default:
        when(iNFmock.getPenultimateBlock()).thenReturn(b);
        when(iNFmock.getLastBlock()).thenReturn(b1);
        blocks = new BlockInfo[]{b, b1};
    }
    
    when(iNFmock.getBlocks()).thenReturn(blocks);
    when(iNFmock.numBlocks()).thenReturn(blocks.length);
    when(iNFmock.isUnderConstruction()).thenReturn(true);
    when(iNFmock.convertToInodeFile()).thenReturn(iNFmock);    
    fsDir.addFile(file.toString(), ps, (short)3, 1l, "test", 
      "test-machine", dnd, 1001l);

    fsn.leaseManager = mock(LeaseManager.class);
    fsn.leaseManager.addLease("mock-lease", file.toString());
    if (setStoredBlock) {
      when(b1.getINode()).thenReturn(iNFmock);
      fsn.blockManager.blocksMap.addINode(b1, iNFmock);
    }

    when(fsDir.getFileINode(anyString())).thenReturn(iNFmock);
  }
}
