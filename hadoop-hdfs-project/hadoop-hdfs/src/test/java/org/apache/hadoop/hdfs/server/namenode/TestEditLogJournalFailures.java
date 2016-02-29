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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.JournalSet.JournalAndStream;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.util.ExitUtil.ExitException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mockito;

@RunWith(Parameterized.class)
public class TestEditLogJournalFailures {

  private int editsPerformed = 0;
  private MiniDFSCluster cluster;
  private FileSystem fs;
  private boolean useAsyncEdits;

  @Parameters
  public static Collection<Object[]> data() {
    Collection<Object[]> params = new ArrayList<Object[]>();
    params.add(new Object[]{Boolean.FALSE});
    params.add(new Object[]{Boolean.TRUE});
    return params;
  }

  public TestEditLogJournalFailures(boolean useAsyncEdits) {
    this.useAsyncEdits = useAsyncEdits;
  }

  private Configuration getConf() {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_EDITS_ASYNC_LOGGING,
        useAsyncEdits);
    return conf;
  }

  /**
   * Create the mini cluster for testing and sub in a custom runtime so that
   * edit log journal failures don't actually cause the JVM to exit.
   */
  @Before
  public void setUpMiniCluster() throws IOException {
    setUpMiniCluster(getConf(), true);
  }

  public void setUpMiniCluster(Configuration conf, boolean manageNameDfsDirs)
      throws IOException {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0)
        .manageNameDfsDirs(manageNameDfsDirs).checkExitOnShutdown(false).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
  }
  
  @After
  public void shutDownMiniCluster() throws IOException {
    if (fs != null) {
      fs.close();
      fs = null;
    }
    if (cluster != null) {
      try {
        cluster.shutdown();
        cluster = null;
      } catch (ExitException ee) {
        // Ignore ExitExceptions as the tests may result in the
        // NameNode doing an immediate shutdown.
      }
    }
  }
   
  @Test
  public void testSingleFailedEditsDirOnFlush() throws IOException {
    assertTrue(doAnEdit());
    // Invalidate one edits journal.
    invalidateEditsDirAtIndex(0, true, false);
    // The NN has not terminated (no ExitException thrown)
    assertTrue(doAnEdit());
    // A single journal failure should not result in a call to terminate
    assertFalse(cluster.getNameNode().isInSafeMode());
  }
   
  @Test
  public void testAllEditsDirsFailOnFlush() throws IOException {
    assertTrue(doAnEdit());
    // Invalidate both edits journals.
    invalidateEditsDirAtIndex(0, true, false);
    invalidateEditsDirAtIndex(1, true, false);
    // The NN has not terminated (no ExitException thrown)
    try {
      doAnEdit();
      fail("The previous edit could not be synced to any persistent storage, "
          + "should have halted the NN");
    } catch (RemoteException re) {
      assertTrue(re.getClassName().contains("ExitException"));
      GenericTestUtils.assertExceptionContains(
          "Could not sync enough journals to persistent storage. " +
          "Unsynced transactions: 1", re);
    }
  }
  
  @Test
  public void testAllEditsDirFailOnWrite() throws IOException {
    assertTrue(doAnEdit());
    // Invalidate both edits journals.
    invalidateEditsDirAtIndex(0, true, true);
    invalidateEditsDirAtIndex(1, true, true);
    // The NN has not terminated (no ExitException thrown)
    try {
      doAnEdit();
      fail("The previous edit could not be synced to any persistent storage, "
          + " should have halted the NN");
    } catch (RemoteException re) {
      assertTrue(re.getClassName().contains("ExitException"));
      GenericTestUtils.assertExceptionContains(
          "Could not sync enough journals to persistent storage due to " +
          "No journals available to flush. " +
          "Unsynced transactions: 1", re);
    }
  }
  
  @Test
  public void testSingleFailedEditsDirOnSetReadyToFlush() throws IOException {
    assertTrue(doAnEdit());
    // Invalidate one edits journal.
    invalidateEditsDirAtIndex(0, false, false);
    // The NN has not terminated (no ExitException thrown)
    assertTrue(doAnEdit());
    // A single journal failure should not result in a call to terminate
    assertFalse(cluster.getNameNode().isInSafeMode());
  }
  
  @Test
  public void testSingleRequiredFailedEditsDirOnSetReadyToFlush()
      throws IOException {
    // Set one of the edits dirs to be required.
    String[] editsDirs = cluster.getConfiguration(0).getTrimmedStrings(
        DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY);
    shutDownMiniCluster();
    Configuration conf = getConf();
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_REQUIRED_KEY, editsDirs[0]);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_MINIMUM_KEY, 0);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_CHECKED_VOLUMES_MINIMUM_KEY, 0);
    setUpMiniCluster(conf, true);
    
    assertTrue(doAnEdit());
    // Invalidated the one required edits journal.
    invalidateEditsDirAtIndex(0, false, false);
    JournalAndStream nonRequiredJas = getJournalAndStream(1);
    EditLogFileOutputStream nonRequiredSpy =
      spyOnStream(nonRequiredJas);
    
    // The NN has not terminated (no ExitException thrown)
    
    // ..and that the other stream is active.
    assertTrue(nonRequiredJas.isActive());
    
    try {
      doAnEdit();
      fail("A single failure of a required journal should have halted the NN");
    } catch (RemoteException re) {
      assertTrue(re.getClassName().contains("ExitException"));
      GenericTestUtils.assertExceptionContains(
          "setReadyToFlush failed for required journal", re);
    }
    
    // Since the required directory failed setReadyToFlush, and that
    // directory was listed prior to the non-required directory,
    // we should not call setReadyToFlush on the non-required
    // directory. Regression test for HDFS-2874.
    Mockito.verify(nonRequiredSpy, Mockito.never()).setReadyToFlush();
    assertFalse(nonRequiredJas.isActive());
  }
  
  @Test
  public void testMultipleRedundantFailedEditsDirOnSetReadyToFlush()
      throws IOException {
    // Set up 4 name/edits dirs.
    shutDownMiniCluster();
    Configuration conf = getConf();
    String[] nameDirs = new String[4];
    for (int i = 0; i < nameDirs.length; i++) {
      File nameDir = new File(PathUtils.getTestDir(getClass()), "name-dir" + i);
      nameDir.mkdirs();
      nameDirs[i] = nameDir.getAbsolutePath();
    }
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        StringUtils.join(nameDirs, ","));
    
    // Keep running unless there are less than 2 edits dirs remaining.
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_MINIMUM_KEY, 2);
    setUpMiniCluster(conf, false);
    
    // All journals active.
    assertTrue(doAnEdit());
    // The NN has not terminated (no ExitException thrown)
    
    // Invalidate 1/4 of the redundant journals.
    invalidateEditsDirAtIndex(0, false, false);
    assertTrue(doAnEdit());
    // The NN has not terminated (no ExitException thrown)

    // Invalidate 2/4 of the redundant journals.
    invalidateEditsDirAtIndex(1, false, false);
    assertTrue(doAnEdit());
    // The NN has not terminated (no ExitException thrown)
    
    // Invalidate 3/4 of the redundant journals.
    invalidateEditsDirAtIndex(2, false, false);

    try {
      doAnEdit();
      fail("A failure of more than the minimum number of redundant journals "
          + "should have halted ");
    } catch (RemoteException re) {
      assertTrue(re.getClassName().contains("ExitException"));
      GenericTestUtils.assertExceptionContains(
          "Could not sync enough journals to persistent storage due to " +
          "setReadyToFlush failed for too many journals. " +
          "Unsynced transactions: 1", re);
    }
  }

  /**
   * Replace the journal at index <code>index</code> with one that throws an
   * exception on flush.
   * 
   * @param index the index of the journal to take offline.
   * @return the original <code>EditLogOutputStream</code> of the journal.
   */
  private void invalidateEditsDirAtIndex(int index,
      boolean failOnFlush, boolean failOnWrite) throws IOException {
    JournalAndStream jas = getJournalAndStream(index);
    EditLogFileOutputStream spyElos = spyOnStream(jas);
    if (failOnWrite) {
      doThrow(new IOException("fail on write()")).when(spyElos).write(
          (FSEditLogOp) any());
    }
    if (failOnFlush) {
      doThrow(new IOException("fail on flush()")).when(spyElos).flush();
    } else {
      doThrow(new IOException("fail on setReadyToFlush()")).when(spyElos)
        .setReadyToFlush();
    }
  }

  private EditLogFileOutputStream spyOnStream(JournalAndStream jas) {
    EditLogFileOutputStream elos =
      (EditLogFileOutputStream) jas.getCurrentStream();
    EditLogFileOutputStream spyElos = spy(elos);
    jas.setCurrentStreamForTests(spyElos);
    return spyElos;
  }

  /**
   * Pull out one of the JournalAndStream objects from the edit log.
   */
  private JournalAndStream getJournalAndStream(int index) {
    FSImage fsimage = cluster.getNamesystem().getFSImage();
    FSEditLog editLog = fsimage.getEditLog();

    return editLog.getJournals().get(index);
  }

  /**
   * Do a mutative metadata operation on the file system.
   * 
   * @return true if the operation was successful, false otherwise.
   */
  private boolean doAnEdit() throws IOException {
    return fs.mkdirs(new Path("/tmp", Integer.toString(editsPerformed++)));
  }
}
