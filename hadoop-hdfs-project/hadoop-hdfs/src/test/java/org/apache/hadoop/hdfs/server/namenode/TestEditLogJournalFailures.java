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
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ExitUtil.ExitException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.fail;

public class TestEditLogJournalFailures {

  private int editsPerformed = 0;
  private MiniDFSCluster cluster;
  private FileSystem fs;

  /**
   * Create the mini cluster for testing and sub in a custom runtime so that
   * edit log journal failures don't actually cause the JVM to exit.
   */
  @Before
  public void setUpMiniCluster() throws IOException {
    setUpMiniCluster(new HdfsConfiguration(), true);
  }
  
  private void setUpMiniCluster(Configuration conf, boolean manageNameDfsDirs)
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
    }
    if (cluster != null) {
      try {
        cluster.shutdown();
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
    invalidateEditsDirAtIndex(0, true);
    // The NN has not terminated (no ExitException thrown)    
    assertTrue(doAnEdit());
    // The NN has not terminated (no ExitException thrown)
    assertFalse(cluster.getNameNode().isInSafeMode());
  }
   
  @Test
  public void testAllEditsDirsFailOnFlush() throws IOException {
    assertTrue(doAnEdit());
    // Invalidate both edits journals.
    invalidateEditsDirAtIndex(0, true);
    invalidateEditsDirAtIndex(1, true);
    // The NN has not terminated (no ExitException thrown)
    try {
      doAnEdit();
      fail("The previous edit could not be synced to any persistent storage, "
            + "should have halted the NN");
    } catch (RemoteException re) {
      assertTrue(re.toString().contains("ExitException"));
      GenericTestUtils.assertExceptionContains(
        "Could not sync enough journals to persistent storage. " +
        "Unsynced transactions: 1", re);
      cluster.getNamesystem().getFSImage().getEditLog()
          .abortCurrentLogSegment();
    }
  }
  
  @Test
  public void testSingleFailedEditsDirOnSetReadyToFlush() throws IOException {
    assertTrue(doAnEdit());
    // Invalidate one edits journal.
    invalidateEditsDirAtIndex(0, false);
    // The NN has not terminated (no ExitException thrown)
    assertTrue(doAnEdit());
    // A single journal failure should not result in a call to terminate
    assertFalse(cluster.getNameNode().isInSafeMode());
  }

  /**
   * Replace the journal at index <code>index</code> with one that throws an
   * exception on flush.
   * 
   * @param index the index of the journal to take offline.
   * @return the original <code>EditLogOutputStream</code> of the journal.
   */
  private EditLogOutputStream invalidateEditsDirAtIndex(int index,
      boolean failOnFlush) throws IOException {
    FSImage fsimage = cluster.getNamesystem().getFSImage();
    FSEditLog editLog = fsimage.getEditLog();
    

    FSEditLog.JournalAndStream jas = editLog.getJournals().get(index);
    EditLogFileOutputStream elos =
      (EditLogFileOutputStream) jas.getCurrentStream();
    EditLogFileOutputStream spyElos = spy(elos);
    
    if (failOnFlush) {
      doThrow(new IOException("fail on flush()")).when(spyElos).flush();
    } else {
      doThrow(new IOException("fail on setReadyToFlush()")).when(spyElos)
        .setReadyToFlush();
    }
    doNothing().when(spyElos).abort();
     
    jas.setCurrentStreamForTests(spyElos);
     
    return elos;
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
