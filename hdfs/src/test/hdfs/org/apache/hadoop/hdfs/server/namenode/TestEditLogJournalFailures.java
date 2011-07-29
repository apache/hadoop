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
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestEditLogJournalFailures {

  private int editsPerformed = 0;
  private Configuration conf;
  private MiniDFSCluster cluster;
  private FileSystem fs;
  private Runtime runtime;

  /**
   * Create the mini cluster for testing and sub in a custom runtime so that
   * edit log journal failures don't actually cause the JVM to exit.
   */
  @Before
  public void setUpMiniCluster() throws IOException {
    conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    
    runtime = Runtime.getRuntime();
    runtime = spy(runtime);
    doNothing().when(runtime).exit(anyInt());
    
    cluster.getNameNode().getFSImage().getEditLog().setRuntimeForTesting(runtime);
  }
   
  @After
  public void shutDownMiniCluster() throws IOException {
    fs.close();
    cluster.shutdown();
  }
   
  @Test
  public void testSingleFailedEditsDirOnFlush() throws IOException {
    assertTrue(doAnEdit());
    // Invalidate one edits journal.
    invalidateEditsDirAtIndex(0, true);
    // Make sure runtime.exit(...) hasn't been called at all yet.
    assertExitInvocations(0);
    assertTrue(doAnEdit());
    // A single journal failure should not result in a call to runtime.exit(...).
    assertExitInvocations(0);
    assertFalse(cluster.getNameNode().isInSafeMode());
  }
   
  @Test
  public void testAllEditsDirsFailOnFlush() throws IOException {
    assertTrue(doAnEdit());
    // Invalidate both edits journals.
    invalidateEditsDirAtIndex(0, true);
    invalidateEditsDirAtIndex(1, true);
    // Make sure runtime.exit(...) hasn't been called at all yet.
    assertExitInvocations(0);
    assertTrue(doAnEdit());
    // The previous edit could not be synced to any persistent storage, should
    // have halted the NN.
    assertExitInvocations(1);
  }
  
  @Test
  public void testSingleFailedEditsDirOnSetReadyToFlush() throws IOException {
    assertTrue(doAnEdit());
    // Invalidate one edits journal.
    invalidateEditsDirAtIndex(0, false);
    // Make sure runtime.exit(...) hasn't been called at all yet.
    assertExitInvocations(0);
    assertTrue(doAnEdit());
    // A single journal failure should not result in a call to runtime.exit(...).
    assertExitInvocations(0);
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
   * Restore the journal at index <code>index</code> with the passed
   * {@link EditLogOutputStream}.
   * 
   * @param index index of the journal to restore.
   * @param elos the {@link EditLogOutputStream} to put at that index.
   */
  private void restoreEditsDirAtIndex(int index, EditLogOutputStream elos) {
    FSImage fsimage = cluster.getNamesystem().getFSImage();
    FSEditLog editLog = fsimage.getEditLog();

    FSEditLog.JournalAndStream jas = editLog.getJournals().get(index);
    jas.setCurrentStreamForTests(elos);
  }

  /**
   * Do a mutative metadata operation on the file system.
   * 
   * @return true if the operation was successful, false otherwise.
   */
  private boolean doAnEdit() throws IOException {
    return fs.mkdirs(new Path("/tmp", Integer.toString(editsPerformed++)));
  }

  /**
   * Make sure that Runtime.exit(...) has been called
   * <code>expectedExits<code> number of times.
   * 
   * @param expectedExits the number of times Runtime.exit(...) should have been called.
   */
  private void assertExitInvocations(int expectedExits) {
    verify(runtime, times(expectedExits)).exit(anyInt());
  }
}
