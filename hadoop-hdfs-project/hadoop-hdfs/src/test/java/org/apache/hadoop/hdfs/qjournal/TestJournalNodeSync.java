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
package org.apache.hadoop.hdfs.qjournal;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager.EditLogFile;
import static org.apache.hadoop.hdfs.server.namenode.FileJournalManager
    .getLogFile;

import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;

/**
 * Unit test for Journal Node formatting upon re-installation and syncing.
 */
public class TestJournalNodeSync {
  private MiniQJMHACluster qjmhaCluster;
  private MiniDFSCluster dfsCluster;
  private MiniJournalCluster jCluster;
  private FileSystem fs;
  private FSNamesystem namesystem;
  private int editsPerformed = 0;
  private final String jid = "ns1";

  @Before
  public void setUpMiniCluster() throws IOException {
    final Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_JOURNALNODE_SYNC_INTERVAL_KEY, 1000L);
    qjmhaCluster = new MiniQJMHACluster.Builder(conf).setNumNameNodes(2)
      .build();
    dfsCluster = qjmhaCluster.getDfsCluster();
    jCluster = qjmhaCluster.getJournalCluster();

    dfsCluster.transitionToActive(0);
    fs = dfsCluster.getFileSystem(0);
    namesystem = dfsCluster.getNamesystem(0);
  }

  @After
  public void shutDownMiniCluster() throws IOException {
    if (qjmhaCluster != null) {
      qjmhaCluster.shutdown();
    }
  }

  @Test(timeout=30000)
  public void testJournalNodeSync() throws Exception {
    File firstJournalDir = jCluster.getJournalDir(0, jid);
    File firstJournalCurrentDir = new StorageDirectory(firstJournalDir)
        .getCurrentDir();

    // Generate some edit logs and delete one.
    long firstTxId = generateEditLog();
    generateEditLog();

    File missingLog = deleteEditLog(firstJournalCurrentDir, firstTxId);

    GenericTestUtils.waitFor(editLogExists(Lists.newArrayList(missingLog)),
        500, 10000);
  }

  @Test(timeout=30000)
  public void testSyncForMultipleMissingLogs() throws Exception {
    File firstJournalDir = jCluster.getJournalDir(0, jid);
    File firstJournalCurrentDir = new StorageDirectory(firstJournalDir)
        .getCurrentDir();

    // Generate some edit logs and delete two.
    long firstTxId = generateEditLog();
    long nextTxId = generateEditLog();

    List<File> missingLogs = Lists.newArrayList();
    missingLogs.add(deleteEditLog(firstJournalCurrentDir, firstTxId));
    missingLogs.add(deleteEditLog(firstJournalCurrentDir, nextTxId));

    GenericTestUtils.waitFor(editLogExists(missingLogs), 500, 10000);
  }

  @Test(timeout=30000)
  public void testSyncForDiscontinuousMissingLogs() throws Exception {
    File firstJournalDir = jCluster.getJournalDir(0, jid);
    File firstJournalCurrentDir = new StorageDirectory(firstJournalDir)
        .getCurrentDir();

    // Generate some edit logs and delete two discontinuous logs.
    long firstTxId = generateEditLog();
    generateEditLog();
    long nextTxId = generateEditLog();

    List<File> missingLogs = Lists.newArrayList();
    missingLogs.add(deleteEditLog(firstJournalCurrentDir, firstTxId));
    missingLogs.add(deleteEditLog(firstJournalCurrentDir, nextTxId));

    GenericTestUtils.waitFor(editLogExists(missingLogs), 500, 10000);
  }

  @Test(timeout=30000)
  public void testMultipleJournalsMissingLogs() throws Exception {
    File firstJournalDir = jCluster.getJournalDir(0, jid);
    File firstJournalCurrentDir = new StorageDirectory(firstJournalDir)
        .getCurrentDir();

    File secondJournalDir = jCluster.getJournalDir(1, jid);
    StorageDirectory sd = new StorageDirectory(secondJournalDir);
    File secondJournalCurrentDir = sd.getCurrentDir();

    // Generate some edit logs and delete one log from two journals.
    long firstTxId = generateEditLog();
    generateEditLog();

    List<File> missingLogs = Lists.newArrayList();
    missingLogs.add(deleteEditLog(firstJournalCurrentDir, firstTxId));
    missingLogs.add(deleteEditLog(secondJournalCurrentDir, firstTxId));

    GenericTestUtils.waitFor(editLogExists(missingLogs), 500, 10000);
  }

  @Test(timeout=60000)
  public void testMultipleJournalsMultipleMissingLogs() throws Exception {
    File firstJournalDir = jCluster.getJournalDir(0, jid);
    File firstJournalCurrentDir = new StorageDirectory(firstJournalDir)
        .getCurrentDir();

    File secondJournalDir = jCluster.getJournalDir(1, jid);
    File secondJournalCurrentDir = new StorageDirectory(secondJournalDir)
        .getCurrentDir();

    File thirdJournalDir = jCluster.getJournalDir(2, jid);
    File thirdJournalCurrentDir = new StorageDirectory(thirdJournalDir)
        .getCurrentDir();

    // Generate some edit logs and delete multiple logs in multiple journals.
    long firstTxId = generateEditLog();
    long secondTxId = generateEditLog();
    long thirdTxId = generateEditLog();

    List<File> missingLogs = Lists.newArrayList();
    missingLogs.add(deleteEditLog(firstJournalCurrentDir, firstTxId));
    missingLogs.add(deleteEditLog(secondJournalCurrentDir, firstTxId));
    missingLogs.add(deleteEditLog(secondJournalCurrentDir, secondTxId));
    missingLogs.add(deleteEditLog(thirdJournalCurrentDir, thirdTxId));

    GenericTestUtils.waitFor(editLogExists(missingLogs), 500, 30000);
  }

  // Test JournalNode Sync by randomly deleting edit logs from one or two of
  // the journals.
  @Test(timeout=60000)
  public void testRandomJournalMissingLogs() throws Exception {
    Random randomJournal = new Random();

    List<File> journalCurrentDirs = Lists.newArrayList();

    for (int i = 0; i < 3; i++) {
      journalCurrentDirs.add(new StorageDirectory(jCluster.getJournalDir(i,
          jid)).getCurrentDir());
    }

    int count = 0;
    long lastStartTxId;
    int journalIndex;
    List<File> missingLogs = Lists.newArrayList();
    while (count < 5) {
      lastStartTxId = generateEditLog();

      // Delete the last edit log segment from randomly selected journal node
      journalIndex = randomJournal.nextInt(3);
      missingLogs.add(deleteEditLog(journalCurrentDirs.get(journalIndex),
          lastStartTxId));

      // Delete the last edit log segment from two journals for some logs
      if (count % 2 == 0) {
        journalIndex = (journalIndex + 1) % 3;
        missingLogs.add(deleteEditLog(journalCurrentDirs.get(journalIndex),
            lastStartTxId));
      }

      count++;
    }

    GenericTestUtils.waitFor(editLogExists(missingLogs), 500, 30000);
  }

  private File deleteEditLog(File currentDir, long startTxId)
      throws IOException {
    EditLogFile logFile = getLogFile(currentDir, startTxId);
    while (logFile.isInProgress()) {
      dfsCluster.getNameNode(0).getRpcServer().rollEditLog();
      logFile = getLogFile(currentDir, startTxId);
    }
    File deleteFile = logFile.getFile();
    Assert.assertTrue("Couldn't delete edit log file", deleteFile.delete());

    return deleteFile;
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
   * Does an edit and rolls the Edit Log.
   *
   * @return the startTxId of next segment after rolling edits.
   */
  private long generateEditLog() throws IOException {
    long startTxId = namesystem.getFSImage().getEditLog().getLastWrittenTxId();
    Assert.assertTrue("Failed to do an edit", doAnEdit());
    dfsCluster.getNameNode(0).getRpcServer().rollEditLog();
    return startTxId;
  }

  private Supplier<Boolean> editLogExists(List<File> editLogs) {
    Supplier<Boolean> supplier = new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        for (File editLog : editLogs) {
          if (!editLog.exists()) {
            return false;
          }
        }
        return true;
      }
    };
    return supplier;
  }
}
