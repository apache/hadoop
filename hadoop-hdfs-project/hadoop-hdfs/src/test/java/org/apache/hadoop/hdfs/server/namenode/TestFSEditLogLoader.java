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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.SortedMap;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogLoader.EditLogValidation;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Level;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.google.common.io.Files;

public class TestFSEditLogLoader {
  
  static {
    ((Log4JLogger)FSImage.LOG).getLogger().setLevel(Level.ALL);
  }
  
  private static final File TEST_DIR = new File(
      System.getProperty("test.build.data","build/test/data"));

  private static final int NUM_DATA_NODES = 0;
  
  @Test
  public void testDisplayRecentEditLogOpCodes() throws IOException {
    // start a cluster 
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    FileSystem fileSys = null;
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATA_NODES)
        .build();
    cluster.waitActive();
    fileSys = cluster.getFileSystem();
    final FSNamesystem namesystem = cluster.getNamesystem();

    FSImage fsimage = namesystem.getFSImage();
    for (int i = 0; i < 20; i++) {
      fileSys.mkdirs(new Path("/tmp/tmp" + i));
    }
    StorageDirectory sd = fsimage.getStorage().dirIterator(NameNodeDirType.EDITS).next();
    cluster.shutdown();

    File editFile = FSImageTestUtil.findLatestEditsLog(sd).getFile();
    assertTrue("Should exist: " + editFile, editFile.exists());

    // Corrupt the edits file.
    long fileLen = editFile.length();
    RandomAccessFile rwf = new RandomAccessFile(editFile, "rw");
    rwf.seek(fileLen - 40);
    for (int i = 0; i < 20; i++) {
      rwf.write(FSEditLogOpCodes.OP_DELETE.getOpCode());
    }
    rwf.close();
    
    String expectedErrorMessage = "^Error replaying edit log at offset \\d+\n";
    expectedErrorMessage += "Recent opcode offsets: (\\d+\\s*){4}$";
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATA_NODES)
          .format(false).build();
      fail("should not be able to start");
    } catch (IOException e) {
      assertTrue("error message contains opcodes message",
          e.getMessage().matches(expectedErrorMessage));
    }
  }
  
  /**
   * Test that, if the NN restarts with a new minimum replication,
   * any files created with the old replication count will get
   * automatically bumped up to the new minimum upon restart.
   */
  @Test
  public void testReplicationAdjusted() throws IOException {
    // start a cluster 
    Configuration conf = new HdfsConfiguration();
    // Replicate and heartbeat fast to shave a few seconds off test
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);

    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2)
          .build();
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
  
      // Create a file with replication count 1
      Path p = new Path("/testfile");
      DFSTestUtil.createFile(fs, p, 10, /*repl*/ (short)1, 1);
      DFSTestUtil.waitReplication(fs, p, (short)1);
  
      // Shut down and restart cluster with new minimum replication of 2
      cluster.shutdown();
      cluster = null;
      
      conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY, 2);
  
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2)
        .format(false).build();
      cluster.waitActive();
      fs = cluster.getFileSystem();
      
      // The file should get adjusted to replication 2 when
      // the edit log is replayed.
      DFSTestUtil.waitReplication(fs, p, (short)2);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  /**
   * Test that the valid number of transactions can be counted from a file.
   * @throws IOException 
   */
  @Test
  public void testCountValidTransactions() throws IOException {
    File testDir = new File(TEST_DIR, "testCountValidTransactions");
    File logFile = new File(testDir,
        NNStorage.getInProgressEditsFileName(1));
    
    // Create a log file, and return the offsets at which each
    // transaction starts.
    FSEditLog fsel = null;
    final int NUM_TXNS = 30;
    SortedMap<Long, Long> offsetToTxId = Maps.newTreeMap();
    try {
      fsel = FSImageTestUtil.createStandaloneEditLog(testDir);
      fsel.open();
      assertTrue("should exist: " + logFile, logFile.exists());
      
      for (int i = 0; i < NUM_TXNS; i++) {
        long trueOffset = getNonTrailerLength(logFile);
        long thisTxId = fsel.getLastWrittenTxId() + 1;
        offsetToTxId.put(trueOffset, thisTxId);
        System.err.println("txid " + thisTxId + " at offset " + trueOffset);
        fsel.logDelete("path" + i, i);
        fsel.logSync();
      }
    } finally {
      if (fsel != null) {
        fsel.close();
      }
    }

    // The file got renamed when the log was closed.
    logFile = testDir.listFiles()[0];
    long validLength = getNonTrailerLength(logFile);

    // Make sure that uncorrupted log has the expected length and number
    // of transactions.
    EditLogValidation validation = EditLogFileInputStream.validateEditLog(logFile);
    assertEquals(NUM_TXNS + 2, validation.getNumTransactions());
    assertEquals(validLength, validation.getValidLength());
    
    // Back up the uncorrupted log
    File logFileBak = new File(testDir, logFile.getName() + ".bak");
    Files.copy(logFile, logFileBak);

    // Corrupt the log file in various ways for each txn
    for (Map.Entry<Long, Long> entry : offsetToTxId.entrySet()) {
      long txOffset = entry.getKey();
      long txid = entry.getValue();
      
      // Restore backup, truncate the file exactly before the txn
      Files.copy(logFileBak, logFile);
      truncateFile(logFile, txOffset);
      validation = EditLogFileInputStream.validateEditLog(logFile);
      assertEquals("Failed when truncating to length " + txOffset,
          txid - 1, validation.getNumTransactions());
      assertEquals(txOffset, validation.getValidLength());

      // Restore backup, truncate the file with one byte in the txn,
      // also isn't valid
      Files.copy(logFileBak, logFile);
      truncateFile(logFile, txOffset + 1);
      validation = EditLogFileInputStream.validateEditLog(logFile);
      assertEquals("Failed when truncating to length " + (txOffset + 1),
          txid - 1, validation.getNumTransactions());
      assertEquals(txOffset, validation.getValidLength());

      // Restore backup, corrupt the txn opcode
      Files.copy(logFileBak, logFile);
      corruptByteInFile(logFile, txOffset);
      validation = EditLogFileInputStream.validateEditLog(logFile);
      assertEquals("Failed when corrupting txn opcode at " + txOffset,
          txid - 1, validation.getNumTransactions());
      assertEquals(txOffset, validation.getValidLength());

      // Restore backup, corrupt a byte a few bytes into the txn
      Files.copy(logFileBak, logFile);
      corruptByteInFile(logFile, txOffset+5);
      validation = EditLogFileInputStream.validateEditLog(logFile);
      assertEquals("Failed when corrupting txn data at " + (txOffset+5),
          txid - 1, validation.getNumTransactions());
      assertEquals(txOffset, validation.getValidLength());
    }
    
    // Corrupt the log at every offset to make sure that validation itself
    // never throws an exception, and that the calculated lengths are monotonically
    // increasing
    long prevNumValid = 0;
    for (long offset = 0; offset < validLength; offset++) {
      Files.copy(logFileBak, logFile);
      corruptByteInFile(logFile, offset);
      EditLogValidation val = EditLogFileInputStream.validateEditLog(logFile);
      assertTrue(val.getNumTransactions() >= prevNumValid);
      prevNumValid = val.getNumTransactions();
    }
  }

  /**
   * Corrupt the byte at the given offset in the given file,
   * by subtracting 1 from it.
   */
  private void corruptByteInFile(File file, long offset)
      throws IOException {
    RandomAccessFile raf = new RandomAccessFile(file, "rw");
    try {
      raf.seek(offset);
      int origByte = raf.read();
      raf.seek(offset);
      raf.writeByte(origByte - 1);
    } finally {
      IOUtils.closeStream(raf);
    }
  }

  /**
   * Truncate the given file to the given length
   */
  private void truncateFile(File logFile, long newLength)
      throws IOException {
    RandomAccessFile raf = new RandomAccessFile(logFile, "rw");
    raf.setLength(newLength);
    raf.close();
  }

  /**
   * Return the length of bytes in the given file after subtracting
   * the trailer of 0xFF (OP_INVALID)s.
   * This seeks to the end of the file and reads chunks backwards until
   * it finds a non-0xFF byte.
   * @throws IOException if the file cannot be read
   */
  private static long getNonTrailerLength(File f) throws IOException {
    final int chunkSizeToRead = 256*1024;
    FileInputStream fis = new FileInputStream(f);
    try {
      
      byte buf[] = new byte[chunkSizeToRead];
  
      FileChannel fc = fis.getChannel();
      long size = fc.size();
      long pos = size - (size % chunkSizeToRead);
      
      while (pos >= 0) {
        fc.position(pos);
  
        int readLen = (int) Math.min(size - pos, chunkSizeToRead);
        IOUtils.readFully(fis, buf, 0, readLen);
        for (int i = readLen - 1; i >= 0; i--) {
          if (buf[i] != FSEditLogOpCodes.OP_INVALID.getOpCode()) {
            return pos + i + 1; // + 1 since we count this byte!
          }
        }
        
        pos -= chunkSizeToRead;
      }
      return 0;
    } finally {
      fis.close();
    }
  }
}
