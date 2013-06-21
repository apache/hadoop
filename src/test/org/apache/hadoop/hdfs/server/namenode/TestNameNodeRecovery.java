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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.FSImage.NameNodeFile;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

/**
 * This tests data recovery mode for the NameNode.
 */
public class TestNameNodeRecovery {
  private static final Log LOG = LogFactory.getLog(TestNameNodeRecovery.class);
  private static StartupOption recoverStartOpt = StartupOption.RECOVER;

  static {
    recoverStartOpt.setForce(MetaRecoveryContext.FORCE_ALL);
  }

  static interface Corruptor {
    public void corrupt(File editFile) throws IOException;
    public boolean fatalCorruption();
  }
    
  static class TruncatingCorruptor implements Corruptor {
    @Override
    public void corrupt(File editFile) throws IOException {
      // Corrupt the last edit
      long fileLen = editFile.length();
      RandomAccessFile rwf = new RandomAccessFile(editFile, "rw");
      rwf.setLength(fileLen - 1);
      rwf.close();
    }
    
    @Override
    public boolean fatalCorruption() {
      return true;
    }
  }

  static final void pad(RandomAccessFile rwf, byte b, int amt)
      throws IOException {
    byte buf[] = new byte[1024];
    for (int i = 0; i < buf.length; i++) {
      buf[i] = 0;
    }
    while (amt > 0) {
      int len = (amt < buf.length) ? amt : buf.length;
      rwf.write(buf, 0, len);
      amt -= len;
    }
  }
  
  static class PaddingCorruptor implements Corruptor {
    @Override
    public void corrupt(File editFile) throws IOException {
      // Add junk to the end of the file
      RandomAccessFile rwf = new RandomAccessFile(editFile, "rw");
      rwf.seek(editFile.length());
      pad(rwf, (byte)0, 2098176);
      rwf.write(0x44);
      rwf.close();
    }
    
    @Override
    public boolean fatalCorruption() {
      return true;
    }
  }
  
  static class SafePaddingCorruptor implements Corruptor {
    private byte padByte;
    
    public SafePaddingCorruptor(byte padByte) {
      this.padByte = padByte;
      assert ((this.padByte == 0) || (this.padByte == -1));
    }

    @Override
    public void corrupt(File editFile) throws IOException {
      // Add junk to the end of the file
      RandomAccessFile rwf = new RandomAccessFile(editFile, "rw");
      rwf.seek(editFile.length());
      rwf.write((byte)-1);
      pad(rwf, padByte, 2098176);
      rwf.close();
    }
    
    @Override
    public boolean fatalCorruption() {
      return false;
    }
  }
  
  static void testNameNodeRecoveryImpl(Corruptor corruptor) throws IOException
  {
    final String TEST_PATH = "/test/path/dir";
    final String TEST_PATH2 = "/alt/test/path";
  
    // Start up the mini dfs cluster
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_EDITS_TOLERATION_LENGTH_KEY, -1);
    MiniDFSCluster cluster;
    cluster = new MiniDFSCluster(0, conf, 0, true, true, false,
        StartupOption.FORMAT, null, null, null);
    cluster.waitActive();
    FileSystem fileSys = cluster.getFileSystem();
    fileSys.mkdirs(new Path(TEST_PATH));
    fileSys.mkdirs(new Path(TEST_PATH2));
  
    List<File> nameEditsDirs =
        (List<File>)FSNamesystem.getNamespaceEditsDirs(conf);
    cluster.shutdown();
  
    File dir = nameEditsDirs.get(0); //has only one
    File editFile = new File(new File(dir, "current"),
        NameNodeFile.EDITS.getName());
    assertTrue("Should exist: " + editFile, editFile.exists());
  
    corruptor.corrupt(editFile);
  
    // Check how our corruption affected NameNode startup.
    try {
      LOG.debug("trying to start normally (this should fail)...");
      cluster = new MiniDFSCluster(0, conf, 0, false, true, false,
          StartupOption.REGULAR, null, null, null);
      cluster.waitActive();
      if (corruptor.fatalCorruption()) {
        fail("expected the truncated edit log to prevent normal startup");
      }
    } catch (IOException e) {
      if (!corruptor.fatalCorruption()) {
        fail("expected to be able to start up normally, but couldn't.");
      }
    } finally {
      cluster.shutdown();
    }
  
    // Perform recovery
    try {
      LOG.debug("running recovery...");
      cluster = new MiniDFSCluster(0, conf, 0, false, true, false,
          StartupOption.RECOVER, null, null, null);
      cluster.waitActive();
    } catch (IOException e) {
      fail("caught IOException while trying to recover. " +
          "message was " + e.getMessage() +
          "\nstack trace\n" + StringUtils.stringifyException(e));
    } finally {
      cluster.shutdown();
    }
  
    // Make sure that we can start the cluster normally after recovery
    try {
      cluster = new MiniDFSCluster(0, conf, 0, false, true, false,
          StartupOption.REGULAR, null, null, null);
      cluster.waitActive();
      assertTrue(cluster.getFileSystem().exists(new Path(TEST_PATH)));
    } catch (IOException e) {
      fail("failed to recover.  Error message: " + e.getMessage());
    } finally {
      cluster.shutdown();
    }
  }
 
  /** Test that we can successfully recover from a situation where the last
   * entry in the edit log has been truncated. */
  @Test(timeout=180000)
  public void testRecoverTruncatedEditLog() throws IOException {
    testNameNodeRecoveryImpl(new TruncatingCorruptor());
    LOG.debug("testRecoverTruncatedEditLog: successfully recovered the " +
        "truncated edit log");
  }

  /** Test that we can successfully recover from a situation where garbage
   * bytes have been added to the end of the file. */
  @Test(timeout=180000)
  public void testRecoverPaddedEditLog() throws IOException {
    testNameNodeRecoveryImpl(new PaddingCorruptor());
    LOG.debug("testRecoverPaddedEditLog: successfully recovered the " +
        "padded edit log");
  }

  /** Test that we can successfully recover from a situation where 0
   * bytes have been added to the end of the file. */
  @Test(timeout=180000)
  public void testRecoverZeroPaddedEditLog() throws IOException {
    testNameNodeRecoveryImpl(new SafePaddingCorruptor((byte)0));
  }

  /** Test that we can successfully recover from a situation where -1
   * bytes have been added to the end of the file. */
  @Test(timeout=180000)
  public void testRecoverNegativeOnePaddedEditLog() throws IOException {
    testNameNodeRecoveryImpl(new SafePaddingCorruptor((byte)-1));
  }
}
