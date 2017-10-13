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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.StringWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Supplier;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.namenode.NamenodeFsck.Result;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.tools.DFSck;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.junit.Test;

import com.google.common.collect.Sets;

/**
 * A JUnit test for doing fsck
 */
public class TestFsck {
  static final String auditLogFile = System.getProperty("test.build.dir",
      "build/test") + "/TestFsck-audit.log";
  
  // Pattern for: 
  // allowed=true ugi=name ip=/address cmd=FSCK src=/ dst=null perm=null
  static final Pattern fsckPattern = Pattern.compile(
      "allowed=.*?\\s" +
      "ugi=.*?\\s" + 
      "ip=/\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\s" + 
      "cmd=fsck\\ssrc=\\/\\sdst=null\\s" + 
      "perm=null\\s" + "proto=.*");
  static final Pattern getfileinfoPattern = Pattern.compile(
      "allowed=.*?\\s" +
      "ugi=.*?\\s" + 
      "ip=/\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\s" + 
      "cmd=getfileinfo\\ssrc=\\/\\sdst=null\\s" + 
      "perm=null\\s" + "proto=.*");
  
  static final Pattern numCorruptBlocksPattern = Pattern.compile(
      ".*Corrupt blocks:\t\t([0123456789]*).*");
  
  private static final String LINE_SEPARATOR =
    System.getProperty("line.separator");

  static String runFsck(Configuration conf, int expectedErrCode, 
                        boolean checkErrorCode,String... path)
                        throws Exception {
    ByteArrayOutputStream bStream = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(bStream, true);
    ((Log4JLogger)FSPermissionChecker.LOG).getLogger().setLevel(Level.ALL);
    int errCode = ToolRunner.run(new DFSck(conf, out), path);
    if (checkErrorCode) {
      assertEquals(expectedErrCode, errCode);
    }
    ((Log4JLogger)FSPermissionChecker.LOG).getLogger().setLevel(Level.INFO);
    FSImage.LOG.error("OUTPUT = " + bStream.toString());
    return bStream.toString();
  }

  /** do fsck */
  @Test
  public void testFsck() throws Exception {
    DFSTestUtil util = new DFSTestUtil.Builder().setName("TestFsck").
        setNumFiles(20).build();
    MiniDFSCluster cluster = null;
    FileSystem fs = null;
    try {
      Configuration conf = new HdfsConfiguration();
      final long precision = 1L;
      conf.setLong(DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY, precision);
      conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 10000L);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(4).build();
      fs = cluster.getFileSystem();
      final String fileName = "/srcdat";
      util.createFiles(fs, fileName);
      util.waitReplication(fs, fileName, (short)3);
      final Path file = new Path(fileName);
      long aTime = fs.getFileStatus(file).getAccessTime();
      Thread.sleep(precision);
      setupAuditLogs();
      String outStr = runFsck(conf, 0, true, "/");
      verifyAuditLogs();
      assertEquals(aTime, fs.getFileStatus(file).getAccessTime());
      System.out.println(outStr);
      assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));
      if (fs != null) {try{fs.close();} catch(Exception e){}}
      cluster.shutdown();
      
      // restart the cluster; bring up namenode but not the data nodes
      cluster = new MiniDFSCluster.Builder(conf)
          .numDataNodes(0).format(false).build();
      outStr = runFsck(conf, 1, true, "/");
      // expect the result is corrupt
      assertTrue(outStr.contains(NamenodeFsck.CORRUPT_STATUS));
      System.out.println(outStr);
      
      // bring up data nodes & cleanup cluster
      cluster.startDataNodes(conf, 4, true, null, null);
      cluster.waitActive();
      cluster.waitClusterUp();
      fs = cluster.getFileSystem();
      util.cleanup(fs, "/srcdat");
    } finally {
      if (fs != null) {try{fs.close();} catch(Exception e){}}
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  /** Sets up log4j logger for auditlogs */
  private void setupAuditLogs() throws IOException {
    File file = new File(auditLogFile);
    if (file.exists()) {
      file.delete();
    }
    Logger logger = ((Log4JLogger) FSNamesystem.auditLog).getLogger();
    logger.setLevel(Level.INFO);
    PatternLayout layout = new PatternLayout("%m%n");
    RollingFileAppender appender = new RollingFileAppender(layout, auditLogFile);
    logger.addAppender(appender);
  }
  
  private void verifyAuditLogs() throws IOException {
    // Turn off the logs
    Logger logger = ((Log4JLogger) FSNamesystem.auditLog).getLogger();
    logger.setLevel(Level.OFF);

    BufferedReader reader = null;
    try {
      // Audit log should contain one getfileinfo and one fsck
      reader = new BufferedReader(new FileReader(auditLogFile));
      String line;

      // one extra getfileinfo stems from resolving the path
      //
      for (int i = 0; i < 2; i++) {
        line = reader.readLine();
        assertNotNull(line);
        assertTrue("Expected getfileinfo event not found in audit log",
            getfileinfoPattern.matcher(line).matches());
      }
      line = reader.readLine();
      assertNotNull(line);
      assertTrue("Expected fsck event not found in audit log", fsckPattern
          .matcher(line).matches());
      assertNull("Unexpected event in audit log", reader.readLine());
    } finally {
      // Close the reader and remove the appender to release the audit log file
      // handle after verifying the content of the file.
      if (reader != null) {
        reader.close();
      }
      if (logger != null) {
        logger.removeAllAppenders();
      }
    }
  }
  
  @Test
  public void testFsckNonExistent() throws Exception {
    DFSTestUtil util = new DFSTestUtil.Builder().setName("TestFsck").
        setNumFiles(20).build();
    MiniDFSCluster cluster = null;
    FileSystem fs = null;
    try {
      Configuration conf = new HdfsConfiguration();
      conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 10000L);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(4).build();
      fs = cluster.getFileSystem();
      util.createFiles(fs, "/srcdat");
      util.waitReplication(fs, "/srcdat", (short)3);
      String outStr = runFsck(conf, 0, true, "/non-existent");
      assertEquals(-1, outStr.indexOf(NamenodeFsck.HEALTHY_STATUS));
      System.out.println(outStr);
      util.cleanup(fs, "/srcdat");
    } finally {
      if (fs != null) {try{fs.close();} catch(Exception e){}}
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  /** Test fsck with permission set on inodes */
  @Test
  public void testFsckPermission() throws Exception {
    final DFSTestUtil util = new DFSTestUtil.Builder().
        setName(getClass().getSimpleName()).setNumFiles(20).build();
    final Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 10000L);

    MiniDFSCluster cluster = null;
    try {
      // Create a cluster with the current user, write some files
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(4).build();
      final MiniDFSCluster c2 = cluster;
      final String dir = "/dfsck";
      final Path dirpath = new Path(dir);
      final FileSystem fs = c2.getFileSystem();

      util.createFiles(fs, dir);
      util.waitReplication(fs, dir, (short) 3);
      fs.setPermission(dirpath, new FsPermission((short) 0700));

      // run DFSck as another user, should fail with permission issue
      UserGroupInformation fakeUGI = UserGroupInformation.createUserForTesting(
          "ProbablyNotARealUserName", new String[] { "ShangriLa" });
      fakeUGI.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          System.out.println(runFsck(conf, -1, true, dir));
          return null;
        }
      });
      
      // set permission and try DFSck again as the fake user, should succeed
      fs.setPermission(dirpath, new FsPermission((short) 0777));
      fakeUGI.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          final String outStr = runFsck(conf, 0, true, dir);
          System.out.println(outStr);
          assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));
          return null;
        }
      });

      util.cleanup(fs, dir);
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  @Test
  public void testFsckMove() throws Exception {
    Configuration conf = new HdfsConfiguration();
    final int DFS_BLOCK_SIZE = 1024;
    final int NUM_DATANODES = 4;
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DFS_BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 10000L);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY, 1);
    DFSTestUtil util = new DFSTestUtil("TestFsck", 5, 3,
        (5 * DFS_BLOCK_SIZE) + (DFS_BLOCK_SIZE - 1), 5 * DFS_BLOCK_SIZE);
    MiniDFSCluster cluster = null;
    FileSystem fs = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).
          numDataNodes(NUM_DATANODES).build();
      String topDir = "/srcdat";
      fs = cluster.getFileSystem();
      cluster.waitActive();
      util.createFiles(fs, topDir);
      util.waitReplication(fs, topDir, (short)3);
      String outStr = runFsck(conf, 0, true, "/");
      assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));
      DFSClient dfsClient = new DFSClient(new InetSocketAddress("localhost",
                                          cluster.getNameNodePort()), conf);
      String fileNames[] = util.getFileNames(topDir);
      CorruptedTestFile ctFiles[] = new CorruptedTestFile[] {
        new CorruptedTestFile(fileNames[0], Sets.newHashSet(0),
          dfsClient, NUM_DATANODES, DFS_BLOCK_SIZE),
        new CorruptedTestFile(fileNames[1], Sets.newHashSet(2, 3),
          dfsClient, NUM_DATANODES, DFS_BLOCK_SIZE),
        new CorruptedTestFile(fileNames[2], Sets.newHashSet(4),
          dfsClient, NUM_DATANODES, DFS_BLOCK_SIZE),
        new CorruptedTestFile(fileNames[3], Sets.newHashSet(0, 1, 2, 3),
          dfsClient, NUM_DATANODES, DFS_BLOCK_SIZE),
        new CorruptedTestFile(fileNames[4], Sets.newHashSet(1, 2, 3, 4),
          dfsClient, NUM_DATANODES, DFS_BLOCK_SIZE)
      };
      int totalMissingBlocks = 0;
      for (CorruptedTestFile ctFile : ctFiles) {
        totalMissingBlocks += ctFile.getTotalMissingBlocks();
      }
      for (CorruptedTestFile ctFile : ctFiles) {
        ctFile.removeBlocks(cluster);
      }
      // Wait for fsck to discover all the missing blocks
      while (true) {
        outStr = runFsck(conf, 1, false, "/");
        String numCorrupt = null;
        for (String line : outStr.split(LINE_SEPARATOR)) {
          Matcher m = numCorruptBlocksPattern.matcher(line);
          if (m.matches()) {
            numCorrupt = m.group(1);
            break;
          }
        }
        if (numCorrupt == null) {
          throw new IOException("failed to find number of corrupt " +
              "blocks in fsck output.");
        }
        if (numCorrupt.equals(Integer.toString(totalMissingBlocks))) {
          assertTrue(outStr.contains(NamenodeFsck.CORRUPT_STATUS));
          break;
        }
        try {
          Thread.sleep(100);
        } catch (InterruptedException ignore) {
        }
      }

      // Copy the non-corrupt blocks of corruptFileName to lost+found.
      outStr = runFsck(conf, 1, false, "/", "-move");
      assertTrue(outStr.contains(NamenodeFsck.CORRUPT_STATUS));

      // Make sure that we properly copied the block files from the DataNodes
      // to lost+found
      for (CorruptedTestFile ctFile : ctFiles) {
        ctFile.checkSalvagedRemains();
      }

      // Fix the filesystem by removing corruptFileName
      outStr = runFsck(conf, 1, true, "/", "-delete");
      assertTrue(outStr.contains(NamenodeFsck.CORRUPT_STATUS));
      
      // Check to make sure we have a healthy filesystem
      outStr = runFsck(conf, 0, true, "/");
      assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS)); 
      util.cleanup(fs, topDir);
    } finally {
      if (fs != null) {try{fs.close();} catch(Exception e){}}
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  static private class CorruptedTestFile {
    final private String name;
    final private Set<Integer> blocksToCorrupt;
    final private DFSClient dfsClient;
    final private int numDataNodes;
    final private int blockSize;
    final private byte[] initialContents;
    
    public CorruptedTestFile(String name, Set<Integer> blocksToCorrupt,
        DFSClient dfsClient, int numDataNodes, int blockSize)
            throws IOException {
      this.name = name;
      this.blocksToCorrupt = blocksToCorrupt;
      this.dfsClient = dfsClient;
      this.numDataNodes = numDataNodes;
      this.blockSize = blockSize;
      this.initialContents = cacheInitialContents();
    }

    public int getTotalMissingBlocks() {
      return blocksToCorrupt.size();
    }

    private byte[] cacheInitialContents() throws IOException {
      HdfsFileStatus status = dfsClient.getFileInfo(name);
      byte[] content = new byte[(int)status.getLen()];
      DFSInputStream in = null;
      try {
        in = dfsClient.open(name);
        IOUtils.readFully(in, content, 0, content.length);
      } finally {
        in.close();
      }
      return content;
    }
    
    public void removeBlocks(MiniDFSCluster cluster)
        throws AccessControlException, FileNotFoundException,
        UnresolvedLinkException, IOException {
      for (int corruptIdx : blocksToCorrupt) {
        // Corrupt a block by deleting it
        ExtendedBlock block = dfsClient.getNamenode().getBlockLocations(
            name, blockSize * corruptIdx, Long.MAX_VALUE).get(0).getBlock();
        for (int i = 0; i < numDataNodes; i++) {
          File blockFile = cluster.getBlockFile(i, block);
          if(blockFile != null && blockFile.exists()) {
            assertTrue(blockFile.delete());
          }
        }
      }
    }
    
    public void checkSalvagedRemains() throws IOException {
      int chainIdx = 0;
      HdfsFileStatus status = dfsClient.getFileInfo(name);
      long length = status.getLen();
      int numBlocks = (int)((length + blockSize - 1) / blockSize);
      DFSInputStream in = null;
      byte[] blockBuffer = new byte[blockSize];

      try {
        for (int blockIdx = 0; blockIdx < numBlocks; blockIdx++) {
          if (blocksToCorrupt.contains(blockIdx)) {
            if (in != null) {
              in.close();
              in = null;
            }
            continue;
          }
          if (in == null) {
            in = dfsClient.open("/lost+found" + name + "/" + chainIdx);
            chainIdx++;
          }
          int len = blockBuffer.length;
          if (blockIdx == (numBlocks - 1)) {
            // The last block might not be full-length
            len = (int)(in.getFileLength() % blockSize);
            if (len == 0) len = blockBuffer.length;
          }
          IOUtils.readFully(in, blockBuffer, 0, len);
          int startIdx = blockIdx * blockSize;
          for (int i = 0; i < len; i++) {
            if (initialContents[startIdx + i] != blockBuffer[i]) {
              throw new IOException("salvaged file " + name + " differed " +
              "from what we expected on block " + blockIdx);
            }
          }
        }
      } finally {
        IOUtils.cleanup(null, in);
      }
    }
  }
  
  @Test
  public void testFsckMoveAndDelete() throws Exception {
    final int MAX_MOVE_TRIES = 5;
    DFSTestUtil util = new DFSTestUtil.Builder().
        setName("TestFsckMoveAndDelete").setNumFiles(5).build();
    MiniDFSCluster cluster = null;
    FileSystem fs = null;
    try {
      Configuration conf = new HdfsConfiguration();
      conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 10000L);
      conf.setInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY, 1);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(4).build();
      String topDir = "/srcdat";
      fs = cluster.getFileSystem();
      cluster.waitActive();
      util.createFiles(fs, topDir);
      util.waitReplication(fs, topDir, (short)3);
      String outStr = runFsck(conf, 0, true, "/");
      assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));
      
      // Corrupt a block by deleting it
      String[] fileNames = util.getFileNames(topDir);
      DFSClient dfsClient = new DFSClient(new InetSocketAddress("localhost",
                                          cluster.getNameNodePort()), conf);
      String corruptFileName = fileNames[0];
      ExtendedBlock block = dfsClient.getNamenode().getBlockLocations(
          corruptFileName, 0, Long.MAX_VALUE).get(0).getBlock();
      for (int i=0; i<4; i++) {
        File blockFile = cluster.getBlockFile(i, block);
        if(blockFile != null && blockFile.exists()) {
          assertTrue(blockFile.delete());
        }
      }

      // We excpect the filesystem to be corrupted
      outStr = runFsck(conf, 1, false, "/");
      while (!outStr.contains(NamenodeFsck.CORRUPT_STATUS)) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException ignore) {
        }
        outStr = runFsck(conf, 1, false, "/");
      } 
      
      // After a fsck -move, the corrupted file should still exist.
      for (int i = 0; i < MAX_MOVE_TRIES; i++) {
        outStr = runFsck(conf, 1, true, "/", "-move" );
        assertTrue(outStr.contains(NamenodeFsck.CORRUPT_STATUS));
        String[] newFileNames = util.getFileNames(topDir);
        boolean found = false;
        for (String f : newFileNames) {
          if (f.equals(corruptFileName)) {
            found = true;
            break;
          }
        }
        assertTrue(found);
      }

      // Fix the filesystem by moving corrupted files to lost+found
      outStr = runFsck(conf, 1, true, "/", "-move", "-delete");
      assertTrue(outStr.contains(NamenodeFsck.CORRUPT_STATUS));
      
      // Check to make sure we have healthy filesystem
      outStr = runFsck(conf, 0, true, "/");
      assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS)); 
      util.cleanup(fs, topDir);
      if (fs != null) {try{fs.close();} catch(Exception e){}}
      cluster.shutdown();
    } finally {
      if (fs != null) {try{fs.close();} catch(Exception e){}}
      if (cluster != null) { cluster.shutdown(); }
    }
  }
  
  @Test
  public void testFsckOpenFiles() throws Exception {
    DFSTestUtil util = new DFSTestUtil.Builder().setName("TestFsck").
        setNumFiles(4).build();
    MiniDFSCluster cluster = null;
    FileSystem fs = null;
    try {
      Configuration conf = new HdfsConfiguration();
      conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 10000L);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(4).build();
      String topDir = "/srcdat";
      String randomString = "HADOOP  ";
      fs = cluster.getFileSystem();
      cluster.waitActive();
      util.createFiles(fs, topDir);
      util.waitReplication(fs, topDir, (short)3);
      String outStr = runFsck(conf, 0, true, "/");
      assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));
      // Open a file for writing and do not close for now
      Path openFile = new Path(topDir + "/openFile");
      FSDataOutputStream out = fs.create(openFile);
      int writeCount = 0;
      while (writeCount != 100) {
        out.write(randomString.getBytes());
        writeCount++;                  
      }
      // We expect the filesystem to be HEALTHY and show one open file
      outStr = runFsck(conf, 0, true, topDir);
      System.out.println(outStr);
      assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));
      assertFalse(outStr.contains("OPENFORWRITE")); 
      // Use -openforwrite option to list open files
      outStr = runFsck(conf, 0, true, topDir, "-openforwrite");
      System.out.println(outStr);
      assertTrue(outStr.contains("OPENFORWRITE"));
      assertTrue(outStr.contains("openFile"));
      // Close the file
      out.close(); 
      // Now, fsck should show HEALTHY fs and should not show any open files
      outStr = runFsck(conf, 0, true, topDir);
      System.out.println(outStr);
      assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));
      assertFalse(outStr.contains("OPENFORWRITE"));
      util.cleanup(fs, topDir);
      if (fs != null) {try{fs.close();} catch(Exception e){}}
      cluster.shutdown();
    } finally {
      if (fs != null) {try{fs.close();} catch(Exception e){}}
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  @Test
  public void testCorruptBlock() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 1000);
    // Set short retry timeouts so this test runs faster
    conf.setInt(DFSConfigKeys.DFS_CLIENT_RETRY_WINDOW_BASE, 10);
    FileSystem fs = null;
    DFSClient dfsClient = null;
    LocatedBlocks blocks = null;
    int replicaCount = 0;
    Random random = new Random();
    String outStr = null;
    short factor = 1;

    MiniDFSCluster cluster = null;
    try {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    Path file1 = new Path("/testCorruptBlock");
    DFSTestUtil.createFile(fs, file1, 1024, factor, 0);
    // Wait until file replication has completed
    DFSTestUtil.waitReplication(fs, file1, factor);
    ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, file1);

    // Make sure filesystem is in healthy state
    outStr = runFsck(conf, 0, true, "/");
    System.out.println(outStr);
    assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));
    
    // corrupt replicas
    File blockFile = cluster.getBlockFile(0, block);
    if (blockFile != null && blockFile.exists()) {
      RandomAccessFile raFile = new RandomAccessFile(blockFile, "rw");
      FileChannel channel = raFile.getChannel();
      String badString = "BADBAD";
      int rand = random.nextInt((int) channel.size()/2);
      raFile.seek(rand);
      raFile.write(badString.getBytes());
      raFile.close();
    }
    // Read the file to trigger reportBadBlocks
    try {
      IOUtils.copyBytes(fs.open(file1), new IOUtils.NullOutputStream(), conf,
                        true);
    } catch (IOException ie) {
      // Ignore exception
    }

    dfsClient = new DFSClient(new InetSocketAddress("localhost",
                               cluster.getNameNodePort()), conf);
    blocks = dfsClient.getNamenode().
               getBlockLocations(file1.toString(), 0, Long.MAX_VALUE);
    replicaCount = blocks.get(0).getLocations().length;
    while (replicaCount != factor) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ignore) {
      }
      blocks = dfsClient.getNamenode().
                getBlockLocations(file1.toString(), 0, Long.MAX_VALUE);
      replicaCount = blocks.get(0).getLocations().length;
    }
    assertTrue (blocks.get(0).isCorrupt());

    // Check if fsck reports the same
    outStr = runFsck(conf, 1, true, "/");
    System.out.println(outStr);
    assertTrue(outStr.contains(NamenodeFsck.CORRUPT_STATUS));
    assertTrue(outStr.contains("testCorruptBlock"));
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }

  @Test
  public void testUnderMinReplicatedBlock() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 1000);
    // Set short retry timeouts so this test runs faster
    conf.setInt(DFSConfigKeys.DFS_CLIENT_RETRY_WINDOW_BASE, 10);
    // Set minReplication to 2
    short minReplication=2;
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY,minReplication);
    FileSystem fs = null;
    DFSClient dfsClient = null;
    LocatedBlocks blocks = null;
    int replicaCount = 0;
    Random random = new Random();
    String outStr = null;
    short factor = 1;
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
      cluster.waitActive();
      fs = cluster.getFileSystem();
      Path file1 = new Path("/testUnderMinReplicatedBlock");
      DFSTestUtil.createFile(fs, file1, 1024, minReplication, 0);
      // Wait until file replication has completed
      DFSTestUtil.waitReplication(fs, file1, minReplication);
      ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, file1);

      // Make sure filesystem is in healthy state
      outStr = runFsck(conf, 0, true, "/");
      System.out.println(outStr);
      assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));

      // corrupt the first replica
      File blockFile = cluster.getBlockFile(0, block);
      if (blockFile != null && blockFile.exists()) {
        RandomAccessFile raFile = new RandomAccessFile(blockFile, "rw");
        FileChannel channel = raFile.getChannel();
        String badString = "BADBAD";
        int rand = random.nextInt((int) channel.size()/2);
        raFile.seek(rand);
        raFile.write(badString.getBytes());
        raFile.close();
      }

      dfsClient = new DFSClient(new InetSocketAddress("localhost",
          cluster.getNameNodePort()), conf);
      blocks = dfsClient.getNamenode().
          getBlockLocations(file1.toString(), 0, Long.MAX_VALUE);
      replicaCount = blocks.get(0).getLocations().length;
      while (replicaCount != factor) {
        try {
          Thread.sleep(100);
          // Read the file to trigger reportBadBlocks
          try {
            IOUtils.copyBytes(fs.open(file1), new IOUtils.NullOutputStream(), conf,
                true);
          } catch (IOException ie) {
            // Ignore exception
          }
          System.out.println("sleep in try: replicaCount="+replicaCount+"  factor="+factor);
        } catch (InterruptedException ignore) {
        }
        blocks = dfsClient.getNamenode().
            getBlockLocations(file1.toString(), 0, Long.MAX_VALUE);
        replicaCount = blocks.get(0).getLocations().length;
      }

      // Check if fsck reports the same
      outStr = runFsck(conf, 0, true, "/");
      System.out.println(outStr);
      assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));
      assertTrue(outStr.contains("UNDER MIN REPL'D BLOCKS:\t1 (100.0 %)"));
      assertTrue(outStr.contains("dfs.namenode.replication.min:\t2"));
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }


  /** Test if fsck can return -1 in case of failure
   * 
   * @throws Exception
   */
  @Test
  public void testFsckError() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      // bring up a one-node cluster
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).build();
      String fileName = "/test.txt";
      Path filePath = new Path(fileName);
      FileSystem fs = cluster.getFileSystem();
      
      // create a one-block file
      DFSTestUtil.createFile(fs, filePath, 1L, (short)1, 1L);
      DFSTestUtil.waitReplication(fs, filePath, (short)1);
      
      // intentionally corrupt NN data structure
      INodeFile node = (INodeFile) cluster.getNamesystem().dir.getINode
          (fileName, true);
      final BlockInfoContiguous[] blocks = node.getBlocks();
      assertEquals(blocks.length, 1);
      blocks[0].setNumBytes(-1L);  // set the block length to be negative
      
      // run fsck and expect a failure with -1 as the error code
      String outStr = runFsck(conf, -1, true, fileName);
      System.out.println(outStr);
      assertTrue(outStr.contains(NamenodeFsck.FAILURE_STATUS));
      
      // clean up file system
      fs.delete(filePath, true);
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }
  
  /** check if option -list-corruptfiles of fsck command works properly */
  @Test
  public void testFsckListCorruptFilesBlocks() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 1000);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY, 1);
    FileSystem fs = null;

    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();
      fs = cluster.getFileSystem();
      DFSTestUtil util = new DFSTestUtil.Builder().
          setName("testGetCorruptFiles").setNumFiles(3).setMaxLevels(1).
          setMaxSize(1024).build();
      util.createFiles(fs, "/corruptData", (short) 1);
      util.waitReplication(fs, "/corruptData", (short) 1);

      // String outStr = runFsck(conf, 0, true, "/corruptData", "-list-corruptfileblocks");
      String outStr = runFsck(conf, 0, false, "/corruptData", "-list-corruptfileblocks");
      System.out.println("1. good fsck out: " + outStr);
      assertTrue(outStr.contains("has 0 CORRUPT files"));
      // delete the blocks
      final String bpid = cluster.getNamesystem().getBlockPoolId();
      for (int i=0; i<4; i++) {
        for (int j=0; j<=1; j++) {
          File storageDir = cluster.getInstanceStorageDir(i, j);
          File data_dir = MiniDFSCluster.getFinalizedDir(storageDir, bpid);
          List<File> metadataFiles = MiniDFSCluster.getAllBlockMetadataFiles(
              data_dir);
          if (metadataFiles == null)
            continue;
          for (File metadataFile : metadataFiles) {
            File blockFile = Block.metaToBlockFile(metadataFile);
            assertTrue("Cannot remove file.", blockFile.delete());
            assertTrue("Cannot remove file.", metadataFile.delete());
          }
        }
      }

      // wait for the namenode to see the corruption
      final NamenodeProtocols namenode = cluster.getNameNodeRpc();
      CorruptFileBlocks corruptFileBlocks = namenode
          .listCorruptFileBlocks("/corruptData", null);
      int numCorrupt = corruptFileBlocks.getFiles().length;
      while (numCorrupt == 0) {
        Thread.sleep(1000);
        corruptFileBlocks = namenode
            .listCorruptFileBlocks("/corruptData", null);
        numCorrupt = corruptFileBlocks.getFiles().length;
      }
      outStr = runFsck(conf, -1, true, "/corruptData", "-list-corruptfileblocks");
      System.out.println("2. bad fsck out: " + outStr);
      assertTrue(outStr.contains("has 3 CORRUPT files"));

      // Do a listing on a dir which doesn't have any corrupt blocks and validate
      util.createFiles(fs, "/goodData");
      outStr = runFsck(conf, 0, true, "/goodData", "-list-corruptfileblocks");
      System.out.println("3. good fsck out: " + outStr);
      assertTrue(outStr.contains("has 0 CORRUPT files"));
      util.cleanup(fs,"/corruptData");
      util.cleanup(fs, "/goodData");
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }
  
  /**
   * Test for checking fsck command on illegal arguments should print the proper
   * usage.
   */
  @Test
  public void testToCheckTheFsckCommandOnIllegalArguments() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      // bring up a one-node cluster
      Configuration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).build();
      String fileName = "/test.txt";
      Path filePath = new Path(fileName);
      FileSystem fs = cluster.getFileSystem();

      // create a one-block file
      DFSTestUtil.createFile(fs, filePath, 1L, (short) 1, 1L);
      DFSTestUtil.waitReplication(fs, filePath, (short) 1);

      // passing illegal option
      String outStr = runFsck(conf, -1, true, fileName, "-thisIsNotAValidFlag");
      System.out.println(outStr);
      assertTrue(!outStr.contains(NamenodeFsck.HEALTHY_STATUS));

      // passing multiple paths are arguments
      outStr = runFsck(conf, -1, true, "/", fileName);
      System.out.println(outStr);
      assertTrue(!outStr.contains(NamenodeFsck.HEALTHY_STATUS));
      // clean up file system
      fs.delete(filePath, true);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  /**
   * Tests that the # of missing block replicas and expected replicas is correct
   * @throws IOException
   */
  @Test
  public void testFsckMissingReplicas() throws IOException {
    // Desired replication factor
    // Set this higher than NUM_REPLICAS so it's under-replicated
    final short REPL_FACTOR = 2;
    // Number of replicas to actually start
    final short NUM_REPLICAS = 1;
    // Number of blocks to write
    final short NUM_BLOCKS = 3;
    // Set a small-ish blocksize
    final long blockSize = 512;
    
    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    
    MiniDFSCluster cluster = null;
    DistributedFileSystem dfs = null;
    
    try {
      // Startup a minicluster
      cluster = 
          new MiniDFSCluster.Builder(conf).numDataNodes(NUM_REPLICAS).build();
      assertNotNull("Failed Cluster Creation", cluster);
      cluster.waitClusterUp();
      dfs = cluster.getFileSystem();
      assertNotNull("Failed to get FileSystem", dfs);
      
      // Create a file that will be intentionally under-replicated
      final String pathString = new String("/testfile");
      final Path path = new Path(pathString);
      long fileLen = blockSize * NUM_BLOCKS;
      DFSTestUtil.createFile(dfs, path, fileLen, REPL_FACTOR, 1);
      
      // Create an under-replicated file
      NameNode namenode = cluster.getNameNode();
      NetworkTopology nettop = cluster.getNamesystem().getBlockManager()
          .getDatanodeManager().getNetworkTopology();
      Map<String,String[]> pmap = new HashMap<String, String[]>();
      Writer result = new StringWriter();
      PrintWriter out = new PrintWriter(result, true);
      InetAddress remoteAddress = InetAddress.getLocalHost();
      NamenodeFsck fsck = new NamenodeFsck(conf, namenode, nettop, pmap, out, 
          NUM_REPLICAS, remoteAddress);
      
      // Run the fsck and check the Result
      final HdfsFileStatus file = 
          namenode.getRpcServer().getFileInfo(pathString);
      assertNotNull(file);
      Result res = new Result(conf);
      fsck.check(pathString, file, res);
      // Also print the output from the fsck, for ex post facto sanity checks
      System.out.println(result.toString());
      assertEquals(res.missingReplicas, 
          (NUM_BLOCKS*REPL_FACTOR) - (NUM_BLOCKS*NUM_REPLICAS));
      assertEquals(res.numExpectedReplicas, NUM_BLOCKS*REPL_FACTOR);
    } finally {
      if(dfs != null) {
        dfs.close();
      }
      if(cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  /**
   * Tests that the # of misreplaced replicas is correct
   * @throws IOException
   */
  @Test
  public void testFsckMisPlacedReplicas() throws IOException {
    // Desired replication factor
    final short REPL_FACTOR = 2;
    // Number of replicas to actually start
    short NUM_DN = 2;
    // Number of blocks to write
    final short NUM_BLOCKS = 3;
    // Set a small-ish blocksize
    final long blockSize = 512;
    
    String [] racks = {"/rack1", "/rack1"};
    String [] hosts = {"host1", "host2"};
    
    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    
    MiniDFSCluster cluster = null;
    DistributedFileSystem dfs = null;
    
    try {
      // Startup a minicluster
      cluster = 
          new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN).hosts(hosts)
          .racks(racks).build();
      assertNotNull("Failed Cluster Creation", cluster);
      cluster.waitClusterUp();
      dfs = cluster.getFileSystem();
      assertNotNull("Failed to get FileSystem", dfs);
      
      // Create a file that will be intentionally under-replicated
      final String pathString = new String("/testfile");
      final Path path = new Path(pathString);
      long fileLen = blockSize * NUM_BLOCKS;
      DFSTestUtil.createFile(dfs, path, fileLen, REPL_FACTOR, 1);
      
      // Create an under-replicated file
      NameNode namenode = cluster.getNameNode();
      NetworkTopology nettop = cluster.getNamesystem().getBlockManager()
          .getDatanodeManager().getNetworkTopology();
      // Add a new node on different rack, so previous blocks' replicas 
      // are considered to be misplaced
      nettop.add(DFSTestUtil.getDatanodeDescriptor("/rack2", "/host3"));
      NUM_DN++;
      
      Map<String,String[]> pmap = new HashMap<String, String[]>();
      Writer result = new StringWriter();
      PrintWriter out = new PrintWriter(result, true);
      InetAddress remoteAddress = InetAddress.getLocalHost();
      NamenodeFsck fsck = new NamenodeFsck(conf, namenode, nettop, pmap, out, 
          NUM_DN, remoteAddress);
      
      // Run the fsck and check the Result
      final HdfsFileStatus file = 
          namenode.getRpcServer().getFileInfo(pathString);
      assertNotNull(file);
      Result res = new Result(conf);
      fsck.check(pathString, file, res);
      // check misReplicatedBlock number.
      assertEquals(res.numMisReplicatedBlocks, NUM_BLOCKS);
    } finally {
      if(dfs != null) {
        dfs.close();
      }
      if(cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /** Test fsck with FileNotFound */
  @Test
  public void testFsckFileNotFound() throws Exception {

    // Number of replicas to actually start
    final short NUM_REPLICAS = 1;

    Configuration conf = new Configuration();
    NameNode namenode = mock(NameNode.class);
    NetworkTopology nettop = mock(NetworkTopology.class);
    Map<String,String[]> pmap = new HashMap<String, String[]>();
    Writer result = new StringWriter();
    PrintWriter out = new PrintWriter(result, true);
    InetAddress remoteAddress = InetAddress.getLocalHost();
    FSNamesystem fsName = mock(FSNamesystem.class);
    BlockManager blockManager = mock(BlockManager.class);
    DatanodeManager dnManager = mock(DatanodeManager.class);

    when(namenode.getNamesystem()).thenReturn(fsName);
    when(fsName.getBlockLocations(any(FSPermissionChecker.class), anyString(),
                                  anyLong(), anyLong(),
                                  anyBoolean(), anyBoolean()))
        .thenThrow(new FileNotFoundException());
    when(fsName.getBlockManager()).thenReturn(blockManager);
    when(blockManager.getDatanodeManager()).thenReturn(dnManager);

    NamenodeFsck fsck = new NamenodeFsck(conf, namenode, nettop, pmap, out,
        NUM_REPLICAS, remoteAddress);

    String pathString = "/tmp/testFile";

    long length = 123L;
    boolean isDir = false;
    int blockReplication = 1;
    long blockSize = 128 *1024L;
    long modTime = 123123123L;
    long accessTime = 123123120L;
    FsPermission perms = FsPermission.getDefault();
    String owner = "foo";
    String group = "bar";
    byte [] symlink = null;
    byte [] path = new byte[128];
    path = DFSUtil.string2Bytes(pathString);
    long fileId = 312321L;
    int numChildren = 1;
    byte storagePolicy = 0;

    HdfsFileStatus file = new HdfsFileStatus(length, isDir, blockReplication,
        blockSize, modTime, accessTime, perms, owner, group, symlink, path,
        fileId, numChildren, null, storagePolicy);
    Result res = new Result(conf);

    try {
      fsck.check(pathString, file, res);
    } catch (Exception e) {
      fail("Unexpected exception "+ e.getMessage());
    }
    assertTrue(res.toString().contains("HEALTHY"));
  }

  /** Test fsck with symlinks in the filesystem */
  @Test
  public void testFsckSymlink() throws Exception {
    final DFSTestUtil util = new DFSTestUtil.Builder().
        setName(getClass().getSimpleName()).setNumFiles(1).build();
    final Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 10000L);

    MiniDFSCluster cluster = null;
    FileSystem fs = null;
    try {
      final long precision = 1L;
      conf.setLong(DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY, precision);
      conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 10000L);
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(4).build();
      fs = cluster.getFileSystem();
      final String fileName = "/srcdat";
      util.createFiles(fs, fileName);
      final FileContext fc = FileContext.getFileContext(
          cluster.getConfiguration(0));
      final Path file = new Path(fileName);
      final Path symlink = new Path("/srcdat-symlink");
      fc.createSymlink(file, symlink, false);
      util.waitReplication(fs, fileName, (short)3);
      long aTime = fc.getFileStatus(symlink).getAccessTime();
      Thread.sleep(precision);
      setupAuditLogs();
      String outStr = runFsck(conf, 0, true, "/");
      verifyAuditLogs();
      assertEquals(aTime, fc.getFileStatus(symlink).getAccessTime());
      System.out.println(outStr);
      assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));
      assertTrue(outStr.contains("Total symlinks:\t\t1"));
      util.cleanup(fs, fileName);
    } finally {
      if (fs != null) {try{fs.close();} catch(Exception e){}}
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  /**
   * Test for including the snapshot files in fsck report
   */
  @Test
  public void testFsckForSnapshotFiles() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
        .build();
    try {
      String runFsck = runFsck(conf, 0, true, "/", "-includeSnapshots",
          "-files");
      assertTrue(runFsck.contains("HEALTHY"));
      final String fileName = "/srcdat";
      DistributedFileSystem hdfs = cluster.getFileSystem();
      Path file1 = new Path(fileName);
      DFSTestUtil.createFile(hdfs, file1, 1024, (short) 1, 1000L);
      hdfs.allowSnapshot(new Path("/"));
      hdfs.createSnapshot(new Path("/"), "mySnapShot");
      runFsck = runFsck(conf, 0, true, "/", "-includeSnapshots", "-files");
      assertTrue(runFsck.contains("/.snapshot/mySnapShot/srcdat"));
      runFsck = runFsck(conf, 0, true, "/", "-files");
      assertFalse(runFsck.contains("mySnapShot"));
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Test for blockIdCK
   */

  @Test
  public void testBlockIdCK() throws Exception {

    final short REPL_FACTOR = 2;
    short NUM_DN = 2;
    final long blockSize = 512;

    String [] racks = {"/rack1", "/rack2"};
    String [] hosts = {"host1", "host2"};

    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 2);

    MiniDFSCluster cluster = null;
    DistributedFileSystem dfs = null;
    cluster =
      new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN).hosts(hosts)
        .racks(racks).build();

    assertNotNull("Failed Cluster Creation", cluster);
    cluster.waitClusterUp();
    dfs = cluster.getFileSystem();
    assertNotNull("Failed to get FileSystem", dfs);

    DFSTestUtil util = new DFSTestUtil.Builder().
      setName(getClass().getSimpleName()).setNumFiles(1).build();
    //create files
    final String pathString = new String("/testfile");
    final Path path = new Path(pathString);
    util.createFile(dfs, path, 1024, REPL_FACTOR , 1000L);
    util.waitReplication(dfs, path, REPL_FACTOR);
    StringBuilder sb = new StringBuilder();
    for (LocatedBlock lb: util.getAllBlocks(dfs, path)){
      sb.append(lb.getBlock().getLocalBlock().getBlockName()+" ");
    }
    String[] bIds = sb.toString().split(" ");

    //run fsck
    try {
      //illegal input test
      String runFsckResult = runFsck(conf, 0, true, "/", "-blockId",
          "not_a_block_id");
      assertTrue(runFsckResult.contains("Incorrect blockId format:"));

      //general test
      runFsckResult = runFsck(conf, 0, true, "/", "-blockId", sb.toString());
      assertTrue(runFsckResult.contains(bIds[0]));
      assertTrue(runFsckResult.contains(bIds[1]));
      assertTrue(runFsckResult.contains(
          "Block replica on datanode/rack: host1/rack1 is HEALTHY"));
      assertTrue(runFsckResult.contains(
          "Block replica on datanode/rack: host2/rack2 is HEALTHY"));
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Test for blockIdCK with datanode decommission
   */
  @Test
  public void testBlockIdCKDecommission() throws Exception {

    final short REPL_FACTOR = 1;
    short NUM_DN = 2;
    final long blockSize = 512;
    boolean checkDecommissionInProgress = false;
    String [] racks = {"/rack1", "/rack2"};
    String [] hosts = {"host1", "host2"};

    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 2);

    MiniDFSCluster cluster;
    DistributedFileSystem dfs ;
    cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN).hosts(hosts)
            .racks(racks).build();

    assertNotNull("Failed Cluster Creation", cluster);
    cluster.waitClusterUp();
    dfs = cluster.getFileSystem();
    assertNotNull("Failed to get FileSystem", dfs);

    DFSTestUtil util = new DFSTestUtil.Builder().
        setName(getClass().getSimpleName()).setNumFiles(1).build();
    //create files
    final String pathString = new String("/testfile");
    final Path path = new Path(pathString);
    util.createFile(dfs, path, 1024, REPL_FACTOR, 1000L);
    util.waitReplication(dfs, path, REPL_FACTOR);
    StringBuilder sb = new StringBuilder();
    for (LocatedBlock lb: util.getAllBlocks(dfs, path)){
      sb.append(lb.getBlock().getLocalBlock().getBlockName()+" ");
    }
    String[] bIds = sb.toString().split(" ");
    try {
      //make sure datanode that has replica is fine before decommission
      String outStr = runFsck(conf, 0, true, "/", "-blockId", bIds[0]);
      System.out.println(outStr);
      assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));

      //decommission datanode
      ExtendedBlock eb = util.getFirstBlock(dfs, path);
      DatanodeDescriptor dn = cluster.getNameNode().getNamesystem()
          .getBlockManager().getBlockCollection(eb.getLocalBlock())
          .getBlocks()[0].getDatanode(0);
      cluster.getNameNode().getNamesystem().getBlockManager()
          .getDatanodeManager().getDecomManager().startDecommission(dn);
      String dnName = dn.getXferAddr();

      //wait for decommission start
      DatanodeInfo datanodeInfo = null;
      int count = 0;
      do {
        Thread.sleep(2000);
        for (DatanodeInfo info : dfs.getDataNodeStats()) {
          if (dnName.equals(info.getXferAddr())) {
            datanodeInfo = info;
          }
        }
         //check decommissioning only once
        if(!checkDecommissionInProgress && datanodeInfo != null
            && datanodeInfo.isDecommissionInProgress()) {
          String fsckOut = runFsck(conf, 3, true, "/", "-blockId", bIds[0]);
          assertTrue(fsckOut.contains(NamenodeFsck.DECOMMISSIONING_STATUS));
          checkDecommissionInProgress =  true;
        }
      } while (datanodeInfo != null && !datanodeInfo.isDecommissioned());

      //check decommissioned
      String fsckOut = runFsck(conf, 2, true, "/", "-blockId", bIds[0]);
      assertTrue(fsckOut.contains(NamenodeFsck.DECOMMISSIONED_STATUS));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test for blockIdCK with block corruption
   */
  @Test
  public void testBlockIdCKCorruption() throws Exception {
    short NUM_DN = 1;
    final long blockSize = 512;
    Random random = new Random();
    DFSClient dfsClient;
    LocatedBlocks blocks;
    ExtendedBlock block;
    short repFactor = 1;
    String [] racks = {"/rack1"};
    String [] hosts = {"host1"};

    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 1000);
    // Set short retry timeouts so this test runs faster
    conf.setInt(DFSConfigKeys.DFS_CLIENT_RETRY_WINDOW_BASE, 10);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);

    MiniDFSCluster cluster = null;
    DistributedFileSystem dfs = null;
    try {
      cluster =
          new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DN).hosts(hosts)
              .racks(racks).build();

      assertNotNull("Failed Cluster Creation", cluster);
      cluster.waitClusterUp();
      dfs = cluster.getFileSystem();
      assertNotNull("Failed to get FileSystem", dfs);

      DFSTestUtil util = new DFSTestUtil.Builder().
        setName(getClass().getSimpleName()).setNumFiles(1).build();
      //create files
      final String pathString = new String("/testfile");
      final Path path = new Path(pathString);
      util.createFile(dfs, path, 1024, repFactor, 1000L);
      util.waitReplication(dfs, path, repFactor);
      StringBuilder sb = new StringBuilder();
      for (LocatedBlock lb: util.getAllBlocks(dfs, path)){
        sb.append(lb.getBlock().getLocalBlock().getBlockName()+" ");
      }
      String[] bIds = sb.toString().split(" ");

      //make sure block is healthy before we corrupt it
      String outStr = runFsck(conf, 0, true, "/", "-blockId", bIds[0]);
      System.out.println(outStr);
      assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));

      // corrupt replicas
      block = DFSTestUtil.getFirstBlock(dfs, path);
      File blockFile = cluster.getBlockFile(0, block);
      if (blockFile != null && blockFile.exists()) {
        RandomAccessFile raFile = new RandomAccessFile(blockFile, "rw");
        FileChannel channel = raFile.getChannel();
        String badString = "BADBAD";
        int rand = random.nextInt((int) channel.size()/2);
        raFile.seek(rand);
        raFile.write(badString.getBytes());
        raFile.close();
      }

      util.waitCorruptReplicas(dfs, cluster.getNamesystem(), path, block, 1);

      outStr = runFsck(conf, 1, false, "/", "-blockId", block.getBlockName());
      System.out.println(outStr);
      assertTrue(outStr.contains(NamenodeFsck.CORRUPT_STATUS));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private void writeFile(final DistributedFileSystem dfs,
      Path dir, String fileName) throws IOException {
    Path filePath = new Path(dir.toString() + Path.SEPARATOR + fileName);
    final FSDataOutputStream out = dfs.create(filePath);
    out.writeChars("teststring");
    out.close();
  }

  private void writeFile(final DistributedFileSystem dfs,
      String dirName, String fileName, String StoragePolicy) throws IOException {
    Path dirPath = new Path(dirName);
    dfs.mkdirs(dirPath);
    dfs.setStoragePolicy(dirPath, StoragePolicy);
    writeFile(dfs, dirPath, fileName);
  }

  /**
   * Test storage policy display
   */
  @Test
  public void testStoragePoliciesCK() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(3)
        .storageTypes(
            new StorageType[] {StorageType.DISK, StorageType.ARCHIVE})
        .build();
    try {
      cluster.waitActive();
      final DistributedFileSystem dfs = cluster.getFileSystem();
      writeFile(dfs, "/testhot", "file", "HOT");
      writeFile(dfs, "/testwarm", "file", "WARM");
      writeFile(dfs, "/testcold", "file", "COLD");
      String outStr = runFsck(conf, 0, true, "/", "-storagepolicies");
      assertTrue(outStr.contains("DISK:3(HOT)"));
      assertTrue(outStr.contains("DISK:1,ARCHIVE:2(WARM)"));
      assertTrue(outStr.contains("ARCHIVE:3(COLD)"));
      assertTrue(outStr.contains("All blocks satisfy specified storage policy."));
      dfs.setStoragePolicy(new Path("/testhot"), "COLD");
      dfs.setStoragePolicy(new Path("/testwarm"), "COLD");
      outStr = runFsck(conf, 0, true, "/", "-storagepolicies");
      assertTrue(outStr.contains("DISK:3(HOT)"));
      assertTrue(outStr.contains("DISK:1,ARCHIVE:2(WARM)"));
      assertTrue(outStr.contains("ARCHIVE:3(COLD)"));
      assertFalse(outStr.contains("All blocks satisfy specified storage policy."));
     } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test(timeout = 300000)
  public void testFsckCorruptWhenOneReplicaIsCorrupt()
      throws Exception {
    Configuration conf = new HdfsConfiguration();
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology()).numDataNodes(2)
        .build();
    try {
      cluster.waitActive();
      FileSystem fs = HATestUtil.configureFailoverFs(cluster, conf);
      cluster.transitionToActive(0);
      String filePath = "/appendTest";
      Path fileName = new Path(filePath);
      DFSTestUtil.createFile(fs, fileName, 512, (short) 2, 0);
      DFSTestUtil.waitReplication(fs, fileName, (short) 2);
      assertTrue("File not created", fs.exists(fileName));
      cluster.getDataNodes().get(1).shutdown();
      DFSTestUtil.appendFile(fs, fileName, "appendCorruptBlock");
      cluster.restartDataNode(1, true);
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          return (
              cluster.getNameNode(0).getNamesystem().getCorruptReplicaBlocks()
                  > 0);
        }
      }, 100, 5000);

      DFSTestUtil.appendFile(fs, fileName, "appendCorruptBlock");
      runFsck(cluster.getConfiguration(0), 0, true, "/");
    }finally {
      if(cluster!=null){
        cluster.shutdown();
      }
    }
  }
}
