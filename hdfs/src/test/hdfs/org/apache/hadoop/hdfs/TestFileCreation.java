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
package org.apache.hadoop.hdfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.EnumSet;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.FSDataset;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Level;


/**
 * This class tests various cases during file creation.
 */
public class TestFileCreation extends junit.framework.TestCase {
  static final String DIR = "/" + TestFileCreation.class.getSimpleName() + "/";

  {
    //((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LeaseManager.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);
  }

  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;
  static final int numBlocks = 2;
  static final int fileSize = numBlocks * blockSize + 1;
  boolean simulatedStorage = false;

  // creates a file but does not close it
  public static FSDataOutputStream createFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    System.out.println("createFile: Created " + name + " with " + repl + " replica.");
    FSDataOutputStream stm = fileSys.create(name, true,
                                            fileSys.getConf().getInt("io.file.buffer.size", 4096),
                                            (short)repl, (long)blockSize);
    return stm;
  }

  //
  // writes to file but does not close it
  //
  static void writeFile(FSDataOutputStream stm) throws IOException {
    writeFile(stm, fileSize);
  }

  //
  // writes specified bytes to file.
  //
  public static void writeFile(FSDataOutputStream stm, int size) throws IOException {
    byte[] buffer = AppendTestUtil.randomBytes(seed, size);
    stm.write(buffer, 0, size);
  }

  static private void checkData(byte[] actual, int from, byte[] expected, String message) {
    for (int idx = 0; idx < actual.length; idx++) {
      assertEquals(message+" byte "+(from+idx)+" differs. expected "+
                   expected[from+idx]+" actual "+actual[idx],
                   expected[from+idx], actual[idx]);
      actual[idx] = 0;
    }
  }

  static void checkFullFile(FileSystem fs, Path name) throws IOException {
    FileStatus stat = fs.getFileStatus(name);
    BlockLocation[] locations = fs.getFileBlockLocations(stat, 0, 
                                                         fileSize);
    for (int idx = 0; idx < locations.length; idx++) {
      String[] hosts = locations[idx].getNames();
      for (int i = 0; i < hosts.length; i++) {
        System.out.print( hosts[i] + " ");
      }
      System.out.println(" off " + locations[idx].getOffset() +
                         " len " + locations[idx].getLength());
    }

    byte[] expected = AppendTestUtil.randomBytes(seed, fileSize);
    FSDataInputStream stm = fs.open(name);
    byte[] actual = new byte[fileSize];
    stm.readFully(0, actual);
    checkData(actual, 0, expected, "Read 2");
    stm.close();
  }

  /**
   * Test that server default values can be retrieved on the client side
   */
  public void testServerDefaults() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, FSConstants.DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, FSConstants.DEFAULT_BYTES_PER_CHECKSUM);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY, FSConstants.DEFAULT_WRITE_PACKET_SIZE);
    conf.setInt("dfs.replication", FSConstants.DEFAULT_REPLICATION_FACTOR + 1);
    conf.setInt("io.file.buffer.size", FSConstants.DEFAULT_FILE_BUFFER_SIZE);
    MiniDFSCluster cluster = new MiniDFSCluster(conf,
        FSConstants.DEFAULT_REPLICATION_FACTOR + 1, true, null);
    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();
    try {
      FsServerDefaults serverDefaults = fs.getServerDefaults();
      assertEquals(FSConstants.DEFAULT_BLOCK_SIZE, serverDefaults.getBlockSize());
      assertEquals(FSConstants.DEFAULT_BYTES_PER_CHECKSUM, serverDefaults.getBytesPerChecksum());
      assertEquals(FSConstants.DEFAULT_WRITE_PACKET_SIZE, serverDefaults.getWritePacketSize());
      assertEquals(FSConstants.DEFAULT_REPLICATION_FACTOR + 1, serverDefaults.getReplication());
      assertEquals(FSConstants.DEFAULT_FILE_BUFFER_SIZE, serverDefaults.getFileBufferSize());
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

  /**
   * Test if file creation and disk space consumption works right
   */
  public void testFileCreation() throws IOException {
    Configuration conf = new HdfsConfiguration();
    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = cluster.getFileSystem();
    try {

      //
      // check that / exists
      //
      Path path = new Path("/");
      System.out.println("Path : \"" + path.toString() + "\"");
      System.out.println(fs.getFileStatus(path).isDirectory()); 
      assertTrue("/ should be a directory", 
                 fs.getFileStatus(path).isDirectory());

      //
      // Create a directory inside /, then try to overwrite it
      //
      Path dir1 = new Path("/test_dir");
      fs.mkdirs(dir1);
      System.out.println("createFile: Creating " + dir1.getName() + 
        " for overwrite of existing directory.");
      try {
        fs.create(dir1, true); // Create path, overwrite=true
        fs.close();
        assertTrue("Did not prevent directory from being overwritten.", false);
      } catch (IOException ie) {
        if (!ie.getMessage().contains("already exists as a directory."))
          throw ie;
      }
      
      // create a new file in home directory. Do not close it.
      //
      Path file1 = new Path("filestatus.dat");
      Path parent = file1.getParent();
      fs.mkdirs(parent);
      DistributedFileSystem dfs = (DistributedFileSystem)fs;
      dfs.setQuota(file1.getParent(), 100L, blockSize*5);
      FSDataOutputStream stm = createFile(fs, file1, 1);

      // verify that file exists in FS namespace
      assertTrue(file1 + " should be a file", 
                 fs.getFileStatus(file1).isFile());
      System.out.println("Path : \"" + file1 + "\"");

      // write to file
      writeFile(stm);

      stm.close();

      // verify that file size has changed to the full size
      long len = fs.getFileStatus(file1).getLen();
      assertTrue(file1 + " should be of size " + fileSize +
                 " but found to be of size " + len, 
                  len == fileSize);
      
      // verify the disk space the file occupied
      long diskSpace = dfs.getContentSummary(file1.getParent()).getLength();
      assertEquals(file1 + " should take " + fileSize + " bytes disk space " +
          "but found to take " + diskSpace + " bytes", fileSize, diskSpace);
      
      // Check storage usage 
      // can't check capacities for real storage since the OS file system may be changing under us.
      if (simulatedStorage) {
        DataNode dn = cluster.getDataNodes().get(0);
        assertEquals(fileSize, dn.getFSDataset().getDfsUsed());
        assertEquals(SimulatedFSDataset.DEFAULT_CAPACITY-fileSize, dn.getFSDataset().getRemaining());
      }
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Test deleteOnExit
   */
  public void testDeleteOnExit() throws IOException {
    Configuration conf = new HdfsConfiguration();
    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = cluster.getFileSystem();
    FileSystem localfs = FileSystem.getLocal(conf);

    try {

      // Creates files in HDFS and local file system.
      //
      Path file1 = new Path("filestatus.dat");
      Path file2 = new Path("filestatus2.dat");
      Path file3 = new Path("filestatus3.dat");
      FSDataOutputStream stm1 = createFile(fs, file1, 1);
      FSDataOutputStream stm2 = createFile(fs, file2, 1);
      FSDataOutputStream stm3 = createFile(localfs, file3, 1);
      System.out.println("DeleteOnExit: Created files.");

      // write to files and close. Purposely, do not close file2.
      writeFile(stm1);
      writeFile(stm3);
      stm1.close();
      stm2.close();
      stm3.close();

      // set delete on exit flag on files.
      fs.deleteOnExit(file1);
      fs.deleteOnExit(file2);
      localfs.deleteOnExit(file3);

      // close the file system. This should make the above files
      // disappear.
      fs.close();
      localfs.close();
      fs = null;
      localfs = null;

      // reopen file system and verify that file does not exist.
      fs = cluster.getFileSystem();
      localfs = FileSystem.getLocal(conf);

      assertTrue(file1 + " still exists inspite of deletOnExit set.",
                 !fs.exists(file1));
      assertTrue(file2 + " still exists inspite of deletOnExit set.",
                 !fs.exists(file2));
      assertTrue(file3 + " still exists inspite of deletOnExit set.",
                 !localfs.exists(file3));
      System.out.println("DeleteOnExit successful.");

    } finally {
      IOUtils.closeStream(fs);
      IOUtils.closeStream(localfs);
      cluster.shutdown();
    }
  }

  /**
   * Test that file data does not become corrupted even in the face of errors.
   */
  public void testFileCreationError1() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1000);
    conf.setInt("dfs.heartbeat.interval", 1);
    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = cluster.getFileSystem();
    cluster.waitActive();
    InetSocketAddress addr = new InetSocketAddress("localhost",
                                                   cluster.getNameNodePort());
    DFSClient client = new DFSClient(addr, conf);

    try {

      // create a new file.
      //
      Path file1 = new Path("/filestatus.dat");
      FSDataOutputStream stm = createFile(fs, file1, 1);

      // verify that file exists in FS namespace
      assertTrue(file1 + " should be a file", 
                 fs.getFileStatus(file1).isFile());
      System.out.println("Path : \"" + file1 + "\"");

      // kill the datanode
      cluster.shutdownDataNodes();

      // wait for the datanode to be declared dead
      while (true) {
        DatanodeInfo[] info = client.datanodeReport(
            FSConstants.DatanodeReportType.LIVE);
        if (info.length == 0) {
          break;
        }
        System.out.println("testFileCreationError1: waiting for datanode " +
                           " to die.");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
      }

      // write 1 byte to file. 
      // This should fail because all datanodes are dead.
      byte[] buffer = AppendTestUtil.randomBytes(seed, 1);
      try {
        stm.write(buffer);
        stm.close();
      } catch (Exception e) {
        System.out.println("Encountered expected exception");
      }

      // verify that no blocks are associated with this file
      // bad block allocations were cleaned up earlier.
      LocatedBlocks locations = client.getNamenode().getBlockLocations(
                                  file1.toString(), 0, Long.MAX_VALUE);
      System.out.println("locations = " + locations.locatedBlockCount());
      assertTrue("Error blocks were not cleaned up",
                 locations.locatedBlockCount() == 0);
    } finally {
      cluster.shutdown();
      client.close();
    }
  }

  /**
   * Test that the filesystem removes the last block from a file if its
   * lease expires.
   */
  public void testFileCreationError2() throws IOException {
    long leasePeriod = 1000;
    System.out.println("testFileCreationError2 start");
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1000);
    conf.setInt("dfs.heartbeat.interval", 1);
    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    DistributedFileSystem dfs = null;
    try {
      cluster.waitActive();
      dfs = (DistributedFileSystem)cluster.getFileSystem();
      DFSClient client = dfs.dfs;

      // create a new file.
      //
      Path file1 = new Path("/filestatus.dat");
      createFile(dfs, file1, 1);
      System.out.println("testFileCreationError2: "
                         + "Created file filestatus.dat with one replicas.");

      LocatedBlocks locations = client.getNamenode().getBlockLocations(
                                  file1.toString(), 0, Long.MAX_VALUE);
      System.out.println("testFileCreationError2: "
          + "The file has " + locations.locatedBlockCount() + " blocks.");

      // add one block to the file
      LocatedBlock location = client.getNamenode().addBlock(file1.toString(), 
          client.clientName, null, null);
      System.out.println("testFileCreationError2: "
          + "Added block " + location.getBlock());

      locations = client.getNamenode().getBlockLocations(file1.toString(), 
                                                    0, Long.MAX_VALUE);
      int count = locations.locatedBlockCount();
      System.out.println("testFileCreationError2: "
          + "The file now has " + count + " blocks.");
      
      // set the soft and hard limit to be 1 second so that the
      // namenode triggers lease recovery
      cluster.setLeasePeriod(leasePeriod, leasePeriod);

      // wait for the lease to expire
      try {
        Thread.sleep(5 * leasePeriod);
      } catch (InterruptedException e) {
      }

      // verify that the last block was synchronized.
      locations = client.getNamenode().getBlockLocations(file1.toString(), 
                                                    0, Long.MAX_VALUE);
      System.out.println("testFileCreationError2: "
          + "locations = " + locations.locatedBlockCount());
      assertEquals(0, locations.locatedBlockCount());
      System.out.println("testFileCreationError2 successful");
    } finally {
      IOUtils.closeStream(dfs);
      cluster.shutdown();
    }
  }

  /**
   * Test that file leases are persisted across namenode restarts.
   * This test is currently not triggered because more HDFS work is 
   * is needed to handle persistent leases.
   */
  public void xxxtestFileCreationNamenodeRestart() throws IOException {
    Configuration conf = new HdfsConfiguration();
    final int MAX_IDLE_TIME = 2000; // 2s
    conf.setInt("ipc.client.connection.maxidletime", MAX_IDLE_TIME);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1000);
    conf.setInt("dfs.heartbeat.interval", 1);
    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }

    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = null;
    try {
      cluster.waitActive();
      fs = cluster.getFileSystem();
      final int nnport = cluster.getNameNodePort();

      // create a new file.
      Path file1 = new Path("/filestatus.dat");
      FSDataOutputStream stm = createFile(fs, file1, 1);
      System.out.println("testFileCreationNamenodeRestart: "
                         + "Created file " + file1);
      int actualRepl = ((DFSOutputStream)(stm.getWrappedStream())).
                        getNumCurrentReplicas();
      assertTrue(file1 + " should be replicated to 1 datanodes.",
                 actualRepl == 1);

      // write two full blocks.
      writeFile(stm, numBlocks * blockSize);
      stm.hflush();
      actualRepl = ((DFSOutputStream)(stm.getWrappedStream())).
                        getNumCurrentReplicas();
      assertTrue(file1 + " should still be replicated to 1 datanodes.",
                 actualRepl == 1);

      // rename file wile keeping it open.
      Path fileRenamed = new Path("/filestatusRenamed.dat");
      fs.rename(file1, fileRenamed);
      System.out.println("testFileCreationNamenodeRestart: "
                         + "Renamed file " + file1 + " to " +
                         fileRenamed);
      file1 = fileRenamed;

      // create another new file.
      //
      Path file2 = new Path("/filestatus2.dat");
      FSDataOutputStream stm2 = createFile(fs, file2, 1);
      System.out.println("testFileCreationNamenodeRestart: "
                         + "Created file " + file2);

      // create yet another new file with full path name. 
      // rename it while open
      //
      Path file3 = new Path("/user/home/fullpath.dat");
      FSDataOutputStream stm3 = createFile(fs, file3, 1);
      System.out.println("testFileCreationNamenodeRestart: "
                         + "Created file " + file3);
      Path file4 = new Path("/user/home/fullpath4.dat");
      FSDataOutputStream stm4 = createFile(fs, file4, 1);
      System.out.println("testFileCreationNamenodeRestart: "
                         + "Created file " + file4);

      fs.mkdirs(new Path("/bin"));
      fs.rename(new Path("/user/home"), new Path("/bin"));
      Path file3new = new Path("/bin/home/fullpath.dat");
      System.out.println("testFileCreationNamenodeRestart: "
                         + "Renamed file " + file3 + " to " +
                         file3new);
      Path file4new = new Path("/bin/home/fullpath4.dat");
      System.out.println("testFileCreationNamenodeRestart: "
                         + "Renamed file " + file4 + " to " +
                         file4new);

      // restart cluster with the same namenode port as before.
      // This ensures that leases are persisted in fsimage.
      cluster.shutdown();
      try {
        Thread.sleep(2*MAX_IDLE_TIME);
      } catch (InterruptedException e) {
      }
      cluster = new MiniDFSCluster(nnport, conf, 1, false, true, 
                                   null, null, null);
      cluster.waitActive();

      // restart cluster yet again. This triggers the code to read in
      // persistent leases from fsimage.
      cluster.shutdown();
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
      }
      cluster = new MiniDFSCluster(nnport, conf, 1, false, true, 
                                   null, null, null);
      cluster.waitActive();
      fs = cluster.getFileSystem();

      // instruct the dfsclient to use a new filename when it requests
      // new blocks for files that were renamed.
      DFSOutputStream dfstream = (DFSOutputStream)
                                                 (stm.getWrappedStream());
      dfstream.setTestFilename(file1.toString());
      dfstream = (DFSOutputStream) (stm3.getWrappedStream());
      dfstream.setTestFilename(file3new.toString());
      dfstream = (DFSOutputStream) (stm4.getWrappedStream());
      dfstream.setTestFilename(file4new.toString());

      // write 1 byte to file.  This should succeed because the 
      // namenode should have persisted leases.
      byte[] buffer = AppendTestUtil.randomBytes(seed, 1);
      stm.write(buffer);
      stm.close();
      stm2.write(buffer);
      stm2.close();
      stm3.close();
      stm4.close();

      // verify that new block is associated with this file
      DFSClient client = ((DistributedFileSystem)fs).dfs;
      LocatedBlocks locations = client.getNamenode().getBlockLocations(
                                  file1.toString(), 0, Long.MAX_VALUE);
      System.out.println("locations = " + locations.locatedBlockCount());
      assertTrue("Error blocks were not cleaned up for file " + file1,
                 locations.locatedBlockCount() == 3);

      // verify filestatus2.dat
      locations = client.getNamenode().getBlockLocations(
                                  file2.toString(), 0, Long.MAX_VALUE);
      System.out.println("locations = " + locations.locatedBlockCount());
      assertTrue("Error blocks were not cleaned up for file " + file2,
                 locations.locatedBlockCount() == 1);
    } finally {
      IOUtils.closeStream(fs);
      cluster.shutdown();
    }
  }

  /**
   * Test that all open files are closed when client dies abnormally.
   */
  public void testDFSClientDeath() throws IOException, InterruptedException {
    Configuration conf = new HdfsConfiguration();
    System.out.println("Testing adbornal client death.");
    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = cluster.getFileSystem();
    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    DFSClient dfsclient = dfs.dfs;
    try {

      // create a new file in home directory. Do not close it.
      //
      Path file1 = new Path("/clienttest.dat");
      FSDataOutputStream stm = createFile(fs, file1, 1);
      System.out.println("Created file clienttest.dat");

      // write to file
      writeFile(stm);

      // close the dfsclient before closing the output stream.
      // This should close all existing file.
      dfsclient.close();

      // reopen file system and verify that file exists.
      assertTrue(file1 + " does not exist.", 
          AppendTestUtil.createHdfsWithDifferentUsername(conf).exists(file1));
    } finally {
      cluster.shutdown();
    }
  }
  
  /**
   * Test file creation using createNonRecursive().
   */
  public void testFileCreationNonRecursive() throws IOException {
    Configuration conf = new HdfsConfiguration();
    if (simulatedStorage) {
      conf.setBoolean(SimulatedFSDataset.CONFIG_PROPERTY_SIMULATED, true);
    }
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    FileSystem fs = cluster.getFileSystem();
    final Path path = new Path("/" + System.currentTimeMillis()
        + "-testFileCreationNonRecursive");
    FSDataOutputStream out = null;

    try {
      IOException expectedException = null;
      final String nonExistDir = "/non-exist-" + System.currentTimeMillis();

      fs.delete(new Path(nonExistDir), true);
      EnumSet<CreateFlag> createFlag = EnumSet.of(CreateFlag.CREATE);
      // Create a new file in root dir, should succeed
      out = createNonRecursive(fs, path, 1, createFlag);
      out.close();
      // Create a file when parent dir exists as file, should fail
      expectedException = null;
      try {
        createNonRecursive(fs, new Path(path, "Create"), 1, createFlag);
      } catch (IOException e) {
        expectedException = e;
      }
      assertTrue("Create a file when parent directory exists as a file"
          + " should throw FileAlreadyExistsException ",
          expectedException != null
              && expectedException instanceof FileAlreadyExistsException);
      fs.delete(path, true);
      // Create a file in a non-exist directory, should fail
      final Path path2 = new Path(nonExistDir + "/testCreateNonRecursive");
      expectedException = null;
      try {
        createNonRecursive(fs, path2, 1, createFlag);
      } catch (IOException e) {
        expectedException = e;
      }
      assertTrue("Create a file in a non-exist dir using"
          + " createNonRecursive() should throw FileNotFoundException ",
          expectedException != null
              && expectedException instanceof FileNotFoundException);

      EnumSet<CreateFlag> overwriteFlag = EnumSet.of(CreateFlag.OVERWRITE);
      // Overwrite a file in root dir, should succeed
      out = createNonRecursive(fs, path, 1, overwriteFlag);
      out.close();
      // Overwrite a file when parent dir exists as file, should fail
      expectedException = null;
      try {
        createNonRecursive(fs, new Path(path, "Overwrite"), 1, overwriteFlag);
      } catch (IOException e) {
        expectedException = e;
      }
      assertTrue("Overwrite a file when parent directory exists as a file"
          + " should throw FileAlreadyExistsException ",
          expectedException != null
              && expectedException instanceof FileAlreadyExistsException);
      fs.delete(path, true);
      // Overwrite a file in a non-exist directory, should fail
      final Path path3 = new Path(nonExistDir + "/testOverwriteNonRecursive");
      expectedException = null;
      try {
        createNonRecursive(fs, path3, 1, overwriteFlag);
      } catch (IOException e) {
        expectedException = e;
      }
      assertTrue("Overwrite a file in a non-exist dir using"
          + " createNonRecursive() should throw FileNotFoundException ",
          expectedException != null
              && expectedException instanceof FileNotFoundException);
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

  // creates a file using DistributedFileSystem.createNonRecursive()
  static FSDataOutputStream createNonRecursive(FileSystem fs, Path name,
      int repl, EnumSet<CreateFlag> flag) throws IOException {
    System.out.println("createNonRecursive: Created " + name + " with " + repl
        + " replica.");
    FSDataOutputStream stm = ((DistributedFileSystem) fs).createNonRecursive(
        name, FsPermission.getDefault(), flag, fs.getConf().getInt(
            "io.file.buffer.size", 4096), (short) repl, (long) blockSize, null);
    return stm;
  }
  

/**
 * Test that file data becomes available before file is closed.
 */
  public void testFileCreationSimulated() throws IOException {
    simulatedStorage = true;
    testFileCreation();
    simulatedStorage = false;
  }

  /**
   * Test creating two files at the same time. 
   */
  public void testConcurrentFileCreation() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);

    try {
      FileSystem fs = cluster.getFileSystem();
      
      Path[] p = {new Path("/foo"), new Path("/bar")};
      
      //write 2 files at the same time
      FSDataOutputStream[] out = {fs.create(p[0]), fs.create(p[1])};
      int i = 0;
      for(; i < 100; i++) {
        out[0].write(i);
        out[1].write(i);
      }
      out[0].close();
      for(; i < 200; i++) {out[1].write(i);}
      out[1].close();

      //verify
      FSDataInputStream[] in = {fs.open(p[0]), fs.open(p[1])};  
      for(i = 0; i < 100; i++) {assertEquals(i, in[0].read());}
      for(i = 0; i < 200; i++) {assertEquals(i, in[1].read());}
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }

  /**
   * Create a file, write something, hflush but not close.
   * Then change lease period and wait for lease recovery.
   * Finally, read the block directly from each Datanode and verify the content.
   */
  public void testLeaseExpireHardLimit() throws Exception {
    System.out.println("testLeaseExpireHardLimit start");
    final long leasePeriod = 1000;
    final int DATANODE_NUM = 3;

    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1000);
    conf.setInt("dfs.heartbeat.interval", 1);

    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster(conf, DATANODE_NUM, true, null);
    DistributedFileSystem dfs = null;
    try {
      cluster.waitActive();
      dfs = (DistributedFileSystem)cluster.getFileSystem();

      // create a new file.
      final String f = DIR + "foo";
      final Path fpath = new Path(f);
      FSDataOutputStream out = TestFileCreation.createFile(dfs, fpath, DATANODE_NUM);
      out.write("something".getBytes());
      out.hflush();
      int actualRepl = ((DFSOutputStream)(out.getWrappedStream())).
                        getNumCurrentReplicas();
      assertTrue(f + " should be replicated to " + DATANODE_NUM + " datanodes.",
                 actualRepl == DATANODE_NUM);

      // set the soft and hard limit to be 1 second so that the
      // namenode triggers lease recovery
      cluster.setLeasePeriod(leasePeriod, leasePeriod);
      // wait for the lease to expire
      try {Thread.sleep(5 * leasePeriod);} catch (InterruptedException e) {}

      LocatedBlocks locations = dfs.dfs.getNamenode().getBlockLocations(
          f, 0, Long.MAX_VALUE);
      assertEquals(1, locations.locatedBlockCount());
      LocatedBlock locatedblock = locations.getLocatedBlocks().get(0);
      int successcount = 0;
      for(DatanodeInfo datanodeinfo: locatedblock.getLocations()) {
        DataNode datanode = cluster.getDataNode(datanodeinfo.ipcPort);
        FSDataset dataset = (FSDataset)datanode.data;
        Block b = dataset.getStoredBlock(locatedblock.getBlock().getBlockId());
        File blockfile = dataset.findBlockFile(b.getBlockId());
        System.out.println("blockfile=" + blockfile);
        if (blockfile != null) {
          BufferedReader in = new BufferedReader(new FileReader(blockfile));
          assertEquals("something", in.readLine());
          in.close();
          successcount++;
        }
      }
      System.out.println("successcount=" + successcount);
      assertTrue(successcount > 0); 
    } finally {
      IOUtils.closeStream(dfs);
      cluster.shutdown();
    }

    System.out.println("testLeaseExpireHardLimit successful");
  }

  // test closing file system before all file handles are closed.
  public void testFsClose() throws Exception {
    System.out.println("test file system close start");
    final int DATANODE_NUM = 3;

    Configuration conf = new HdfsConfiguration();

    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster(conf, DATANODE_NUM, true, null);
    DistributedFileSystem dfs = null;
    try {
      cluster.waitActive();
      dfs = (DistributedFileSystem)cluster.getFileSystem();

      // create a new file.
      final String f = DIR + "foofs";
      final Path fpath = new Path(f);
      FSDataOutputStream out = TestFileCreation.createFile(dfs, fpath, DATANODE_NUM);
      out.write("something".getBytes());

      // close file system without closing file
      dfs.close();
    } finally {
      System.out.println("testFsClose successful");
      cluster.shutdown();
    }
  }

  // test closing file after cluster is shutdown
  public void testFsCloseAfterClusterShutdown() throws IOException {
    System.out.println("test testFsCloseAfterClusterShutdown start");
    final int DATANODE_NUM = 3;

    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY, 3);
    conf.setBoolean("ipc.client.ping", false); // hdfs timeout is default 60 seconds
    conf.setInt("ipc.ping.interval", 10000); // hdfs timeout is now 10 second

    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster(conf, DATANODE_NUM, true, null);
    DistributedFileSystem dfs = null;
    try {
      cluster.waitActive();
      dfs = (DistributedFileSystem)cluster.getFileSystem();

      // create a new file.
      final String f = DIR + "dhrubashutdown";
      final Path fpath = new Path(f);
      FSDataOutputStream out = TestFileCreation.createFile(dfs, fpath, DATANODE_NUM);
      out.write("something_dhruba".getBytes());
      out.hflush();    // ensure that block is allocated

      // shutdown last datanode in pipeline.
      cluster.stopDataNode(2);

      // close file. Since we have set the minReplcatio to 3 but have killed one
      // of the three datanodes, the close call will loop until the hdfsTimeout is
      // encountered.
      boolean hasException = false;
      try {
        out.close();
        System.out.println("testFsCloseAfterClusterShutdown: Error here");
      } catch (IOException e) {
        hasException = true;
      }
      assertTrue("Failed to close file after cluster shutdown", hasException);
    } finally {
      System.out.println("testFsCloseAfterClusterShutdown successful");
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
