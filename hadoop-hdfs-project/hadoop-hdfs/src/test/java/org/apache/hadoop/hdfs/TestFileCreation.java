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
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SYNCONCLOSE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.util.EnumSet;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.log4j.Level;
import org.junit.Test;

/**
 * This class tests various cases during file creation.
 */
public class TestFileCreation {
  static final String DIR = "/" + TestFileCreation.class.getSimpleName() + "/";

  {
    //((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LeaseManager.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LogFactory.getLog(FSNamesystem.class)).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);
  }

  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;
  static final int numBlocks = 2;
  static final int fileSize = numBlocks * blockSize + 1;
  boolean simulatedStorage = false;
  
  private static final String[] NON_CANONICAL_PATHS = new String[] {
    "//foo",
    "///foo2",
    "//dir//file",
    "////test2/file",
    "/dir/./file2",
    "/dir/../file3"
  };

  // creates a file but does not close it
  public static FSDataOutputStream createFile(FileSystem fileSys, Path name, int repl)
    throws IOException {
    System.out.println("createFile: Created " + name + " with " + repl + " replica.");
    FSDataOutputStream stm = fileSys.create(name, true, fileSys.getConf()
        .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096),
        (short) repl, blockSize);
    return stm;
  }

  public static HdfsDataOutputStream create(DistributedFileSystem dfs,
      Path name, int repl) throws IOException {
    return (HdfsDataOutputStream)createFile(dfs, name, repl);
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

  /**
   * Test that server default values can be retrieved on the client side
   */
  @Test
  public void testServerDefaults() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFS_BLOCK_SIZE_KEY, DFS_BLOCK_SIZE_DEFAULT);
    conf.setInt(DFS_BYTES_PER_CHECKSUM_KEY, DFS_BYTES_PER_CHECKSUM_DEFAULT);
    conf.setInt(DFS_CLIENT_WRITE_PACKET_SIZE_KEY, DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT);
    conf.setInt(DFS_REPLICATION_KEY, DFS_REPLICATION_DEFAULT + 1);
    conf.setInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
                     .numDataNodes(DFSConfigKeys.DFS_REPLICATION_DEFAULT + 1)
                     .build();
    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();
    try {
      FsServerDefaults serverDefaults = fs.getServerDefaults();
      assertEquals(DFS_BLOCK_SIZE_DEFAULT, serverDefaults.getBlockSize());
      assertEquals(DFS_BYTES_PER_CHECKSUM_DEFAULT, serverDefaults.getBytesPerChecksum());
      assertEquals(DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT, serverDefaults.getWritePacketSize());
      assertEquals(DFS_REPLICATION_DEFAULT + 1, serverDefaults.getReplication());
      assertEquals(IO_FILE_BUFFER_SIZE_DEFAULT, serverDefaults.getFileBufferSize());
    } finally {
      fs.close();
      cluster.shutdown();
    }
  }

  @Test
  public void testFileCreation() throws IOException {
    checkFileCreation(null, false);
  }

  /** Same test but the client should use DN hostnames */
  @Test
  public void testFileCreationUsingHostname() throws IOException {
    assumeTrue(System.getProperty("os.name").startsWith("Linux"));
    checkFileCreation(null, true);
  }

  /** Same test but the client should bind to a local interface */
  @Test
  public void testFileCreationSetLocalInterface() throws IOException {
    assumeTrue(System.getProperty("os.name").startsWith("Linux"));

    // The mini cluster listens on the loopback so we can use it here
    checkFileCreation("lo", false);

    try {
      checkFileCreation("bogus-interface", false);
      fail("Able to specify a bogus interface");
    } catch (UnknownHostException e) {
      assertEquals("No such interface bogus-interface", e.getMessage());
    }
  }

  /**
   * Test if file creation and disk space consumption works right
   * @param netIf the local interface, if any, clients should use to access DNs
   * @param useDnHostname whether the client should contact DNs by hostname
   */
  public void checkFileCreation(String netIf, boolean useDnHostname)
      throws IOException {
    Configuration conf = new HdfsConfiguration();
    if (netIf != null) {
      conf.set(DFSConfigKeys.DFS_CLIENT_LOCAL_INTERFACES, netIf);
    }
    conf.setBoolean(DFSConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME, useDnHostname);
    if (useDnHostname) {
      // Since the mini cluster only listens on the loopback we have to
      // ensure the hostname used to access DNs maps to the loopback. We
      // do this by telling the DN to advertise localhost as its hostname
      // instead of the default hostname.
      conf.set(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY, "localhost");
    }
    if (simulatedStorage) {
      SimulatedFSDataset.setFactory(conf);
    }
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .checkDataNodeHostConfig(true)
      .build();
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
        FsDatasetSpi<?> dataset = DataNodeTestUtils.getFSDataset(dn);
        assertEquals(fileSize, dataset.getDfsUsed());
        assertEquals(SimulatedFSDataset.DEFAULT_CAPACITY-fileSize,
            dataset.getRemaining());
      }
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Test deleteOnExit
   */
  @Test
  public void testDeleteOnExit() throws IOException {
    Configuration conf = new HdfsConfiguration();
    if (simulatedStorage) {
      SimulatedFSDataset.setFactory(conf);
    }
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
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
   * Test that a file which is open for write is overwritten by another
   * client. Regression test for HDFS-3755.
   */
  @Test
  public void testOverwriteOpenForWrite() throws Exception {
    Configuration conf = new HdfsConfiguration();
    SimulatedFSDataset.setFactory(conf);
    conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, false);
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    FileSystem fs = cluster.getFileSystem();
    
    UserGroupInformation otherUgi = UserGroupInformation.createUserForTesting(
        "testuser", new String[]{"testgroup"});
    FileSystem fs2 = otherUgi.doAs(new PrivilegedExceptionAction<FileSystem>() {
      @Override
      public FileSystem run() throws Exception {
        return FileSystem.get(cluster.getConfiguration(0));
      }
    });
    
    try {
      Path p = new Path("/testfile");
      FSDataOutputStream stm1 = fs.create(p);
      stm1.write(1);
      stm1.hflush();

      // Create file again without overwrite
      try {
        fs2.create(p, false);
        fail("Did not throw!");
      } catch (IOException abce) {
        GenericTestUtils.assertExceptionContains("already being created by",
            abce);
      }
      
      FSDataOutputStream stm2 = fs2.create(p, true);
      stm2.write(2);
      stm2.close();
      
      try {
        stm1.close();
        fail("Should have exception closing stm1 since it was deleted");
      } catch (IOException ioe) {
        GenericTestUtils.assertExceptionContains("File is not open for writing", ioe);
      }
      
    } finally {
      IOUtils.closeStream(fs);
      IOUtils.closeStream(fs2);
      cluster.shutdown();
    }
  }
  
  /**
   * Test that file data does not become corrupted even in the face of errors.
   */
  @Test
  public void testFileCreationError1() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1000);
    conf.setInt(DFS_HEARTBEAT_INTERVAL_KEY, 1);
    if (simulatedStorage) {
      SimulatedFSDataset.setFactory(conf);
    }
    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
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
            HdfsConstants.DatanodeReportType.LIVE);
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
  @Test
  public void testFileCreationError2() throws IOException {
    long leasePeriod = 1000;
    System.out.println("testFileCreationError2 start");
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1000);
    conf.setInt(DFS_HEARTBEAT_INTERVAL_KEY, 1);
    if (simulatedStorage) {
      SimulatedFSDataset.setFactory(conf);
    }
    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
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
          client.clientName, null, null, INodeId.GRANDFATHER_INODE_ID);
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

  /** test addBlock(..) when replication<min and excludeNodes==null. */
  @Test
  public void testFileCreationError3() throws IOException {
    System.out.println("testFileCreationError3 start");
    Configuration conf = new HdfsConfiguration();
    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
    DistributedFileSystem dfs = null;
    try {
      cluster.waitActive();
      dfs = (DistributedFileSystem)cluster.getFileSystem();
      DFSClient client = dfs.dfs;

      // create a new file.
      final Path f = new Path("/foo.txt");
      createFile(dfs, f, 3);
      try {
        cluster.getNameNodeRpc().addBlock(f.toString(), client.clientName,
            null, null, INodeId.GRANDFATHER_INODE_ID);
        fail();
      } catch(IOException ioe) {
        FileSystem.LOG.info("GOOD!", ioe);
      }

      System.out.println("testFileCreationError3 successful");
    } finally {
      IOUtils.closeStream(dfs);
      cluster.shutdown();
    }
  }

  /**
   * Test that file leases are persisted across namenode restarts.
   */
  @Test
  public void testFileCreationNamenodeRestart() throws IOException {
    Configuration conf = new HdfsConfiguration();
    final int MAX_IDLE_TIME = 2000; // 2s
    conf.setInt("ipc.client.connection.maxidletime", MAX_IDLE_TIME);
    conf.setInt(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1000);
    conf.setInt(DFS_HEARTBEAT_INTERVAL_KEY, 1);
    if (simulatedStorage) {
      SimulatedFSDataset.setFactory(conf);
    }

    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    DistributedFileSystem fs = null;
    try {
      cluster.waitActive();
      fs = cluster.getFileSystem();
      final int nnport = cluster.getNameNodePort();

      // create a new file.
      Path file1 = new Path("/filestatus.dat");
      HdfsDataOutputStream stm = create(fs, file1, 1);
      System.out.println("testFileCreationNamenodeRestart: "
                         + "Created file " + file1);
      assertEquals(file1 + " should be replicated to 1 datanode.", 1,
          stm.getCurrentBlockReplication());

      // write two full blocks.
      writeFile(stm, numBlocks * blockSize);
      stm.hflush();
      assertEquals(file1 + " should still be replicated to 1 datanode.", 1,
          stm.getCurrentBlockReplication());

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
      cluster = new MiniDFSCluster.Builder(conf).nameNodePort(nnport)
                                               .format(false)
                                               .build();
      cluster.waitActive();

      // restart cluster yet again. This triggers the code to read in
      // persistent leases from fsimage.
      cluster.shutdown();
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
      }
      cluster = new MiniDFSCluster.Builder(conf).nameNodePort(nnport)
                                                .format(false)
                                                .build();
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
  @Test
  public void testDFSClientDeath() throws IOException, InterruptedException {
    Configuration conf = new HdfsConfiguration();
    System.out.println("Testing adbornal client death.");
    if (simulatedStorage) {
      SimulatedFSDataset.setFactory(conf);
    }
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
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
  @Test
  public void testFileCreationNonRecursive() throws IOException {
    Configuration conf = new HdfsConfiguration();
    if (simulatedStorage) {
      SimulatedFSDataset.setFactory(conf);
    }
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    FileSystem fs = cluster.getFileSystem();
    final Path path = new Path("/" + Time.now()
        + "-testFileCreationNonRecursive");
    FSDataOutputStream out = null;

    try {
      IOException expectedException = null;
      final String nonExistDir = "/non-exist-" + Time.now();

      fs.delete(new Path(nonExistDir), true);
      EnumSet<CreateFlag> createFlag = EnumSet.of(CreateFlag.CREATE);
      // Create a new file in root dir, should succeed
      out = createNonRecursive(fs, path, 1, createFlag);
      out.close();
      // Create a file when parent dir exists as file, should fail
      try {
        createNonRecursive(fs, new Path(path, "Create"), 1, createFlag);
      } catch (IOException e) {
        expectedException = e;
      }
      assertTrue("Create a file when parent directory exists as a file"
          + " should throw ParentNotDirectoryException ",
          expectedException != null
              && expectedException instanceof ParentNotDirectoryException);
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

      EnumSet<CreateFlag> overwriteFlag = 
        EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE);
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
          + " should throw ParentNotDirectoryException ",
          expectedException != null
              && expectedException instanceof ParentNotDirectoryException);
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
            CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096), (short) repl,  blockSize, null);
    return stm;
  }
  

/**
 * Test that file data becomes available before file is closed.
 */
  @Test
  public void testFileCreationSimulated() throws IOException {
    simulatedStorage = true;
    testFileCreation();
    simulatedStorage = false;
  }

  /**
   * Test creating two files at the same time. 
   */
  @Test
  public void testConcurrentFileCreation() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();

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
   * Test creating a file whose data gets sync when closed
   */
  @Test
  public void testFileCreationSyncOnClose() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFS_DATANODE_SYNCONCLOSE_KEY, true);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();

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
  @Test
  public void testLeaseExpireHardLimit() throws Exception {
    System.out.println("testLeaseExpireHardLimit start");
    final long leasePeriod = 1000;
    final int DATANODE_NUM = 3;

    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1000);
    conf.setInt(DFS_HEARTBEAT_INTERVAL_KEY, 1);

    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(DATANODE_NUM).build();
    DistributedFileSystem dfs = null;
    try {
      cluster.waitActive();
      dfs = (DistributedFileSystem)cluster.getFileSystem();

      // create a new file.
      final String f = DIR + "foo";
      final Path fpath = new Path(f);
      HdfsDataOutputStream out = create(dfs, fpath, DATANODE_NUM);
      out.write("something".getBytes());
      out.hflush();
      int actualRepl = out.getCurrentBlockReplication();
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
        DataNode datanode = cluster.getDataNode(datanodeinfo.getIpcPort());
        ExtendedBlock blk = locatedblock.getBlock();
        Block b = DataNodeTestUtils.getFSDataset(datanode).getStoredBlock(
            blk.getBlockPoolId(), blk.getBlockId());
        final File blockfile = DataNodeTestUtils.getFile(datanode,
            blk.getBlockPoolId(), b.getBlockId());
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
  @Test
  public void testFsClose() throws Exception {
    System.out.println("test file system close start");
    final int DATANODE_NUM = 3;

    Configuration conf = new HdfsConfiguration();

    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(DATANODE_NUM).build();
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
  @Test
  public void testFsCloseAfterClusterShutdown() throws IOException {
    System.out.println("test testFsCloseAfterClusterShutdown start");
    final int DATANODE_NUM = 3;

    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFS_NAMENODE_REPLICATION_MIN_KEY, 3);
    conf.setBoolean("ipc.client.ping", false); // hdfs timeout is default 60 seconds
    conf.setInt("ipc.ping.interval", 10000); // hdfs timeout is now 10 second

    // create cluster
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(DATANODE_NUM).build();
    DistributedFileSystem dfs = null;
    try {
      cluster.waitActive();
      dfs = (DistributedFileSystem)cluster.getFileSystem();

      // create a new file.
      final String f = DIR + "testFsCloseAfterClusterShutdown";
      final Path fpath = new Path(f);
      FSDataOutputStream out = TestFileCreation.createFile(dfs, fpath, DATANODE_NUM);
      out.write("something_test".getBytes());
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

  /**
   * Regression test for HDFS-3626. Creates a file using a non-canonical path
   * (i.e. with extra slashes between components) and makes sure that the NN
   * can properly restart.
   * 
   * This test RPCs directly to the NN, to ensure that even an old client
   * which passes an invalid path won't cause corrupt edits.
   */
  @Test
  public void testCreateNonCanonicalPathAndRestartRpc() throws Exception {
    doCreateTest(CreationMethod.DIRECT_NN_RPC);
  }
  
  /**
   * Another regression test for HDFS-3626. This one creates files using
   * a Path instantiated from a string object.
   */
  @Test
  public void testCreateNonCanonicalPathAndRestartFromString()
      throws Exception {
    doCreateTest(CreationMethod.PATH_FROM_STRING);
  }

  /**
   * Another regression test for HDFS-3626. This one creates files using
   * a Path instantiated from a URI object.
   */
  @Test
  public void testCreateNonCanonicalPathAndRestartFromUri()
      throws Exception {
    doCreateTest(CreationMethod.PATH_FROM_URI);
  }
  
  private static enum CreationMethod {
    DIRECT_NN_RPC,
    PATH_FROM_URI,
    PATH_FROM_STRING
  };
  private void doCreateTest(CreationMethod method) throws Exception {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(1).build();
    try {
      FileSystem fs = cluster.getFileSystem();
      NamenodeProtocols nnrpc = cluster.getNameNodeRpc();

      for (String pathStr : NON_CANONICAL_PATHS) {
        System.out.println("Creating " + pathStr + " by " + method);
        switch (method) {
        case DIRECT_NN_RPC:
          try {
            nnrpc.create(pathStr, new FsPermission((short)0755), "client",
                new EnumSetWritable<CreateFlag>(EnumSet.of(CreateFlag.CREATE)),
                true, (short)1, 128*1024*1024L);
            fail("Should have thrown exception when creating '"
                + pathStr + "'" + " by " + method);
          } catch (InvalidPathException ipe) {
            // When we create by direct NN RPC, the NN just rejects the
            // non-canonical paths, rather than trying to normalize them.
            // So, we expect all of them to fail. 
          }
          break;
          
        case PATH_FROM_URI:
        case PATH_FROM_STRING:
          // Unlike the above direct-to-NN case, we expect these to succeed,
          // since the Path constructor should normalize the path.
          Path p;
          if (method == CreationMethod.PATH_FROM_URI) {
            p = new Path(new URI(fs.getUri() + pathStr));
          } else {
            p = new Path(fs.getUri() + pathStr);  
          }
          FSDataOutputStream stm = fs.create(p);
          IOUtils.closeStream(stm);
          break;
        default:
          throw new AssertionError("bad method: " + method);
        }
      }
      
      cluster.restartNameNode();

    } finally {
      cluster.shutdown();
    }
  }

}
