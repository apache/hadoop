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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.web.HftpFileSystem;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This class tests the FileStatus API.
 */
public class TestFileStatus {
  {
    ((Log4JLogger)LogFactory.getLog(FSNamesystem.class)).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)FileSystem.LOG).getLogger().setLevel(Level.ALL);
  }

  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;
  static final int fileSize = 16384;

  private static Configuration conf;
  private static MiniDFSCluster cluster;
  private static FileSystem fs;
  private static FileContext fc;
  private static HftpFileSystem hftpfs; 
  private static DFSClient dfsClient;
  private static Path file1;
  
  @BeforeClass
  public static void testSetUp() throws Exception {
    conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_LIST_LIMIT, 2);
    cluster = new MiniDFSCluster.Builder(conf).build();
    fs = cluster.getFileSystem();
    fc = FileContext.getFileContext(cluster.getURI(0), conf);
    hftpfs = cluster.getHftpFileSystem(0);
    dfsClient = new DFSClient(NameNode.getAddress(conf), conf);
    file1 = new Path("filestatus.dat");
    writeFile(fs, file1, 1, fileSize, blockSize);
  }
  
  @AfterClass
  public static void testTearDown() throws Exception {
    fs.close();
    cluster.shutdown();
  }

  private static void writeFile(FileSystem fileSys, Path name, int repl,
      int fileSize, int blockSize) throws IOException {
    // Create and write a file that contains three blocks of data
    FSDataOutputStream stm = fileSys.create(name, true,
        HdfsConstants.IO_FILE_BUFFER_SIZE, (short)repl, (long)blockSize);
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
  }
  
  private void checkFile(FileSystem fileSys, Path name, int repl)
      throws IOException, InterruptedException, TimeoutException {
    DFSTestUtil.waitReplication(fileSys, name, (short) repl);
  }
  
  /** Test calling getFileInfo directly on the client */
  @Test
  public void testGetFileInfo() throws IOException {
    // Check that / exists
    Path path = new Path("/");
    assertTrue("/ should be a directory", 
               fs.getFileStatus(path).isDirectory());
    
    // Make sure getFileInfo returns null for files which do not exist
    HdfsFileStatus fileInfo = dfsClient.getFileInfo("/noSuchFile");
    assertEquals("Non-existant file should result in null", null, fileInfo);
    
    Path path1 = new Path("/name1");
    Path path2 = new Path("/name1/name2");
    assertTrue(fs.mkdirs(path1));
    FSDataOutputStream out = fs.create(path2, false);
    out.close();
    fileInfo = dfsClient.getFileInfo(path1.toString());
    assertEquals(1, fileInfo.getChildrenNum());
    fileInfo = dfsClient.getFileInfo(path2.toString());
    assertEquals(0, fileInfo.getChildrenNum());

    // Test getFileInfo throws the right exception given a non-absolute path.
    try {
      dfsClient.getFileInfo("non-absolute");
      fail("getFileInfo for a non-absolute path did not throw IOException");
    } catch (RemoteException re) {
      assertTrue("Wrong exception for invalid file name", 
          re.toString().contains("Invalid file name"));
    }
  }


  /** Test the FileStatus obtained calling getFileStatus on a file */  
  @Test
  public void testGetFileStatusOnFile() throws Exception {
    checkFile(fs, file1, 1);
    // test getFileStatus on a file
    FileStatus status = fs.getFileStatus(file1);
    assertFalse(file1 + " should be a file", status.isDirectory());
    assertEquals(blockSize, status.getBlockSize());
    assertEquals(1, status.getReplication());
    assertEquals(fileSize, status.getLen());
    assertEquals(file1.makeQualified(fs.getUri(), 
        fs.getWorkingDirectory()).toString(), 
        status.getPath().toString());
  }

  /** Test the FileStatus obtained calling listStatus on a file */
  @Test
  public void testListStatusOnFile() throws IOException {
    FileStatus[] stats = fs.listStatus(file1);
    assertEquals(1, stats.length);
    FileStatus status = stats[0];
    assertFalse(file1 + " should be a file", status.isDirectory());
    assertEquals(blockSize, status.getBlockSize());
    assertEquals(1, status.getReplication());
    assertEquals(fileSize, status.getLen());
    assertEquals(file1.makeQualified(fs.getUri(), 
        fs.getWorkingDirectory()).toString(), 
        status.getPath().toString());
    
    RemoteIterator<FileStatus> itor = fc.listStatus(file1);
    status = itor.next();
    assertEquals(stats[0], status);
    assertFalse(file1 + " should be a file", status.isDirectory());
  }

  /** Test getting a FileStatus object using a non-existant path */
  @Test
  public void testGetFileStatusOnNonExistantFileDir() throws IOException {
    Path dir = new Path("/test/mkdirs");
    try {
      fs.listStatus(dir);
      fail("listStatus of non-existent path should fail");
    } catch (FileNotFoundException fe) {
      assertEquals("File " + dir + " does not exist.",fe.getMessage());
    }
    
    try {
      fc.listStatus(dir);
      fail("listStatus of non-existent path should fail");
    } catch (FileNotFoundException fe) {
      assertEquals("File " + dir + " does not exist.", fe.getMessage());
    }
    try {
      fs.getFileStatus(dir);
      fail("getFileStatus of non-existent path should fail");
    } catch (FileNotFoundException fe) {
      assertTrue("Exception doesn't indicate non-existant path", 
          fe.getMessage().startsWith("File does not exist"));
    }
  }

  /** Test FileStatus objects obtained from a directory */
  @Test
  public void testGetFileStatusOnDir() throws Exception {
    // Create the directory
    Path dir = new Path("/test/mkdirs");
    assertTrue("mkdir failed", fs.mkdirs(dir));
    assertTrue("mkdir failed", fs.exists(dir));
    
    // test getFileStatus on an empty directory
    FileStatus status = fs.getFileStatus(dir);
    assertTrue(dir + " should be a directory", status.isDirectory());
    assertTrue(dir + " should be zero size ", status.getLen() == 0);
    assertEquals(dir.makeQualified(fs.getUri(), 
        fs.getWorkingDirectory()).toString(), 
        status.getPath().toString());
    
    // test listStatus on an empty directory
    FileStatus[] stats = fs.listStatus(dir);
    assertEquals(dir + " should be empty", 0, stats.length);
    assertEquals(dir + " should be zero size ",
        0, fs.getContentSummary(dir).getLength());
    assertEquals(dir + " should be zero size using hftp",
        0, hftpfs.getContentSummary(dir).getLength());
    
    RemoteIterator<FileStatus> itor = fc.listStatus(dir);
    assertFalse(dir + " should be empty", itor.hasNext());

    itor = fs.listStatusIterator(dir);
    assertFalse(dir + " should be empty", itor.hasNext());

    // create another file that is smaller than a block.
    Path file2 = new Path(dir, "filestatus2.dat");
    writeFile(fs, file2, 1, blockSize/4, blockSize);
    checkFile(fs, file2, 1);
    
    // verify file attributes
    status = fs.getFileStatus(file2);
    assertEquals(blockSize, status.getBlockSize());
    assertEquals(1, status.getReplication());
    file2 = fs.makeQualified(file2);
    assertEquals(file2.toString(), status.getPath().toString());

    // Create another file in the same directory
    Path file3 = new Path(dir, "filestatus3.dat");
    writeFile(fs, file3, 1, blockSize/4, blockSize);
    checkFile(fs, file3, 1);
    file3 = fs.makeQualified(file3);

    // Verify that the size of the directory increased by the size 
    // of the two files
    final int expected = blockSize/2;  
    assertEquals(dir + " size should be " + expected, 
        expected, fs.getContentSummary(dir).getLength());
    assertEquals(dir + " size should be " + expected + " using hftp", 
        expected, hftpfs.getContentSummary(dir).getLength());
    
    // Test listStatus on a non-empty directory
    stats = fs.listStatus(dir);
    assertEquals(dir + " should have two entries", 2, stats.length);
    assertEquals(file2.toString(), stats[0].getPath().toString());
    assertEquals(file3.toString(), stats[1].getPath().toString());

    itor = fc.listStatus(dir);
    assertEquals(file2.toString(), itor.next().getPath().toString());
    assertEquals(file3.toString(), itor.next().getPath().toString());
    assertFalse("Unexpected addtional file", itor.hasNext());

    itor = fs.listStatusIterator(dir);
    assertEquals(file2.toString(), itor.next().getPath().toString());
    assertEquals(file3.toString(), itor.next().getPath().toString());
    assertFalse("Unexpected addtional file", itor.hasNext());


    // Test iterative listing. Now dir has 2 entries, create one more.
    Path dir3 = fs.makeQualified(new Path(dir, "dir3"));
    fs.mkdirs(dir3);
    dir3 = fs.makeQualified(dir3);
    stats = fs.listStatus(dir);
    assertEquals(dir + " should have three entries", 3, stats.length);
    assertEquals(dir3.toString(), stats[0].getPath().toString());
    assertEquals(file2.toString(), stats[1].getPath().toString());
    assertEquals(file3.toString(), stats[2].getPath().toString());

    itor = fc.listStatus(dir);
    assertEquals(dir3.toString(), itor.next().getPath().toString());
    assertEquals(file2.toString(), itor.next().getPath().toString());
    assertEquals(file3.toString(), itor.next().getPath().toString());
    assertFalse("Unexpected addtional file", itor.hasNext());

    itor = fs.listStatusIterator(dir);
    assertEquals(dir3.toString(), itor.next().getPath().toString());
    assertEquals(file2.toString(), itor.next().getPath().toString());
    assertEquals(file3.toString(), itor.next().getPath().toString());
    assertFalse("Unexpected addtional file", itor.hasNext());

    // Now dir has 3 entries, create two more
    Path dir4 = fs.makeQualified(new Path(dir, "dir4"));
    fs.mkdirs(dir4);
    dir4 = fs.makeQualified(dir4);
    Path dir5 = fs.makeQualified(new Path(dir, "dir5"));
    fs.mkdirs(dir5);
    dir5 = fs.makeQualified(dir5);
    stats = fs.listStatus(dir);
    assertEquals(dir + " should have five entries", 5, stats.length);
    assertEquals(dir3.toString(), stats[0].getPath().toString());
    assertEquals(dir4.toString(), stats[1].getPath().toString());
    assertEquals(dir5.toString(), stats[2].getPath().toString());
    assertEquals(file2.toString(), stats[3].getPath().toString());
    assertEquals(file3.toString(), stats[4].getPath().toString());
    
    itor = fc.listStatus(dir);
    assertEquals(dir3.toString(), itor.next().getPath().toString());
    assertEquals(dir4.toString(), itor.next().getPath().toString());
    assertEquals(dir5.toString(), itor.next().getPath().toString());
    assertEquals(file2.toString(), itor.next().getPath().toString());
    assertEquals(file3.toString(), itor.next().getPath().toString());
    assertFalse(itor.hasNext());      

    itor = fs.listStatusIterator(dir);
    assertEquals(dir3.toString(), itor.next().getPath().toString());
    assertEquals(dir4.toString(), itor.next().getPath().toString());
    assertEquals(dir5.toString(), itor.next().getPath().toString());
    assertEquals(file2.toString(), itor.next().getPath().toString());
    assertEquals(file3.toString(), itor.next().getPath().toString());
    assertFalse(itor.hasNext());

    { //test permission error on hftp 
      fs.setPermission(dir, new FsPermission((short)0));
      try {
        final String username = UserGroupInformation.getCurrentUser().getShortUserName() + "1";
        final HftpFileSystem hftp2 = cluster.getHftpFileSystemAs(username, conf, 0, "somegroup");
        hftp2.getContentSummary(dir);
        fail();
      } catch(IOException ioe) {
        FileSystem.LOG.info("GOOD: getting an exception", ioe);
      }
    }
    fs.delete(dir, true);
  }
}
