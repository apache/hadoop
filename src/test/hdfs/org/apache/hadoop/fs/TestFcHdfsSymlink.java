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
package org.apache.hadoop.fs;

import java.io.*;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.ipc.RemoteException;
import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.AfterClass;

/**
 * Test symbolic links using FileContext and Hdfs.
 */
public class TestFcHdfsSymlink extends FileContextSymlinkBaseTest {

  private static MiniDFSCluster cluster;
  
  protected String getScheme() {
    return "hdfs";
  }

  protected String testBaseDir1() {
    return "/test1";
  }
  
  protected String testBaseDir2() {
    return "/test2";
  }

  protected URI testURI() {
    return cluster.getURI();
  }

  @Override
  protected IOException unwrapException(IOException e) {
    if (e instanceof RemoteException) {
      return ((RemoteException)e).unwrapRemoteException();
    }
    return e;
  }

  @BeforeClass
  public static void testSetUp() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, true);
    conf.set(FsPermission.UMASK_LABEL, "000");
    cluster = new MiniDFSCluster(conf, 1, true, null);
    fc = FileContext.getFileContext(cluster.getURI());
  }
  
  @AfterClass
  public static void testTearDown() throws Exception {
    cluster.shutdown();
  }
     
  @Test
  /** Link from Hdfs to LocalFs */
  public void testLinkAcrossFileSystems() throws IOException {
    Path localDir  = new Path("file:///tmp/test");
    Path localFile = new Path("file:///tmp/test/file");
    Path link      = new Path(testBaseDir1(), "linkToFile");
    FileContext localFc = FileContext.getLocalFSFileContext();
    localFc.delete(new Path("file:///tmp/test"), true);
    localFc.mkdir(localDir, FileContext.DEFAULT_PERM, true);
    localFc.setWorkingDirectory(localDir);
    assertEquals(localDir, localFc.getWorkingDirectory());
    createAndWriteFile(localFc, localFile);
    fc.createSymlink(localFile, link, false);
    readFile(link);
    assertEquals(fileSize, fc.getFileStatus(link).getLen());
  }
  
  @Test
  /** Test access a symlink using AbstractFileSystem */
  public void testAccessLinkFromAbstractFileSystem() throws IOException {
    Path file = new Path(testBaseDir1(), "file");
    Path link = new Path(testBaseDir1(), "linkToFile");
    createAndWriteFile(file);
    fc.createSymlink(file, link, false);
    try {
      AbstractFileSystem afs = fc.getDefaultFileSystem();
      afs.open(link);
      fail("Opened a link using AFS");
    } catch (UnresolvedLinkException x) {
      // Expected
    }
  }

  @Test
  /** Test create symlink to / */
  public void testCreateLinkToSlash() throws IOException {
    Path dir  = new Path(testBaseDir1());
    Path file = new Path(testBaseDir1(), "file");
    Path link = new Path(testBaseDir1(), "linkToSlash");
    Path fileViaLink = new Path(testBaseDir1()+"/linkToSlash"+
                                testBaseDir1()+"/file");
    createAndWriteFile(file);
    fc.setWorkingDirectory(dir);
    fc.createSymlink(new Path("/"), link, false);
    readFile(fileViaLink);
    assertEquals(fileSize, fc.getFileStatus(fileViaLink).getLen());
    // Ditto when using another file context since the file system
    // for the slash is resolved according to the link's parent.
    FileContext localFc = FileContext.getLocalFSFileContext();
    Path linkQual = new Path(cluster.getURI().toString(), fileViaLink); 
    assertEquals(fileSize, localFc.getFileStatus(linkQual).getLen());    
  }
  
  
  @Test
  /** setPermission affects the target not the link */
  public void testSetPermissionAffectsTarget() throws IOException {    
    Path file       = new Path(testBaseDir1(), "file");
    Path dir        = new Path(testBaseDir2());
    Path linkToFile = new Path(testBaseDir1(), "linkToFile");
    Path linkToDir  = new Path(testBaseDir1(), "linkToDir");
    createAndWriteFile(file);
    fc.createSymlink(file, linkToFile, false);
    fc.createSymlink(dir, linkToDir, false);
    
    // Changing the permissions using the link does not modify
    // the permissions of the link..
    FsPermission perms = fc.getFileLinkStatus(linkToFile).getPermission();
    fc.setPermission(linkToFile, new FsPermission((short)0664));
    fc.setOwner(linkToFile, "user", "group");
    assertEquals(perms, fc.getFileLinkStatus(linkToFile).getPermission());
    // but the file's permissions were adjusted appropriately
    FileStatus stat = fc.getFileStatus(file);
    assertEquals(0664, stat.getPermission().toShort()); 
    assertEquals("user", stat.getOwner());
    assertEquals("group", stat.getGroup());
    // Getting the file's permissions via the link is the same
    // as getting the permissions directly.
    assertEquals(stat.getPermission(), 
                 fc.getFileStatus(linkToFile).getPermission());

    // Ditto for a link to a directory
    perms = fc.getFileLinkStatus(linkToDir).getPermission();
    fc.setPermission(linkToDir, new FsPermission((short)0664));
    fc.setOwner(linkToDir, "user", "group");
    assertEquals(perms, fc.getFileLinkStatus(linkToDir).getPermission());
    stat = fc.getFileStatus(dir);
    assertEquals(0664, stat.getPermission().toShort()); 
    assertEquals("user", stat.getOwner());
    assertEquals("group", stat.getGroup());
    assertEquals(stat.getPermission(), 
                 fc.getFileStatus(linkToDir).getPermission());
  }  

  @Test
  /** Create a symlink using a path with scheme but no authority */
  public void testCreateWithPartQualPathFails() throws IOException {
    Path fileWoAuth = new Path("hdfs:///test/file");
    Path linkWoAuth = new Path("hdfs:///test/link");
    try {
      createAndWriteFile(fileWoAuth);
      fail("HDFS requires URIs with schemes have an authority");
    } catch (RuntimeException e) {
      // Expected
    }
    try {
      fc.createSymlink(new Path("foo"), linkWoAuth, false);
      fail("HDFS requires URIs with schemes have an authority");
    } catch (RuntimeException e) {
      // Expected
    }
  }

  @Test
  /** setReplication affects the target not the link */  
  public void testSetReplication() throws IOException {
    Path file = new Path(testBaseDir1(), "file");
    Path link = new Path(testBaseDir1(), "linkToFile");
    createAndWriteFile(file);
    fc.createSymlink(file, link, false);
    fc.setReplication(link, (short)2);
    assertEquals(0, fc.getFileLinkStatus(link).getReplication());
    assertEquals(2, fc.getFileStatus(link).getReplication());      
    assertEquals(2, fc.getFileStatus(file).getReplication());
  }
  
  @Test
  /** Test create symlink with a max len name */
  public void testCreateLinkMaxPathLink() throws IOException {
    Path dir  = new Path(testBaseDir1());
    Path file = new Path(testBaseDir1(), "file");
    final int maxPathLen = FSConstants.MAX_PATH_LENGTH;
    final int dirLen     = dir.toString().length() + 1;
    int   len            = maxPathLen - dirLen;
    
    // Build a MAX_PATH_LENGTH path
    StringBuilder sb = new StringBuilder("");
    for (int i = 0; i < (len / 10); i++) {
      sb.append("0123456789");
    }
    for (int i = 0; i < (len % 10); i++) {
      sb.append("x");
    }
    Path link = new Path(sb.toString());
    assertEquals(maxPathLen, dirLen + link.toString().length()); 
    
    // Check that it works
    createAndWriteFile(file);
    fc.setWorkingDirectory(dir);
    fc.createSymlink(file, link, false);
    readFile(link);
    
    // Now modify the path so it's too large
    link = new Path(sb.toString()+"x");
    try {
      fc.createSymlink(file, link, false);
      fail("Path name should be too long");
    } catch (IOException x) {
      // Expected
    }
  } 
}