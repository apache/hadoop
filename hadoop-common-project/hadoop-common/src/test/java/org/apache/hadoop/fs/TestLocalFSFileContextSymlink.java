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
import java.net.URISyntaxException;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileUtil;
import static org.apache.hadoop.fs.FileContextTestHelper.*;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;
import org.junit.Test;
import org.junit.Before;

/**
 * Test symbolic links using FileContext and LocalFs.
 */
public class TestLocalFSFileContextSymlink extends FileContextSymlinkBaseTest {
  
  @Override
  protected String getScheme() {
    return "file";
  }

  @Override
  protected String testBaseDir1() throws IOException {
    return getAbsoluteTestRootDir(fc)+"/test1";
  }
  
  @Override
  protected String testBaseDir2() throws IOException {
    return getAbsoluteTestRootDir(fc)+"/test2";
  }

  @Override
  protected URI testURI() {
    try {
      return new URI("file:///");
    } catch (URISyntaxException e) {
      return null;
    }
  }
  
  @Override
  @Before
  public void setUp() throws Exception {
    fc = FileContext.getLocalFSFileContext();
    super.setUp();
  }

  @Override
  public void testCreateDanglingLink() throws IOException {
    // Dangling symlinks are not supported on Windows local file system.
    assumeTrue(!Path.WINDOWS);
    super.testCreateDanglingLink();
  }

  @Override
  public void testCreateFileViaDanglingLinkParent() throws IOException {
    assumeTrue(!Path.WINDOWS);
    super.testCreateFileViaDanglingLinkParent();
  }

  @Override
  public void testOpenResolvesLinks() throws IOException {
    assumeTrue(!Path.WINDOWS);
    super.testOpenResolvesLinks();
  }

  @Override
  public void testRecursiveLinks() throws IOException {
    assumeTrue(!Path.WINDOWS);
    super.testRecursiveLinks();
  }

  @Override
  public void testRenameDirToDanglingSymlink() throws IOException {
    assumeTrue(!Path.WINDOWS);
    super.testRenameDirToDanglingSymlink();
  }

  @Override  
  public void testStatDanglingLink() throws IOException {
    assumeTrue(!Path.WINDOWS);
    super.testStatDanglingLink();
  }

  @Test
  /** lstat a non-existant file using a partially qualified path */
  public void testDanglingLinkFilePartQual() throws IOException {
    Path filePartQual = new Path(getScheme()+":///doesNotExist");
    try {
      fc.getFileLinkStatus(filePartQual);
      fail("Got FileStatus for non-existant file");
    } catch (FileNotFoundException f) {
      // Expected
    }
    try {
      fc.getLinkTarget(filePartQual);
      fail("Got link target for non-existant file");      
    } catch (FileNotFoundException f) {
      // Expected
    }
  }
  
  @Test
  /** Stat and lstat a dangling link */
  public void testDanglingLink() throws IOException {
    assumeTrue(!Path.WINDOWS);
    Path fileAbs  = new Path(testBaseDir1()+"/file");    
    Path fileQual = new Path(testURI().toString(), fileAbs);    
    Path link     = new Path(testBaseDir1()+"/linkToFile");
    fc.createSymlink(fileAbs, link, false);
    // Deleting the link using FileContext currently fails because
    // resolve looks up LocalFs rather than RawLocalFs for the path 
    // so we call ChecksumFs delete (which doesn't delete dangling 
    // links) instead of delegating to delete in RawLocalFileSystem 
    // which deletes via fullyDelete. testDeleteLink above works 
    // because the link is not dangling.
    //assertTrue(fc.delete(link, false));
    FileUtil.fullyDelete(new File(link.toUri().getPath()));
    fc.createSymlink(fileAbs, link, false);
    try {
      fc.getFileStatus(link);
      fail("Got FileStatus for dangling link");
    } catch (FileNotFoundException f) {
      // Expected. File's exists method returns false for dangling links
    }
    // We can stat a dangling link
    FileStatus fsd = fc.getFileLinkStatus(link);
    assertEquals(fileQual, fsd.getSymlink());
    assertTrue(fsd.isSymlink());
    assertFalse(fsd.isDirectory());
    assertEquals("", fsd.getOwner());
    assertEquals("", fsd.getGroup());
    assertEquals(link, fsd.getPath());
    assertEquals(0, fsd.getLen());
    assertEquals(0, fsd.getBlockSize());
    assertEquals(0, fsd.getReplication());
    assertEquals(0, fsd.getAccessTime());
    assertEquals(FsPermission.getDefault(), fsd.getPermission());
    // Accessing the link 
    try {
      readFile(link);
      fail("Got FileStatus for dangling link");
    } catch (FileNotFoundException f) {
      // Ditto.
    }
    // Creating the file makes the link work
    createAndWriteFile(fileAbs);
    fc.getFileStatus(link);
  }

  @Test
  /** 
   * Test getLinkTarget with a partially qualified target. 
   * NB: Hadoop does not support fully qualified URIs for the 
   * file scheme (eg file://host/tmp/test).
   */  
  public void testGetLinkStatusPartQualTarget() throws IOException {
    Path fileAbs  = new Path(testBaseDir1()+"/file");
    Path fileQual = new Path(testURI().toString(), fileAbs);
    Path dir      = new Path(testBaseDir1());    
    Path link     = new Path(testBaseDir1()+"/linkToFile");
    Path dirNew   = new Path(testBaseDir2());
    Path linkNew  = new Path(testBaseDir2()+"/linkToFile");
    fc.delete(dirNew, true);
    createAndWriteFile(fileQual);
    fc.setWorkingDirectory(dir);
    // Link target is partially qualified, we get the same back.
    fc.createSymlink(fileQual, link, false);
    assertEquals(fileQual, fc.getFileLinkStatus(link).getSymlink());
    // Because the target was specified with an absolute path the
    // link fails to resolve after moving the parent directory. 
    fc.rename(dir, dirNew);
    // The target is still the old path
    assertEquals(fileQual, fc.getFileLinkStatus(linkNew).getSymlink());    
    try {
      readFile(linkNew);
      fail("The link should be dangling now.");
    } catch (FileNotFoundException x) {
      // Expected.
    }
    // RawLocalFs only maintains the path part, not the URI, and
    // therefore does not support links to other file systems.
    Path anotherFs = new Path("hdfs://host:1000/dir/file");
    FileUtil.fullyDelete(new File(linkNew.toString()));
    try {
      fc.createSymlink(anotherFs, linkNew, false);
      fail("Created a local fs link to a non-local fs");
    } catch (IOException x) {
      // Excpected.
    }
  }
}
