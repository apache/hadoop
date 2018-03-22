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

import static org.apache.hadoop.test.PlatformAssumptions.assumeNotWindows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.util.Shell;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

/**
 * Test symbolic links using LocalFs.
 */
abstract public class TestSymlinkLocalFS extends SymlinkBaseTest {

  // Workaround for HADOOP-9652
  static {
    RawLocalFileSystem.useStatIfAvailable();
  }
  
  @Override
  protected String getScheme() {
    return "file";
  }

  @Override
  protected String testBaseDir1() throws IOException {
    return wrapper.getAbsoluteTestRootDir()+"/test1";
  }
  
  @Override
  protected String testBaseDir2() throws IOException {
    return wrapper.getAbsoluteTestRootDir()+"/test2";
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
  public void testCreateDanglingLink() throws IOException {
    // Dangling symlinks are not supported on Windows local file system.
    assumeNotWindows();
    super.testCreateDanglingLink();
  }

  @Override
  public void testCreateFileViaDanglingLinkParent() throws IOException {
    assumeNotWindows();
    super.testCreateFileViaDanglingLinkParent();
  }

  @Override
  public void testOpenResolvesLinks() throws IOException {
    assumeNotWindows();
    super.testOpenResolvesLinks();
  }

  @Override
  public void testRecursiveLinks() throws IOException {
    assumeNotWindows();
    super.testRecursiveLinks();
  }

  @Override
  public void testRenameDirToDanglingSymlink() throws IOException {
    assumeNotWindows();
    super.testRenameDirToDanglingSymlink();
  }

  @Override  
  public void testStatDanglingLink() throws IOException {
    assumeNotWindows();
    super.testStatDanglingLink();
  }

  @Test(timeout=10000)
  /** lstat a non-existant file using a partially qualified path */
  public void testDanglingLinkFilePartQual() throws IOException {
    Path filePartQual = new Path(getScheme()+":///doesNotExist");
    try {
      wrapper.getFileLinkStatus(filePartQual);
      fail("Got FileStatus for non-existant file");
    } catch (FileNotFoundException f) {
      // Expected
    }
    try {
      wrapper.getLinkTarget(filePartQual);
      fail("Got link target for non-existant file");
    } catch (FileNotFoundException f) {
      // Expected
    }
  }
  
  @Test(timeout=10000)
  /** Stat and lstat a dangling link */
  public void testDanglingLink() throws IOException {
    assumeNotWindows();
    Path fileAbs  = new Path(testBaseDir1()+"/file");
    Path fileQual = new Path(testURI().toString(), fileAbs);
    Path link     = new Path(testBaseDir1()+"/linkToFile");
    Path linkQual = new Path(testURI().toString(), link.toString());
    wrapper.createSymlink(fileAbs, link, false);
    // Deleting the link using FileContext currently fails because
    // resolve looks up LocalFs rather than RawLocalFs for the path 
    // so we call ChecksumFs delete (which doesn't delete dangling 
    // links) instead of delegating to delete in RawLocalFileSystem 
    // which deletes via fullyDelete. testDeleteLink above works 
    // because the link is not dangling.
    //assertTrue(fc.delete(link, false));
    FileUtil.fullyDelete(new File(link.toUri().getPath()));
    wrapper.createSymlink(fileAbs, link, false);
    try {
      wrapper.getFileStatus(link);
      fail("Got FileStatus for dangling link");
    } catch (FileNotFoundException f) {
      // Expected. File's exists method returns false for dangling links
    }
    // We can stat a dangling link
    UserGroupInformation user = UserGroupInformation.getCurrentUser();
    FileStatus fsd = wrapper.getFileLinkStatus(link);
    assertEquals(fileQual, fsd.getSymlink());
    assertTrue(fsd.isSymlink());
    assertFalse(fsd.isDirectory());
    assertEquals(user.getUserName(), fsd.getOwner());
    // Compare against user's primary group
    assertEquals(user.getGroupNames()[0], fsd.getGroup());
    assertEquals(linkQual, fsd.getPath());
    // Accessing the link 
    try {
      readFile(link);
      fail("Got FileStatus for dangling link");
    } catch (FileNotFoundException f) {
      // Ditto.
    }
    // Creating the file makes the link work
    createAndWriteFile(fileAbs);
    wrapper.getFileStatus(link);
  }

  @Test(timeout=10000)
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
    wrapper.delete(dirNew, true);
    createAndWriteFile(fileQual);
    wrapper.setWorkingDirectory(dir);
    // Link target is partially qualified, we get the same back.
    wrapper.createSymlink(fileQual, link, false);
    assertEquals(fileQual, wrapper.getFileLinkStatus(link).getSymlink());
    // Because the target was specified with an absolute path the
    // link fails to resolve after moving the parent directory. 
    wrapper.rename(dir, dirNew);
    // The target is still the old path
    assertEquals(fileQual, wrapper.getFileLinkStatus(linkNew).getSymlink());    
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
      wrapper.createSymlink(anotherFs, linkNew, false);
      fail("Created a local fs link to a non-local fs");
    } catch (IOException x) {
      // Excpected.
    }
  }

  /** Test create symlink to . */
  @Override
  public void testCreateLinkToDot() throws IOException {
    try {
      super.testCreateLinkToDot();
    } catch (IllegalArgumentException iae) {
      // Expected.
    }
  }

  @Override
  public void testSetTimesSymlinkToFile() throws IOException {
    assumeTrue(!Shell.WINDOWS && !Shell.SOLARIS);
    super.testSetTimesSymlinkToFile();
  }

  @Override
  public void testSetTimesSymlinkToDir() throws IOException {
    assumeTrue(!Path.WINDOWS && !Shell.SOLARIS);
    super.testSetTimesSymlinkToDir();
  }

  @Override
  public void testSetTimesDanglingLink() throws IOException {
    assumeNotWindows();
    super.testSetTimesDanglingLink();
  }
}
