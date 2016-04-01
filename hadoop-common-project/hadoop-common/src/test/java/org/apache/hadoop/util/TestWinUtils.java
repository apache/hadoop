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

package org.apache.hadoop.util;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;
import static org.junit.matchers.JUnitMatchers.containsString;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;

/**
 * Test cases for helper Windows winutils.exe utility.
 */
public class TestWinUtils {

  private static final Log LOG = LogFactory.getLog(TestWinUtils.class);
  private static File TEST_DIR = GenericTestUtils.getTestDir(
      TestWinUtils.class.getSimpleName());

  String winutils;

  @Before
  public void setUp() throws IOException {
    // Not supported on non-Windows platforms
    assumeTrue(Shell.WINDOWS);
    TEST_DIR.mkdirs();
    assertTrue("Failed to create Test directory " + TEST_DIR,
        TEST_DIR.isDirectory() );
    winutils = Shell.getWinUtilsPath();
  }

  @After
  public void tearDown() throws IOException {
    FileUtil.fullyDelete(TEST_DIR);
  }

  private void requireWinutils() throws IOException {
    Shell.getWinUtilsPath();
  }

  // Helper routine that writes the given content to the file.
  private void writeFile(File file, String content) throws IOException {
    byte[] data = content.getBytes();
    try (FileOutputStream os = new FileOutputStream(file)) {
      os.write(data);
      os.close();
    }
  }

  // Helper routine that reads the first 100 bytes from the file.
  private String readFile(File file) throws IOException {
    byte[] b;
    try (FileInputStream fos = new FileInputStream(file)) {
      b = new byte[100];
      int count = fos.read(b);
      assertEquals(100, count);
    }
    return new String(b);
  }

  @Test (timeout = 30000)
  public void testLs() throws IOException {
    requireWinutils();
    final String content = "6bytes";
    final int contentSize = content.length();
    File testFile = new File(TEST_DIR, "file1");
    writeFile(testFile, content);

    // Verify permissions and file name return tokens
    String testPath = testFile.getCanonicalPath();
    String output = Shell.execCommand(
        winutils, "ls", testPath);
    String[] outputArgs = output.split("[ \r\n]");
    assertEquals("-rwx------", outputArgs[0]);
    assertEquals(outputArgs[outputArgs.length - 1], testPath);

    // Verify most tokens when using a formatted output (other tokens
    // will be verified with chmod/chown)
    output = Shell.execCommand(
        winutils, "ls", "-F", testPath);
    outputArgs = output.split("[|\r\n]");
    assertEquals(9, outputArgs.length);
    assertEquals("-rwx------", outputArgs[0]);
    assertEquals(contentSize, Long.parseLong(outputArgs[4]));
    assertEquals(outputArgs[8], testPath);

    testFile.delete();
    assertFalse(testFile.exists());
  }

  @Test (timeout = 30000)
  public void testGroups() throws IOException {
    requireWinutils();
    String currentUser = System.getProperty("user.name");

    // Verify that groups command returns information about the current user
    // groups when invoked with no args
    String outputNoArgs = Shell.execCommand(
        winutils, "groups").trim();
    String output = Shell.execCommand(
        winutils, "groups", currentUser).trim();
    assertEquals(output, outputNoArgs);

    // Verify that groups command with the -F flag returns the same information
    String outputFormat = Shell.execCommand(
        winutils, "groups", "-F", currentUser).trim();
    outputFormat = outputFormat.replace("|", " ");
    assertEquals(output, outputFormat);
  }

  private void chmod(String mask, File file) throws IOException {
    Shell.execCommand(
        winutils, "chmod", mask, file.getCanonicalPath());
  }

  private void chmodR(String mask, File file) throws IOException {
    Shell.execCommand(
        winutils, "chmod", "-R", mask, file.getCanonicalPath());
  }

  private String ls(File file) throws IOException {
    return Shell.execCommand(
        winutils, "ls", file.getCanonicalPath());
  }

  private String lsF(File file) throws IOException {
    return Shell.execCommand(
        winutils, "ls", "-F", file.getCanonicalPath());
  }

  private void assertPermissions(File file, String expected)
      throws IOException {
    String output = ls(file).split("[ \r\n]")[0];
    assertEquals(expected, output);
  }

  private void testChmodInternal(String mode, String expectedPerm)
      throws IOException {
    requireWinutils();
    File a = new File(TEST_DIR, "file1");
    assertTrue(a.createNewFile());

    // Reset permissions on the file to default
    chmod("700", a);

    // Apply the mode mask
    chmod(mode, a);

    // Compare the output
    assertPermissions(a, expectedPerm);

    a.delete();
    assertFalse(a.exists());
  }

  private void testNewFileChmodInternal(String expectedPerm) throws IOException {
    requireWinutils();
    // Create a new directory
    File dir = new File(TEST_DIR, "dir1");

    assertTrue(dir.mkdir());

    // Set permission use chmod
    chmod("755", dir);

    // Create a child file in the directory
    File child = new File(dir, "file1");
    assertTrue(child.createNewFile());

    // Verify the child file has correct permissions
    assertPermissions(child, expectedPerm);

    child.delete();
    dir.delete();
    assertFalse(dir.exists());
  }

  private void testChmodInternalR(String mode, String expectedPerm,
      String expectedPermx) throws IOException {
    requireWinutils();
    // Setup test folder hierarchy
    File a = new File(TEST_DIR, "a");
    assertTrue(a.mkdir());
    chmod("700", a);
    File aa = new File(a, "a");
    assertTrue(aa.createNewFile());
    chmod("600", aa);
    File ab = new File(a, "b");
    assertTrue(ab.mkdir());
    chmod("700", ab);
    File aba = new File(ab, "a");
    assertTrue(aba.mkdir());
    chmod("700", aba);
    File abb = new File(ab, "b");
    assertTrue(abb.createNewFile());
    chmod("600", abb);
    File abx = new File(ab, "x");
    assertTrue(abx.createNewFile());
    chmod("u+x", abx);

    // Run chmod recursive
    chmodR(mode, a);

    // Verify outcome
    assertPermissions(a, "d" + expectedPermx);
    assertPermissions(aa, "-" + expectedPerm);
    assertPermissions(ab, "d" + expectedPermx);
    assertPermissions(aba, "d" + expectedPermx);
    assertPermissions(abb, "-" + expectedPerm);
    assertPermissions(abx, "-" + expectedPermx);

    assertTrue(FileUtil.fullyDelete(a));
  }

  @Test (timeout = 30000)
  public void testBasicChmod() throws IOException {
    requireWinutils();
    // - Create a file.
    // - Change mode to 377 so owner does not have read permission.
    // - Verify the owner truly does not have the permissions to read.
    File a = new File(TEST_DIR, "a");
    a.createNewFile();
    chmod("377", a);

    try {
      readFile(a);
      assertFalse("readFile should have failed!", true);
    } catch (IOException ex) {
      LOG.info("Expected: Failed read from a file with permissions 377");
    }
    // restore permissions
    chmod("700", a);

    // - Create a file.
    // - Change mode to 577 so owner does not have write permission.
    // - Verify the owner truly does not have the permissions to write.
    chmod("577", a);
 
    try {
      writeFile(a, "test");
      fail("writeFile should have failed!");
    } catch (IOException ex) {
      LOG.info("Expected: Failed write to a file with permissions 577");
    }
    // restore permissions
    chmod("700", a);
    assertTrue(a.delete());

    // - Copy WINUTILS to a new executable file, a.exe.
    // - Change mode to 677 so owner does not have execute permission.
    // - Verify the owner truly does not have the permissions to execute the file.

    File winutilsFile = Shell.getWinUtilsFile();
    File aExe = new File(TEST_DIR, "a.exe");
    FileUtils.copyFile(winutilsFile, aExe);
    chmod("677", aExe);

    try {
      Shell.execCommand(aExe.getCanonicalPath(), "ls");
      fail("executing " + aExe + " should have failed!");
    } catch (IOException ex) {
      LOG.info("Expected: Failed to execute a file with permissions 677");
    }
    assertTrue(aExe.delete());
  }

  /** Validate behavior of chmod commands on directories on Windows. */
  @Test (timeout = 30000)
  public void testBasicChmodOnDir() throws IOException {
    requireWinutils();
    // Validate that listing a directory with no read permission fails
    File a = new File(TEST_DIR, "a");
    File b = new File(a, "b");
    a.mkdirs();
    assertTrue(b.createNewFile());

    // Remove read permissions on directory a
    chmod("300", a);
    String[] files = a.list();
    assertNull("Listing a directory without read permission should fail", files);

    // restore permissions
    chmod("700", a);
    // validate that the directory can be listed now
    files = a.list();
    assertEquals("b", files[0]);

    // Remove write permissions on the directory and validate the
    // behavior for adding, deleting and renaming files
    chmod("500", a);
    File c = new File(a, "c");
 
    try {
      // Adding a new file will fail as expected because the
      // FILE_WRITE_DATA/FILE_ADD_FILE privilege is denied on
      // the dir.
      c.createNewFile();
      fail("writeFile should have failed!");
    } catch (IOException ex) {
      LOG.info("Expected: Failed to create a file when directory "
          + "permissions are 577");
    }

    // Deleting a file will succeed even if write permissions are not present
    // on the parent dir. Check the following link for additional details:
    // http://support.microsoft.com/kb/238018
    assertTrue("Special behavior: deleting a file will succeed on Windows "
        + "even if a user does not have write permissions on the parent dir",
        b.delete());

    assertFalse("Renaming a file should fail on the dir where a user does "
        + "not have write permissions", b.renameTo(new File(a, "d")));

    // restore permissions
    chmod("700", a);

    // Make sure adding new files and rename succeeds now
    assertTrue(c.createNewFile());
    File d = new File(a, "d");
    assertTrue(c.renameTo(d));
    // at this point in the test, d is the only remaining file in directory a

    // Removing execute permissions does not have the same behavior on
    // Windows as on Linux. Adding, renaming, deleting and listing files
    // will still succeed. Windows default behavior is to bypass directory
    // traverse checking (BYPASS_TRAVERSE_CHECKING privilege) for all users.
    // See the following link for additional details:
    // http://msdn.microsoft.com/en-us/library/windows/desktop/aa364399(v=vs.85).aspx
    chmod("600", a);

    // validate directory listing
    files = a.list();
    assertEquals("d", files[0]);
    // validate delete
    assertTrue(d.delete());
    // validate add
    File e = new File(a, "e");
    assertTrue(e.createNewFile());
    // validate rename
    assertTrue(e.renameTo(new File(a, "f")));

    // restore permissions
    chmod("700", a);
  }

  @Test (timeout = 30000)
  public void testChmod() throws IOException {
    requireWinutils();
    testChmodInternal("7", "-------rwx");
    testChmodInternal("70", "----rwx---");
    testChmodInternal("u-x,g+r,o=g", "-rw-r--r--");
    testChmodInternal("u-x,g+rw", "-rw-rw----");
    testChmodInternal("u-x,g+rwx-x,o=u", "-rw-rw-rw-");
    testChmodInternal("+", "-rwx------");

    // Recursive chmod tests
    testChmodInternalR("755", "rwxr-xr-x", "rwxr-xr-x");
    testChmodInternalR("u-x,g+r,o=g", "rw-r--r--", "rw-r--r--");
    testChmodInternalR("u-x,g+rw", "rw-rw----", "rw-rw----");
    testChmodInternalR("u-x,g+rwx-x,o=u", "rw-rw-rw-", "rw-rw-rw-");
    testChmodInternalR("a+rX", "rw-r--r--", "rwxr-xr-x");

    // Test a new file created in a chmod'ed directory has expected permission
    testNewFileChmodInternal("-rwxr-xr-x");
  }

  private void chown(String userGroup, File file) throws IOException {
    Shell.execCommand(
        winutils, "chown", userGroup, file.getCanonicalPath());
  }

  private void assertOwners(File file, String expectedUser,
      String expectedGroup) throws IOException {
    String [] args = lsF(file).trim().split("[\\|]");
    assertEquals(StringUtils.toLowerCase(expectedUser),
        StringUtils.toLowerCase(args[2]));
    assertEquals(StringUtils.toLowerCase(expectedGroup),
        StringUtils.toLowerCase(args[3]));
  }

  @Test (timeout = 30000)
  public void testChown() throws IOException {
    requireWinutils();
    File a = new File(TEST_DIR, "a");
    assertTrue(a.createNewFile());
    String username = System.getProperty("user.name");
    // username including the domain aka DOMAIN\\user
    String qualifiedUsername = Shell.execCommand("whoami").trim();
    String admins = "Administrators";
    String qualifiedAdmins = "BUILTIN\\Administrators";

    chown(username + ":" + admins, a);
    assertOwners(a, qualifiedUsername, qualifiedAdmins);
 
    chown(username, a);
    chown(":" + admins, a);
    assertOwners(a, qualifiedUsername, qualifiedAdmins);

    chown(":" + admins, a);
    chown(username + ":", a);
    assertOwners(a, qualifiedUsername, qualifiedAdmins);

    assertTrue(a.delete());
    assertFalse(a.exists());
  }

  @Test (timeout = 30000)
  public void testSymlinkRejectsForwardSlashesInLink() throws IOException {
    requireWinutils();
    File newFile = new File(TEST_DIR, "file");
    assertTrue(newFile.createNewFile());
    String target = newFile.getPath();
    String link = new File(TEST_DIR, "link").getPath().replaceAll("\\\\", "/");
    try {
      Shell.execCommand(winutils, "symlink", link, target);
      fail(String.format("did not receive expected failure creating symlink "
        + "with forward slashes in link: link = %s, target = %s", link, target));
    } catch (IOException e) {
      LOG.info(
        "Expected: Failed to create symlink with forward slashes in target");
    }
  }

  @Test (timeout = 30000)
  public void testSymlinkRejectsForwardSlashesInTarget() throws IOException {
    requireWinutils();
    File newFile = new File(TEST_DIR, "file");
    assertTrue(newFile.createNewFile());
    String target = newFile.getPath().replaceAll("\\\\", "/");
    String link = new File(TEST_DIR, "link").getPath();
    try {
      Shell.execCommand(winutils, "symlink", link, target);
      fail(String.format("did not receive expected failure creating symlink "
        + "with forward slashes in target: link = %s, target = %s", link, target));
    } catch (IOException e) {
      LOG.info(
        "Expected: Failed to create symlink with forward slashes in target");
    }
  }

  @Test (timeout = 30000)
  public void testReadLink() throws IOException {
    requireWinutils();
    // Create TEST_DIR\dir1\file1.txt
    //
    File dir1 = new File(TEST_DIR, "dir1");
    assertTrue(dir1.mkdirs());

    File file1 = new File(dir1, "file1.txt");
    assertTrue(file1.createNewFile());

    File dirLink = new File(TEST_DIR, "dlink");
    File fileLink = new File(TEST_DIR, "flink");

    // Next create a directory symlink to dir1 and a file
    // symlink to file1.txt.
    //
    Shell.execCommand(
        winutils, "symlink", dirLink.toString(), dir1.toString());
    Shell.execCommand(
        winutils, "symlink", fileLink.toString(), file1.toString());

    // Read back the two links and ensure we get what we expected.
    //
    String readLinkOutput = Shell.execCommand(winutils,
        "readlink",
        dirLink.toString());
    assertThat(readLinkOutput, equalTo(dir1.toString()));

    readLinkOutput = Shell.execCommand(winutils,
        "readlink",
        fileLink.toString());
    assertThat(readLinkOutput, equalTo(file1.toString()));

    // Try a few invalid inputs and verify we get an ExitCodeException for each.
    //
    try {
      // No link name specified.
      //
      Shell.execCommand(winutils, "readlink", "");
      fail("Failed to get Shell.ExitCodeException when reading bad symlink");
    } catch (Shell.ExitCodeException ece) {
      assertThat(ece.getExitCode(), is(1));
    }

    try {
      // Bad link name.
      //
      Shell.execCommand(winutils, "readlink", "ThereIsNoSuchLink");
      fail("Failed to get Shell.ExitCodeException when reading bad symlink");
    } catch (Shell.ExitCodeException ece) {
      assertThat(ece.getExitCode(), is(1));
    }

    try {
      // Non-symlink directory target.
      //
      Shell.execCommand(winutils, "readlink", dir1.toString());
      fail("Failed to get Shell.ExitCodeException when reading bad symlink");
    } catch (Shell.ExitCodeException ece) {
      assertThat(ece.getExitCode(), is(1));
    }

    try {
      // Non-symlink file target.
      //
      Shell.execCommand(winutils, "readlink", file1.toString());
      fail("Failed to get Shell.ExitCodeException when reading bad symlink");
    } catch (Shell.ExitCodeException ece) {
      assertThat(ece.getExitCode(), is(1));
    }

    try {
      // Too many parameters.
      //
      Shell.execCommand(winutils, "readlink", "a", "b");
      fail("Failed to get Shell.ExitCodeException with bad parameters");
    } catch (Shell.ExitCodeException ece) {
      assertThat(ece.getExitCode(), is(1));
    }
  }
  
  @SuppressWarnings("deprecation")
  @Test(timeout=10000)
  public void testTaskCreate() throws IOException {
    requireWinutils();
    File batch = new File(TEST_DIR, "testTaskCreate.cmd");
    File proof = new File(TEST_DIR, "testTaskCreate.out");
    FileWriter fw = new FileWriter(batch);
    String testNumber = String.format("%f", Math.random());
    fw.write(String.format("echo %s > \"%s\"", testNumber, proof.getAbsolutePath()));
    fw.close();
    
    assertFalse(proof.exists());
    
    Shell.execCommand(winutils, "task", "create", "testTaskCreate" + testNumber,
        batch.getAbsolutePath());
    
    assertTrue(proof.exists());
    
    String outNumber = FileUtils.readFileToString(proof);
    
    assertThat(outNumber, containsString(testNumber));
  }

  @Test (timeout = 30000)
  public void testTaskCreateWithLimits() throws IOException {
    requireWinutils();
    // Generate a unique job id
    String jobId = String.format("%f", Math.random());

    // Run a task without any options
    String out = Shell.execCommand(winutils, "task", "create",
        "job" + jobId, "cmd /c echo job" + jobId);
    assertTrue(out.trim().equals("job" + jobId));

    // Run a task without any limits
    jobId = String.format("%f", Math.random());
    out = Shell.execCommand(winutils, "task", "create", "-c", "-1", "-m",
        "-1", "job" + jobId, "cmd /c echo job" + jobId);
    assertTrue(out.trim().equals("job" + jobId));

    // Run a task with limits (128MB should be enough for a cmd)
    jobId = String.format("%f", Math.random());
    out = Shell.execCommand(winutils, "task", "create", "-c", "10000", "-m",
        "128", "job" + jobId, "cmd /c echo job" + jobId);
    assertTrue(out.trim().equals("job" + jobId));

    // Run a task without enough memory
    try {
      jobId = String.format("%f", Math.random());
      out = Shell.execCommand(winutils, "task", "create", "-m", "128", "job"
          + jobId, "java -Xmx256m -version");
      fail("Failed to get Shell.ExitCodeException with insufficient memory");
    } catch (Shell.ExitCodeException ece) {
      assertThat(ece.getExitCode(), is(1));
    }

    // Run tasks with wrong parameters
    //
    try {
      jobId = String.format("%f", Math.random());
      Shell.execCommand(winutils, "task", "create", "-c", "-1", "-m",
          "-1", "foo", "job" + jobId, "cmd /c echo job" + jobId);
      fail("Failed to get Shell.ExitCodeException with bad parameters");
    } catch (Shell.ExitCodeException ece) {
      assertThat(ece.getExitCode(), is(1639));
    }

    try {
      jobId = String.format("%f", Math.random());
      Shell.execCommand(winutils, "task", "create", "-c", "-m", "-1",
          "job" + jobId, "cmd /c echo job" + jobId);
      fail("Failed to get Shell.ExitCodeException with bad parameters");
    } catch (Shell.ExitCodeException ece) {
      assertThat(ece.getExitCode(), is(1639));
    }

    try {
      jobId = String.format("%f", Math.random());
      Shell.execCommand(winutils, "task", "create", "-c", "foo",
          "job" + jobId, "cmd /c echo job" + jobId);
      fail("Failed to get Shell.ExitCodeException with bad parameters");
    } catch (Shell.ExitCodeException ece) {
      assertThat(ece.getExitCode(), is(1639));
    }
  }
}
