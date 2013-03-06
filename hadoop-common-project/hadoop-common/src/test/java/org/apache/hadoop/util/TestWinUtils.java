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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for helper Windows winutils.exe utility.
 */
public class TestWinUtils {

  private static final Log LOG = LogFactory.getLog(TestWinUtils.class);
  private static File TEST_DIR = new File(System.getProperty("test.build.data",
      "/tmp"), TestWinUtils.class.getSimpleName());

  @Before
  public void setUp() {
    TEST_DIR.mkdirs();
  }

  @After
  public void tearDown() throws IOException {
    FileUtil.fullyDelete(TEST_DIR);
  }

  // Helper routine that writes the given content to the file.
  private void writeFile(File file, String content) throws IOException {
    byte[] data = content.getBytes();
    FileOutputStream os = new FileOutputStream(file);
    os.write(data);
    os.close();
  }

  // Helper routine that reads the first 100 bytes from the file.
  private String readFile(File file) throws IOException {
    FileInputStream fos = new FileInputStream(file);
    byte[] b = new byte[100];
    fos.read(b);
    return b.toString();
  }

  @Test (timeout = 30000)
  public void testLs() throws IOException {
    if (!Shell.WINDOWS) {
      // Not supported on non-Windows platforms
      return;
    }

    final String content = "6bytes";
    final int contentSize = content.length();
    File testFile = new File(TEST_DIR, "file1");
    writeFile(testFile, content);

    // Verify permissions and file name return tokens
    String output = Shell.execCommand(
        Shell.WINUTILS, "ls", testFile.getCanonicalPath());
    String[] outputArgs = output.split("[ \r\n]");
    assertTrue(outputArgs[0].equals("-rwx------"));
    assertTrue(outputArgs[outputArgs.length - 1]
        .equals(testFile.getCanonicalPath()));

    // Verify most tokens when using a formatted output (other tokens
    // will be verified with chmod/chown)
    output = Shell.execCommand(
        Shell.WINUTILS, "ls", "-F", testFile.getCanonicalPath());
    outputArgs = output.split("[|\r\n]");
    assertEquals(9, outputArgs.length);
    assertTrue(outputArgs[0].equals("-rwx------"));
    assertEquals(contentSize, Long.parseLong(outputArgs[4]));
    assertTrue(outputArgs[8].equals(testFile.getCanonicalPath()));

    testFile.delete();
    assertFalse(testFile.exists());
  }

  @Test (timeout = 30000)
  public void testGroups() throws IOException {
    if (!Shell.WINDOWS) {
      // Not supported on non-Windows platforms
      return;
    }

    String currentUser = System.getProperty("user.name");

    // Verify that groups command returns information about the current user
    // groups when invoked with no args
    String outputNoArgs = Shell.execCommand(
        Shell.WINUTILS, "groups").trim();
    String output = Shell.execCommand(
        Shell.WINUTILS, "groups", currentUser).trim();
    assertEquals(output, outputNoArgs);

    // Verify that groups command with the -F flag returns the same information
    String outputFormat = Shell.execCommand(
        Shell.WINUTILS, "groups", "-F", currentUser).trim();
    outputFormat = outputFormat.replace("|", " ");
    assertEquals(output, outputFormat);
  }

  private void chmod(String mask, File file) throws IOException {
    Shell.execCommand(
        Shell.WINUTILS, "chmod", mask, file.getCanonicalPath());
  }

  private void chmodR(String mask, File file) throws IOException {
    Shell.execCommand(
        Shell.WINUTILS, "chmod", "-R", mask, file.getCanonicalPath());
  }

  private String ls(File file) throws IOException {
    return Shell.execCommand(
        Shell.WINUTILS, "ls", file.getCanonicalPath());
  }

  private String lsF(File file) throws IOException {
    return Shell.execCommand(
        Shell.WINUTILS, "ls", "-F", file.getCanonicalPath());
  }

  private void assertPermissions(File file, String expected)
      throws IOException {
    String output = ls(file).split("[ \r\n]")[0];
    assertEquals(expected, output);
  }

  @Test (timeout = 30000)
  private void testChmodInternal(String mode, String expectedPerm)
      throws IOException {
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

  @Test (timeout = 30000)
  private void testNewFileChmodInternal(String expectedPerm) throws IOException {
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

  @Test (timeout = 30000)
  private void testChmodInternalR(String mode, String expectedPerm,
      String expectedPermx) throws IOException {
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
    if (!Shell.WINDOWS) {
      // Not supported on non-Windows platforms
      return;
    }

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
      assertFalse("writeFile should have failed!", true);
    } catch (IOException ex) {
      LOG.info("Expected: Failed write to a file with permissions 577");
    }
    // restore permissions
    chmod("700", a);
    assertTrue(a.delete());

    // - Copy WINUTILS to a new executable file, a.exe.
    // - Change mode to 677 so owner does not have execute permission.
    // - Verify the owner truly does not have the permissions to execute the file.

    File winutilsFile = new File(Shell.WINUTILS);
    File aExe = new File(TEST_DIR, "a.exe");
    FileUtils.copyFile(winutilsFile, aExe);
    chmod("677", aExe);

    try {
      Shell.execCommand(aExe.getCanonicalPath(), "ls");
      assertFalse("executing " + aExe + " should have failed!", true);
    } catch (IOException ex) {
      LOG.info("Expected: Failed to execute a file with permissions 677");
    }
    assertTrue(aExe.delete());
  }

  @Test (timeout = 30000)
  public void testChmod() throws IOException {
    if (!Shell.WINDOWS) {
      // Not supported on non-Windows platforms
      return;
    }

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
    testNewFileChmodInternal("-rwx------");
  }

  private void chown(String userGroup, File file) throws IOException {
    Shell.execCommand(
        Shell.WINUTILS, "chown", userGroup, file.getCanonicalPath());
  }

  private void assertOwners(File file, String expectedUser,
      String expectedGroup) throws IOException {
    String [] args = lsF(file).trim().split("[\\|]");
    assertEquals(expectedUser.toLowerCase(), args[2].toLowerCase());
    assertEquals(expectedGroup.toLowerCase(), args[3].toLowerCase());
  }

  @Test (timeout = 30000)
  public void testChown() throws IOException {
    if (!Shell.WINDOWS) {
      // Not supported on non-Windows platforms
      return;
    }

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
}
