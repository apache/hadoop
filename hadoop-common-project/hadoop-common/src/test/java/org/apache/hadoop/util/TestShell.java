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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.security.alias.AbstractJavaKeyStoreProvider;
import org.junit.Assert;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.test.GenericTestUtils;

import static org.apache.hadoop.util.Shell.*;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;

public class TestShell extends Assert {
  /**
   * Set the timeout for every test
   */
  @Rule
  public Timeout testTimeout = new Timeout(30000);

  @Rule
  public TestName methodName = new TestName();

  private File rootTestDir = GenericTestUtils.getTestDir();

  /**
   * A filename generated uniquely for each test method. The file
   * itself is neither created nor deleted during test setup/teardown.
   */
  private File methodDir;

  private static class Command extends Shell {
    private int runCount = 0;

    private Command(long interval) {
      super(interval);
    }

    @Override
    protected String[] getExecString() {
      // There is no /bin/echo equivalent on Windows so just launch it as a
      // shell built-in.
      //
      return WINDOWS ?
          (new String[] {"cmd.exe", "/c", "echo", "hello"}) :
          (new String[] {"echo", "hello"});
    }

    @Override
    protected void parseExecResult(BufferedReader lines) throws IOException {
      ++runCount;
    }

    public int getRunCount() {
      return runCount;
    }
  }

  @Before
  public void setup() {
    rootTestDir.mkdirs();
    assertTrue("Not a directory " + rootTestDir, rootTestDir.isDirectory());
    methodDir = new File(rootTestDir, methodName.getMethodName());
  }

  @Test
  public void testInterval() throws IOException {
    testInterval(Long.MIN_VALUE / 60000);  // test a negative interval
    testInterval(0L);  // test a zero interval
    testInterval(10L); // interval equal to 10mins
    testInterval(Time.now() / 60000 + 60); // test a very big interval
  }

  /**
   * Assert that a string has a substring in it
   * @param string string to search
   * @param search what to search for it
   */
  private void assertInString(String string, String search) {
    assertNotNull("Empty String", string);
    if (!string.contains(search)) {
      fail("Did not find \"" + search + "\" in " + string);
    }
  }

  @Test
  public void testShellCommandExecutorToString() throws Throwable {
    Shell.ShellCommandExecutor sce=new Shell.ShellCommandExecutor(
            new String[] { "ls", "..","arg 2"});
    String command = sce.toString();
    assertInString(command,"ls");
    assertInString(command, " .. ");
    assertInString(command, "\"arg 2\"");
  }

  @Test
  public void testShellCommandTimeout() throws Throwable {
    Assume.assumeFalse(WINDOWS);
    String rootDir = rootTestDir.getAbsolutePath();
    File shellFile = new File(rootDir, "timeout.sh");
    String timeoutCommand = "sleep 4; echo \"hello\"";
    Shell.ShellCommandExecutor shexc;
    try (PrintWriter writer = new PrintWriter(new FileOutputStream(shellFile))) {
      writer.println(timeoutCommand);
      writer.close();
    }
    FileUtil.setExecutable(shellFile, true);
    shexc = new Shell.ShellCommandExecutor(new String[]{shellFile.getAbsolutePath()},
        null, null, 100);
    try {
      shexc.execute();
    } catch (Exception e) {
      //When timing out exception is thrown.
    }
    shellFile.delete();
    assertTrue("Script did not timeout" , shexc.isTimedOut());
  }

  @Test
  public void testEnvVarsWithInheritance() throws Exception {
    Assume.assumeFalse(WINDOWS);
    testEnvHelper(true);
  }

  @Test
  public void testEnvVarsWithoutInheritance() throws Exception {
    Assume.assumeFalse(WINDOWS);
    testEnvHelper(false);
  }

  private void testEnvHelper(boolean inheritParentEnv) throws Exception {
    Map<String, String> customEnv = new HashMap<>();
    customEnv.put("AAA" + System.currentTimeMillis(), "AAA");
    customEnv.put("BBB" + System.currentTimeMillis(), "BBB");
    customEnv.put("CCC" + System.currentTimeMillis(), "CCC");
    Shell.ShellCommandExecutor command = new ShellCommandExecutor(
        new String[]{"env"}, null, customEnv, 0L, inheritParentEnv);
    command.execute();
    String[] varsArr = command.getOutput().split("\n");
    Map<String, String> vars = new HashMap<>();
    for (String var : varsArr) {
      int eqIndex = var.indexOf('=');
      vars.put(var.substring(0, eqIndex), var.substring(eqIndex + 1));
    }
    Map<String, String> expectedEnv = new HashMap<>();
    expectedEnv.putAll(customEnv);
    if (inheritParentEnv) {
      expectedEnv.putAll(System.getenv());
    }
    assertEquals(expectedEnv, vars);
  }
  
  private static int countTimerThreads() {
    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
    
    int count = 0;
    ThreadInfo[] infos = threadBean.getThreadInfo(threadBean.getAllThreadIds(), 20);
    for (ThreadInfo info : infos) {
      if (info == null) continue;
      for (StackTraceElement elem : info.getStackTrace()) {
        if (elem.getClassName().contains("Timer")) {
          count++;
          break;
        }
      }
    }
    return count;
  }

  @Test
  public void testShellCommandTimerLeak() throws Exception {
    String quickCommand[] = new String[] {"/bin/sleep", "100"};
    
    int timersBefore = countTimerThreads();
    System.err.println("before: " + timersBefore);
    
    for (int i = 0; i < 10; i++) {
      Shell.ShellCommandExecutor shexec = new Shell.ShellCommandExecutor(
            quickCommand, null, null, 1);
      try {
        shexec.execute();
        fail("Bad command should throw exception");
      } catch (Exception e) {
        // expected
      }
    }
    Thread.sleep(1000);
    int timersAfter = countTimerThreads();
    System.err.println("after: " + timersAfter);
    assertEquals(timersBefore, timersAfter);
  }

  @Test
  public void testGetCheckProcessIsAliveCommand() throws Exception {
    String anyPid = "9999";
    String[] checkProcessAliveCommand = getCheckProcessIsAliveCommand(
        anyPid);

    String[] expectedCommand;

    if (Shell.WINDOWS) {
      expectedCommand =
          new String[]{getWinUtilsPath(), "task", "isAlive", anyPid };
    } else if (Shell.isSetsidAvailable) {
      expectedCommand = new String[] { "bash", "-c", "kill -0 -- -" + anyPid };
    } else {
      expectedCommand = new String[]{ "bash", "-c", "kill -0 " + anyPid };
    }
    Assert.assertArrayEquals(expectedCommand, checkProcessAliveCommand);
  }

  @Test
  public void testGetSignalKillCommand() throws Exception {
    String anyPid = "9999";
    int anySignal = 9;
    String[] checkProcessAliveCommand = getSignalKillCommand(anySignal,
        anyPid);

    String[] expectedCommand;

    if (Shell.WINDOWS) {
      expectedCommand =
          new String[]{getWinUtilsPath(), "task", "kill", anyPid };
    } else if (Shell.isSetsidAvailable) {
      expectedCommand = new String[] { "bash", "-c", "kill -9 -- -" + anyPid };
    } else {
      expectedCommand = new String[]{ "bash", "-c", "kill -9 " + anyPid };
    }
    Assert.assertArrayEquals(expectedCommand, checkProcessAliveCommand);
  }

  private void testInterval(long interval) throws IOException {
    Command command = new Command(interval);

    command.run();
    assertEquals(1, command.getRunCount());

    command.run();
    if (interval > 0) {
      assertEquals(1, command.getRunCount());
    } else {
      assertEquals(2, command.getRunCount());
    }
  }

  @Test
  public void testHadoopHomeUnset() throws Throwable {
    assertHomeResolveFailed(null, "unset");
  }

  @Test
  public void testHadoopHomeEmpty() throws Throwable {
    assertHomeResolveFailed("", E_HADOOP_PROPS_EMPTY);
  }

  @Test
  public void testHadoopHomeEmptyDoubleQuotes() throws Throwable {
    assertHomeResolveFailed("\"\"", E_HADOOP_PROPS_EMPTY);
  }

  @Test
  public void testHadoopHomeEmptySingleQuote() throws Throwable {
    assertHomeResolveFailed("\"", E_HADOOP_PROPS_EMPTY);
  }

  @Test
  public void testHadoopHomeValid() throws Throwable {
    File f = checkHadoopHomeInner(rootTestDir.getCanonicalPath());
    assertEquals(rootTestDir, f);
  }

  @Test
  public void testHadoopHomeValidQuoted() throws Throwable {
    File f = checkHadoopHomeInner('"'+ rootTestDir.getCanonicalPath() + '"');
    assertEquals(rootTestDir, f);
  }

  @Test
  public void testHadoopHomeNoDir() throws Throwable {
    assertHomeResolveFailed(methodDir.getCanonicalPath(), E_DOES_NOT_EXIST);
  }

  @Test
  public void testHadoopHomeNotADir() throws Throwable {
    File touched = touch(methodDir);
    try {
      assertHomeResolveFailed(touched.getCanonicalPath(), E_NOT_DIRECTORY);
    } finally {
      FileUtils.deleteQuietly(touched);
    }
  }

  @Test
  public void testHadoopHomeRelative() throws Throwable {
    assertHomeResolveFailed("./target", E_IS_RELATIVE);
  }

  @Test
  public void testBinDirMissing() throws Throwable {
    FileNotFoundException ex = assertWinutilsResolveFailed(methodDir,
        E_DOES_NOT_EXIST);
    assertInString(ex.toString(), "Hadoop bin directory");
  }

  @Test
  public void testHadoopBinNotADir() throws Throwable {
    File bin = new File(methodDir, "bin");
    touch(bin);
    try {
      assertWinutilsResolveFailed(methodDir, E_NOT_DIRECTORY);
    } finally {
      FileUtils.deleteQuietly(methodDir);
    }
  }

  @Test
  public void testBinWinUtilsFound() throws Throwable {
    try {
      File bin = new File(methodDir, "bin");
      File winutils = new File(bin, WINUTILS_EXE);
      touch(winutils);
      assertEquals(winutils.getCanonicalPath(),
          getQualifiedBinInner(methodDir, WINUTILS_EXE).getCanonicalPath());
    } finally {
      FileUtils.deleteQuietly(methodDir);
    }
  }

  @Test
  public void testBinWinUtilsNotAFile() throws Throwable {
    try {
      File bin = new File(methodDir, "bin");
      File winutils = new File(bin, WINUTILS_EXE);
      winutils.mkdirs();
      assertWinutilsResolveFailed(methodDir, E_NOT_EXECUTABLE_FILE);
    } finally {
      FileUtils.deleteDirectory(methodDir);
    }
  }

  /**
   * This test takes advantage of the invariant winutils path is valid
   * or access to it will raise an exception holds on Linux, and without
   * any winutils binary even if HADOOP_HOME points to a real hadoop
   * directory, the exception reporting can be validated
   */
  @Test
  public void testNoWinutilsOnUnix() throws Throwable {
    Assume.assumeFalse(WINDOWS);
    try {
      getWinUtilsFile();
    } catch (FileNotFoundException ex) {
      assertExContains(ex, E_NOT_A_WINDOWS_SYSTEM);
    }
    try {
      getWinUtilsPath();
    } catch (RuntimeException ex) {
      assertExContains(ex, E_NOT_A_WINDOWS_SYSTEM);
      if ( ex.getCause() == null
          || !(ex.getCause() instanceof FileNotFoundException)) {
        throw ex;
      }
    }
  }

  /**
   * Touch a file; creating parent dirs on demand.
   * @param path path of file
   * @return the file created
   * @throws IOException on any failure to write
   */
  private File touch(File path) throws IOException {
    path.getParentFile().mkdirs();
    FileUtils.writeByteArrayToFile(path, new byte[]{});
    return path;
  }

  /**
   * Assert that an attept to resolve the hadoop home dir failed with
   * an expected text in the exception string value.
   * @param path input
   * @param expectedText expected exception text
   * @return the caught exception
   * @throws FileNotFoundException any FileNotFoundException that was thrown
   * but which did not contain the expected text
   */
  private FileNotFoundException assertHomeResolveFailed(String path,
      String expectedText) throws Exception {
    try {
      File f = checkHadoopHomeInner(path);
      fail("Expected an exception with the text `" + expectedText + "`"
          + " -but got the path " + f);
      // unreachable
      return null;
    } catch (FileNotFoundException ex) {
      assertExContains(ex, expectedText);
      return ex;
    }
  }

  /**
   * Assert that an attept to resolve the {@code bin/winutils.exe} failed with
   * an expected text in the exception string value.
   * @param hadoopHome hadoop home directory
   * @param expectedText expected exception text
   * @return the caught exception
   * @throws Exception any Exception that was thrown
   * but which did not contain the expected text
   */
  private FileNotFoundException assertWinutilsResolveFailed(File hadoopHome,
      String expectedText) throws Exception {
    try {
      File f = getQualifiedBinInner(hadoopHome, WINUTILS_EXE);
      fail("Expected an exception with the text `" + expectedText + "`"
          + " -but got the path " + f);
      // unreachable
      return null;
    } catch (FileNotFoundException ex) {
      assertExContains(ex, expectedText);
      return ex;
    }
  }

  private void assertExContains(Exception ex, String expectedText)
      throws Exception {
    if (!ex.toString().contains(expectedText)) {
      throw ex;
    }
  }

}
