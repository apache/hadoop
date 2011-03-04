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

import junit.framework.TestCase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

public class TestShell extends TestCase {

  private static class Command extends Shell {
    private int runCount = 0;

    private Command(long interval) {
      super(interval);
    }

    protected String[] getExecString() {
      return new String[] {"echo", "hello"};
    }

    protected void parseExecResult(BufferedReader lines) throws IOException {
      ++runCount;
    }

    public int getRunCount() {
      return runCount;
    }
  }

  public void testInterval() throws IOException {
    testInterval(Long.MIN_VALUE / 60000);  // test a negative interval
    testInterval(0L);  // test a zero interval
    testInterval(10L); // interval equal to 10mins
    testInterval(System.currentTimeMillis() / 60000 + 60); // test a very big interval
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

  public void testShellCommandExecutorToString() throws Throwable {
    Shell.ShellCommandExecutor sce=new Shell.ShellCommandExecutor(
            new String[] { "ls","..","arg 2"});
    String command = sce.toString();
    assertInString(command,"ls");
    assertInString(command, " .. ");
    assertInString(command, "\"arg 2\"");
  }
  
  public void testShellCommandTimeout() throws Throwable {
    String rootDir = new File(System.getProperty(
        "test.build.data", "/tmp")).getAbsolutePath();
    File shellFile = new File(rootDir, "timeout.sh");
    String timeoutCommand = "sleep 4; echo \"hello\"";
    PrintWriter writer = new PrintWriter(new FileOutputStream(shellFile));
    writer.println(timeoutCommand);
    writer.close();
    shellFile.setExecutable(true);
    Shell.ShellCommandExecutor shexc 
    = new Shell.ShellCommandExecutor(new String[]{shellFile.getAbsolutePath()},
                                      null, null, 100);
    try {
      shexc.execute();
    } catch (Exception e) {
      //When timing out exception is thrown.
    }
    shellFile.delete();
    assertTrue("Script didnt not timeout" , shexc.isTimedOut());
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
}
