package org.apache.hadoop.mapred;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.util.ProcessTree;
import org.apache.hadoop.util.Shell;

import junit.framework.TestCase;

/**
 * Tests for the TaskLog.
 */
public class TestTaskLog extends TestCase {
  private List<String> cmd;
  private List<String> setup;
  private File stdoutFilename = new File("stdout");
  private File stderrFilename = new File("stderr");
  
  /**
   * Setup
   */
  public void setUp() throws Exception {
    cmd = new LinkedList<String>();
    setup = new LinkedList<String>();
    setup.add("set setup");
    cmd.add("command1");
    cmd.add("command2");
  }

  /**
   * Verify that TaskLog.buildCommandLine() returns the expected results.
   */
  public void testBuildCommandLine() throws IOException {
    // useSetSid set to false 
    String result = TaskLog.buildCommandLine(setup, cmd, stdoutFilename,
      stderrFilename, 0, false);

    if (Shell.WINDOWS) {
      assertEquals("set setup\n" +
          "command1 \"command2\"  < nul  1>> stdout 2>> stderr",
          result);
    } else {
      assertEquals("export JVM_PID=`echo $$`\n" +
          "set setup\n" +
          "exec 'command1' 'command2'  < /dev/null  1>> stdout 2>> stderr",
          result);
    }

    // useSetSid set to true
    boolean setsidAvailableOrig = ProcessTree.isSetsidAvailable;
    try {
      ProcessTree.isSetsidAvailable = true;
      result = TaskLog.buildCommandLine(setup, cmd, stdoutFilename,
        stderrFilename, 0, true);

      if (Shell.WINDOWS) {
        assertEquals("set setup\n" +
            "command1 \"command2\"  < nul  1>> stdout 2>> stderr",
            result);
      } else {
        assertEquals("export JVM_PID=`echo $$`\n" +
            "set setup\n" +
            "exec setsid 'command1' 'command2'  < /dev/null  1>> stdout 2>> stderr",
            result);
      }
    } finally {
      // restore the original value back
      ProcessTree.isSetsidAvailable = setsidAvailableOrig;
    }
  }

  private String generateString(String prefix, int length) {
    StringBuffer result = new StringBuffer();
    result.append(prefix);
    for (int i = 0; i < length; ++i) {
      result.append("c");
    }
    return result.toString();
  }

  /**
   * Verify that TaskLog.buildCommandLine() throws if the command line
   * exceeds the command line limit.
   */
  public void testBuildCommandLineLongCommand() {
    int maxCmdLineLengthOrig = TaskLog.MAX_CMD_LINE_LENGTH;
    TaskLog.MAX_CMD_LINE_LENGTH = 1000;
    try {
      // Test long command in setup
      String setupTestCommand = generateString("setup ",
        TaskLog.MAX_CMD_LINE_LENGTH + 1);
      setup.add(setupTestCommand);

      IOException ex = null;
      try {
        TaskLog.buildCommandLine(setup, cmd, stdoutFilename, stderrFilename,
          0, false);
      } catch (IOException e) {
        ex = e;
      }
      assertNotNull(ex);
      assertTrue(ex.getMessage().contains(setupTestCommand));

      // Test long command in cmd
      setup.clear();
      String cmdTestCommand = generateString("command ",
        TaskLog.MAX_CMD_LINE_LENGTH + 1);
      cmd.add(cmdTestCommand.toString());

      ex = null;
      try {
        TaskLog.buildCommandLine(setup, cmd, stdoutFilename, stderrFilename,
          0, false);
      } catch (IOException e) {
        ex = e;
      }
      assertNotNull(ex);
      assertTrue(ex.getMessage().contains(cmdTestCommand));
    } finally {
      // restore the original value back
      TaskLog.MAX_CMD_LINE_LENGTH = maxCmdLineLengthOrig;
    }
  }
}
