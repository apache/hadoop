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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.mapred.UtilsForTests;

import junit.framework.TestCase;

/**
 * A JUnit test to test ProcfsBasedProcessTree.
 */
public class TestProcfsBasedProcessTree extends TestCase {

  private static final Log LOG = LogFactory
      .getLog(TestProcfsBasedProcessTree.class);
  private static String TEST_ROOT_DIR = new Path(System.getProperty(
         "test.build.data", "/tmp")).toString().replace(' ', '+');

  private ShellCommandExecutor shexec = null;
  private String pidFile, lowestDescendant;
  private String shellScript;
  private static final int N = 6; // Controls the RogueTask

  private class RogueTaskThread extends Thread {
    public void run() {
      try {
        Vector<String> args = new Vector<String>();
        if(ProcessTree.isSetsidAvailable) {
          args.add("setsid");
        }
        args.add("bash");
        args.add("-c");
        args.add(" echo $$ > " + pidFile + "; sh " +
                          shellScript + " " + N + ";") ;
        shexec = new ShellCommandExecutor(args.toArray(new String[0]));
        shexec.execute();
      } catch (ExitCodeException ee) {
        LOG.info("Shell Command exit with a non-zero exit code. This is" +
                 " expected as we are killing the subprocesses of the" +
                 " task intentionally. " + ee);
      } catch (IOException ioe) {
        LOG.info("Error executing shell command " + ioe);
      } finally {
        LOG.info("Exit code: " + shexec.getExitCode());
      }
    }
  }

  private String getRogueTaskPID() {
    File f = new File(pidFile);
    while (!f.exists()) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException ie) {
        break;
      }
    }

    // read from pidFile
    return UtilsForTests.getPidFromPidFile(pidFile);
  }

  public void testProcessTree() {

    try {
      if (!ProcfsBasedProcessTree.isAvailable()) {
        System.out
            .println("ProcfsBasedProcessTree is not available on this system. Not testing");
        return;
      }
    } catch (Exception e) {
      LOG.info(StringUtils.stringifyException(e));
      return;
    }
    // create shell script
    Random rm = new Random();
    File tempFile = new File(TEST_ROOT_DIR, this.getName() + "_shellScript_" +
                             rm.nextInt() + ".sh");
    tempFile.deleteOnExit();
    shellScript = TEST_ROOT_DIR + File.separator + tempFile.getName();

    // create pid file
    tempFile = new File(TEST_ROOT_DIR,  this.getName() + "_pidFile_" +
                        rm.nextInt() + ".pid");
    tempFile.deleteOnExit();
    pidFile = TEST_ROOT_DIR + File.separator + tempFile.getName();

    lowestDescendant = TEST_ROOT_DIR + File.separator + "lowestDescendantPidFile";

    // write to shell-script
    try {
      FileWriter fWriter = new FileWriter(shellScript);
      fWriter.write(
          "# rogue task\n" +
          "sleep 1\n" +
          "echo hello\n" +
          "if [ $1 -ne 0 ]\n" +
          "then\n" +
          " sh " + shellScript + " $(($1-1))\n" +
          "else\n" +
          " echo $$ > " + lowestDescendant + "\n" +
          " while true\n do\n" +
          "  sleep 5\n" +
          " done\n" +
          "fi");
      fWriter.close();
    } catch (IOException ioe) {
      LOG.info("Error: " + ioe);
      return;
    }

    Thread t = new RogueTaskThread();
    t.start();
    String pid = getRogueTaskPID();
    LOG.info("Root process pid: " + pid);
    ProcfsBasedProcessTree p = new ProcfsBasedProcessTree(pid,
                               ProcessTree.isSetsidAvailable,
                               ProcessTree.DEFAULT_SLEEPTIME_BEFORE_SIGKILL);
    p = p.getProcessTree(); // initialize
    LOG.info("ProcessTree: " + p.toString());

    File leaf = new File(lowestDescendant);
    //wait till lowest descendant process of Rougue Task starts execution
    while (!leaf.exists()) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException ie) {
        break;
      }
    }

    p = p.getProcessTree(); // reconstruct
    LOG.info("ProcessTree: " + p.toString());

    // destroy the map task and all its subprocesses
    p.destroy(true/*in the background*/);

    if(ProcessTree.isSetsidAvailable) {// whole processtree should be gone
      assertEquals(false, p.isAnyProcessInTreeAlive());
    }
    else {// process should be gone
      assertFalse("ProcessTree must have been gone", p.isAlive());
    }
    // Not able to join thread sometimes when forking with large N.
    try {
      t.join(2000);
      LOG.info("RogueTaskThread successfully joined.");
    } catch (InterruptedException ie) {
      LOG.info("Interrupted while joining RogueTaskThread.");
    }

    // ProcessTree is gone now. Any further calls should be sane.
    p = p.getProcessTree();
    assertFalse("ProcessTree must have been gone", p.isAlive());
    assertTrue("Cumulative vmem for the gone-process is "
        + p.getCumulativeVmem() + " . It should be zero.", p
        .getCumulativeVmem() == 0);
    assertTrue(p.toString().equals("[ ]"));
  }
}
