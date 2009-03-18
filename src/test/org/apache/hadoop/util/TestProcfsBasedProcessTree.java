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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

import junit.framework.TestCase;

public class TestProcfsBasedProcessTree extends TestCase {

  private static final Log LOG = LogFactory
      .getLog(TestProcfsBasedProcessTree.class);
  private ShellCommandExecutor shexec = null;
  private String pidFile;
  private String shellScript;
  private static final int N = 10; // Controls the RogueTask

  private static final int memoryLimit = 15 * 1024 * 1024; // 15MB
  private static final long PROCESSTREE_RECONSTRUCTION_INTERVAL =
    ProcfsBasedProcessTree.DEFAULT_SLEEPTIME_BEFORE_SIGKILL; // msec

  private class RogueTaskThread extends Thread {
    public void run() {
      try {
        String args[] = { "bash", "-c",
            "echo $$ > " + pidFile + "; sh " + shellScript + " " + N + ";" };
        shexec = new ShellCommandExecutor(args);
        shexec.execute();
      } catch (ExitCodeException ee) {
        LOG.info("Shell Command exit with a non-zero exit code. " + ee);
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
    return ProcfsBasedProcessTree.getPidFromPidFile(pidFile);
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
    File tempFile = new File(this.getName() + "_shellScript_" + rm.nextInt()
        + ".sh");
    tempFile.deleteOnExit();
    shellScript = tempFile.getName();

    // create pid file
    tempFile = new File(this.getName() + "_pidFile_" + rm.nextInt() + ".pid");
    tempFile.deleteOnExit();
    pidFile = tempFile.getName();

    // write to shell-script
    try {
      FileWriter fWriter = new FileWriter(shellScript);
      fWriter.write(
          "# rogue task\n" +
          "sleep 10\n" +
          "echo hello\n" +
          "if [ $1 -ne 0 ]\n" +
          "then\n" +
          " sh " + shellScript + " $(($1-1))\n" +
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
    ProcfsBasedProcessTree p = new ProcfsBasedProcessTree(pid);
    p = p.getProcessTree(); // initialize
    try {
      while (true) {
        LOG.info("ProcessTree: " + p.toString());
        long mem = p.getCumulativeVmem();
        LOG.info("Memory usage: " + mem + "bytes.");
        if (mem > memoryLimit) {
          p.destroy();
          break;
        }
        Thread.sleep(PROCESSTREE_RECONSTRUCTION_INTERVAL);
        p = p.getProcessTree(); // reconstruct
      }
    } catch (InterruptedException ie) {
      LOG.info("Interrupted.");
    }

    assertFalse("ProcessTree must have been gone", p.isAlive());

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
