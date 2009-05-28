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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

import junit.framework.TestCase;

public class TestProcfsBasedProcessTree extends TestCase {

  private static final Log LOG = LogFactory
      .getLog(TestProcfsBasedProcessTree.class);
  private static String TEST_ROOT_DIR = new Path(System.getProperty(
      "test.build.data", "/tmp")).toString().replace(' ', '+');

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
  
  public static class ProcessStatInfo {
    // sample stat in a single line : 3910 (gpm) S 1 3910 3910 0 -1 4194624 
    // 83 0 0 0 0 0 0 0 16 0 1 0 7852 2408448 88 4294967295 134512640 
    // 134590050 3220521392 3220520036 10975138 0 0 4096 134234626 
    // 4294967295 0 0 17 1 0 0
    String pid;
    String name;
    String ppid;
    String pgrpId;
    String session;
    String vmem;
    
    public ProcessStatInfo(String[] statEntries) {
      pid = statEntries[0];
      name = statEntries[1];
      ppid = statEntries[2];
      pgrpId = statEntries[3];
      session = statEntries[4];
      vmem = statEntries[5];
    }
    
    // construct a line that mimics the procfs stat file.
    // all unused numerical entries are set to 0.
    public String getStatLine() {
      return String.format("%s (%s) S %s %s %s 0 0 0" +
                      " 0 0 0 0 0 0 0 0 0 0 0 0 0 %s 0 0 0" +
                      " 0 0 0 0 0 0 0 0" +
                      " 0 0 0 0 0", 
                      pid, name, ppid, pgrpId, session, vmem);
    }
  }
  
  /**
   * A basic test that creates a few process directories and writes
   * stat files. Verifies that the virtual memory is correctly  
   * computed.
   * @throws IOException if there was a problem setting up the
   *                      fake procfs directories or files.
   */
  public void testVirtualMemoryForProcessTree() throws IOException {

    // test processes
    String[] pids = { "100", "200", "300", "400" };
    // create the fake procfs root directory. 
    File procfsRootDir = new File(TEST_ROOT_DIR, "proc");

    try {
      setupProcfsRootDir(procfsRootDir);
      setupPidDirs(procfsRootDir, pids);
      
      // create stat objects.
      // assuming processes 100, 200, 300 are in tree and 400 is not.
      ProcessStatInfo[] procInfos = new ProcessStatInfo[4];
      procInfos[0] = new ProcessStatInfo(new String[] 
                                  {"100", "proc1", "1", "100", "100", "100000"});
      procInfos[1] = new ProcessStatInfo(new String[] 
                                  {"200", "proc2", "100", "100", "100", "200000"});
      procInfos[2] = new ProcessStatInfo(new String[] 
                                  {"300", "proc3", "200", "100", "100", "300000"});
      procInfos[3] = new ProcessStatInfo(new String[] 
                                  {"400", "proc4", "1", "400", "400", "400000"});
      
      writeStatFiles(procfsRootDir, pids, procInfos);
      
      // crank up the process tree class.
      ProcfsBasedProcessTree processTree = 
          new ProcfsBasedProcessTree("100", procfsRootDir.getAbsolutePath());
      // build the process tree.
      processTree.getProcessTree();
      
      // verify cumulative memory
      assertEquals("Cumulative memory does not match", 
              Long.parseLong("600000"), processTree.getCumulativeVmem());
    } finally {
      FileUtil.fullyDelete(procfsRootDir);
    }
  }
  
  /**
   * Tests that cumulative memory is computed only for
   * processes older than a given age.
   * @throws IOException if there was a problem setting up the
   *                      fake procfs directories or files.
   */
  public void testVMemForOlderProcesses() throws IOException {
    // initial list of processes
    String[] pids = { "100", "200", "300", "400" };
    // create the fake procfs root directory. 
    File procfsRootDir = new File(TEST_ROOT_DIR, "proc");

    try {
      setupProcfsRootDir(procfsRootDir);
      setupPidDirs(procfsRootDir, pids);
      
      // create stat objects.
      // assuming 100, 200 and 400 are in tree, 300 is not.
      ProcessStatInfo[] procInfos = new ProcessStatInfo[4];
      procInfos[0] = new ProcessStatInfo(new String[] 
                                  {"100", "proc1", "1", "100", "100", "100000"});
      procInfos[1] = new ProcessStatInfo(new String[] 
                                  {"200", "proc2", "100", "100", "100", "200000"});
      procInfos[2] = new ProcessStatInfo(new String[] 
                                  {"300", "proc3", "1", "300", "300", "300000"});
      procInfos[3] = new ProcessStatInfo(new String[] 
                                  {"400", "proc4", "100", "100", "100", "400000"});
      
      writeStatFiles(procfsRootDir, pids, procInfos);
      
      // crank up the process tree class.
      ProcfsBasedProcessTree processTree = 
          new ProcfsBasedProcessTree("100", procfsRootDir.getAbsolutePath());
      // build the process tree.
      processTree.getProcessTree();
      
      // verify cumulative memory
      assertEquals("Cumulative memory does not match", 
              Long.parseLong("700000"), processTree.getCumulativeVmem());
      
      // write one more process as child of 100.
      String[] newPids = { "500" };
      setupPidDirs(procfsRootDir, newPids);
      
      ProcessStatInfo[] newProcInfos = new ProcessStatInfo[1];
      newProcInfos[0] = new ProcessStatInfo(new String[]
                             {"500", "proc5", "100", "100", "100", "500000"});
      writeStatFiles(procfsRootDir, newPids, newProcInfos);
      
      // check vmem includes the new process.
      processTree.getProcessTree();
      assertEquals("Cumulative memory does not include new process",
              Long.parseLong("1200000"), processTree.getCumulativeVmem());
      
      // however processes older than 1 iteration will retain the older value
      assertEquals("Cumulative memory shouldn't have included new process",
              Long.parseLong("700000"), processTree.getCumulativeVmem(1));
      
      // one more process
      newPids = new String[]{ "600" };
      setupPidDirs(procfsRootDir, newPids);
      
      newProcInfos = new ProcessStatInfo[1];
      newProcInfos[0] = new ProcessStatInfo(new String[]
                                     {"600", "proc6", "100", "100", "100", "600000"});
      writeStatFiles(procfsRootDir, newPids, newProcInfos);

      // refresh process tree
      processTree.getProcessTree();
      
      // processes older than 2 iterations should be same as before.
      assertEquals("Cumulative memory shouldn't have included new processes",
          Long.parseLong("700000"), processTree.getCumulativeVmem(2));
      
      // processes older than 1 iteration should not include new process,
      // but include process 500
      assertEquals("Cumulative memory shouldn't have included new processes",
          Long.parseLong("1200000"), processTree.getCumulativeVmem(1));
      
      // no processes older than 3 iterations, this should be 0
      assertEquals("Getting non-zero vmem for processes older than 3 iterations",
                    0L, processTree.getCumulativeVmem(3));
    } finally {
      FileUtil.fullyDelete(procfsRootDir);
    }
  }

  /**
   * Create a directory to mimic the procfs file system's root.
   * @param procfsRootDir root directory to create.
   * @throws IOException if could not delete the procfs root directory
   */
  public static void setupProcfsRootDir(File procfsRootDir) 
                                        throws IOException { 
    // cleanup any existing process root dir.
    if (procfsRootDir.exists()) {
      assertTrue(FileUtil.fullyDelete(procfsRootDir));  
    }

    // create afresh
    assertTrue(procfsRootDir.mkdirs());
  }

  /**
   * Create PID directories under the specified procfs root directory
   * @param procfsRootDir root directory of procfs file system
   * @param pids the PID directories to create.
   * @throws IOException If PID dirs could not be created
   */
  public static void setupPidDirs(File procfsRootDir, String[] pids) 
                      throws IOException {
    for (String pid : pids) {
      File pidDir = new File(procfsRootDir, pid);
      pidDir.mkdir();
      if (!pidDir.exists()) {
        throw new IOException ("couldn't make process directory under " +
            "fake procfs");
      } else {
        LOG.info("created pid dir");
      }
    }
  }
  
  /**
   * Write stat files under the specified pid directories with data
   * setup in the corresponding ProcessStatInfo objects
   * @param procfsRootDir root directory of procfs file system
   * @param pids the PID directories under which to create the stat file
   * @param procs corresponding ProcessStatInfo objects whose data should be
   *              written to the stat files.
   * @throws IOException if stat files could not be written
   */
  public static void writeStatFiles(File procfsRootDir, String[] pids, 
                              ProcessStatInfo[] procs) throws IOException {
    for (int i=0; i<pids.length; i++) {
      File statFile = new File(new File(procfsRootDir, pids[i]), "stat");
      BufferedWriter bw = null;
      try {
        FileWriter fw = new FileWriter(statFile);
        bw = new BufferedWriter(fw);
        bw.write(procs[i].getStatLine());
        LOG.info("wrote stat file for " + pids[i] + 
                  " with contents: " + procs[i].getStatLine());
      } finally {
        // not handling exception - will throw an error and fail the test.
        if (bw != null) {
          bw.close();
        }
      }
    }
  }
}
