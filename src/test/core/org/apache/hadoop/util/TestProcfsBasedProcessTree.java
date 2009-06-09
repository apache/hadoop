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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

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
    return getPidFromPidFile(pidFile);
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

  /**
   * Get PID from a pid-file.
   * 
   * @param pidFileName
   *          Name of the pid-file.
   * @return the PID string read from the pid-file. Returns null if the
   *         pidFileName points to a non-existing file or if read fails from the
   *         file.
   */
  public static String getPidFromPidFile(String pidFileName) {
    BufferedReader pidFile = null;
    FileReader fReader = null;
    String pid = null;

    try {
      fReader = new FileReader(pidFileName);
      pidFile = new BufferedReader(fReader);
    } catch (FileNotFoundException f) {
      LOG.debug("PidFile doesn't exist : " + pidFileName);
      return pid;
    }

    try {
      pid = pidFile.readLine();
    } catch (IOException i) {
      LOG.error("Failed to read from " + pidFileName);
    } finally {
      try {
        if (fReader != null) {
          fReader.close();
        }
        try {
          if (pidFile != null) {
            pidFile.close();
          }
        } catch (IOException i) {
          LOG.warn("Error closing the stream " + pidFile);
        }
      } catch (IOException i) {
        LOG.warn("Error closing the stream " + fReader);
      }
    }
    return pid;
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
          new ProcfsBasedProcessTree("100", true, 100L, 
                                  procfsRootDir.getAbsolutePath());
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
          new ProcfsBasedProcessTree("100", true, 100L, 
                                  procfsRootDir.getAbsolutePath());
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
