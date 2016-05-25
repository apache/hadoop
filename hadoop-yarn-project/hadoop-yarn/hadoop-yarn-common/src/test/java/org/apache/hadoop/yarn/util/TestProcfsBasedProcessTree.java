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

package org.apache.hadoop.yarn.util;

import static org.apache.hadoop.yarn.util.ProcfsBasedProcessTree.KB_TO_BYTES;
import static org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree.UNAVAILABLE;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ProcfsBasedProcessTree.MemInfo;
import org.apache.hadoop.yarn.util.ProcfsBasedProcessTree.ProcessSmapMemoryInfo;
import org.apache.hadoop.yarn.util.ProcfsBasedProcessTree.ProcessTreeSmapMemInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A JUnit test to test ProcfsBasedProcessTree.
 */
public class TestProcfsBasedProcessTree {

  private static final Log LOG = LogFactory
    .getLog(TestProcfsBasedProcessTree.class);
  protected static File TEST_ROOT_DIR = new File("target",
    TestProcfsBasedProcessTree.class.getName() + "-localDir");

  private ShellCommandExecutor shexec = null;
  private String pidFile, lowestDescendant, lostDescendant;
  private String shellScript;

  private static final int N = 6; // Controls the RogueTask

  private class RogueTaskThread extends Thread {
    public void run() {
      try {
        Vector<String> args = new Vector<String>();
        if (isSetsidAvailable()) {
          args.add("setsid");
        }
        args.add("bash");
        args.add("-c");
        args.add(" echo $$ > " + pidFile + "; sh " + shellScript + " " + N
            + ";");
        shexec = new ShellCommandExecutor(args.toArray(new String[0]));
        shexec.execute();
      } catch (ExitCodeException ee) {
        LOG.info("Shell Command exit with a non-zero exit code. This is"
            + " expected as we are killing the subprocesses of the"
            + " task intentionally. " + ee);
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

  @Before
  public void setup() throws IOException {
    assumeTrue(Shell.LINUX);
    FileContext.getLocalFSFileContext().delete(
      new Path(TEST_ROOT_DIR.getAbsolutePath()), true);
  }

  @Test(timeout = 30000)
  @SuppressWarnings("deprecation")
  public void testProcessTree() throws Exception {
    try {
      Assert.assertTrue(ProcfsBasedProcessTree.isAvailable());
    } catch (Exception e) {
      LOG.info(StringUtils.stringifyException(e));
      Assert.assertTrue("ProcfsBaseProcessTree should be available on Linux",
        false);
      return;
    }
    // create shell script
    Random rm = new Random();
    File tempFile =
        new File(TEST_ROOT_DIR, getClass().getName() + "_shellScript_"
            + rm.nextInt() + ".sh");
    tempFile.deleteOnExit();
    shellScript = TEST_ROOT_DIR + File.separator + tempFile.getName();

    // create pid file
    tempFile =
        new File(TEST_ROOT_DIR, getClass().getName() + "_pidFile_"
            + rm.nextInt() + ".pid");
    tempFile.deleteOnExit();
    pidFile = TEST_ROOT_DIR + File.separator + tempFile.getName();

    lowestDescendant =
        TEST_ROOT_DIR + File.separator + "lowestDescendantPidFile";
    lostDescendant =
        TEST_ROOT_DIR + File.separator + "lostDescendantPidFile";

    // write to shell-script
    File file = new File(shellScript);
    FileUtils.writeStringToFile(file, "# rogue task\n" + "sleep 1\n" + "echo hello\n"
        + "if [ $1 -ne 0 ]\n" + "then\n" + " sh " + shellScript
        + " $(($1-1))\n" + "else\n" + " echo $$ > " + lowestDescendant + "\n"
        + "(sleep 300&\n"
        + "echo $! > " + lostDescendant + ")\n"
        + " while true\n do\n" + "  sleep 5\n" + " done\n" + "fi");

    Thread t = new RogueTaskThread();
    t.start();
    String pid = getRogueTaskPID();
    LOG.info("Root process pid: " + pid);
    ProcfsBasedProcessTree p = createProcessTree(pid);
    p.updateProcessTree(); // initialize
    LOG.info("ProcessTree: " + p.toString());

    File leaf = new File(lowestDescendant);
    // wait till lowest descendant process of Rougue Task starts execution
    while (!leaf.exists()) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException ie) {
        break;
      }
    }

    p.updateProcessTree(); // reconstruct
    LOG.info("ProcessTree: " + p.toString());

    // Verify the orphaned pid is In process tree
    String lostpid = getPidFromPidFile(lostDescendant);
    LOG.info("Orphaned pid: " + lostpid);
    Assert.assertTrue("Child process owned by init escaped process tree.",
       p.contains(lostpid));

    // Get the process-tree dump
    String processTreeDump = p.getProcessTreeDump();

    // destroy the process and all its subprocesses
    destroyProcessTree(pid);

    boolean isAlive = true;
    for (int tries = 100; tries > 0; tries--) {
      if (isSetsidAvailable()) {// whole processtree
        isAlive = isAnyProcessInTreeAlive(p);
      } else {// process
        isAlive = isAlive(pid);
      }
      if (!isAlive) {
        break;
      }
      Thread.sleep(100);
    }
    if (isAlive) {
      fail("ProcessTree shouldn't be alive");
    }

    LOG.info("Process-tree dump follows: \n" + processTreeDump);
    Assert.assertTrue("Process-tree dump doesn't start with a proper header",
      processTreeDump.startsWith("\t|- PID PPID PGRPID SESSID CMD_NAME "
          + "USER_MODE_TIME(MILLIS) SYSTEM_TIME(MILLIS) VMEM_USAGE(BYTES) "
          + "RSSMEM_USAGE(PAGES) FULL_CMD_LINE\n"));
    for (int i = N; i >= 0; i--) {
      String cmdLineDump =
          "\\|- [0-9]+ [0-9]+ [0-9]+ [0-9]+ \\(sh\\)"
              + " [0-9]+ [0-9]+ [0-9]+ [0-9]+ sh " + shellScript + " " + i;
      Pattern pat = Pattern.compile(cmdLineDump);
      Matcher mat = pat.matcher(processTreeDump);
      Assert.assertTrue("Process-tree dump doesn't contain the cmdLineDump of "
          + i + "th process!", mat.find());
    }

    // Not able to join thread sometimes when forking with large N.
    try {
      t.join(2000);
      LOG.info("RogueTaskThread successfully joined.");
    } catch (InterruptedException ie) {
      LOG.info("Interrupted while joining RogueTaskThread.");
    }

    // ProcessTree is gone now. Any further calls should be sane.
    p.updateProcessTree();
    Assert.assertFalse("ProcessTree must have been gone", isAlive(pid));
    Assert.assertTrue(
      "vmem for the gone-process is " + p.getVirtualMemorySize()
          + " . It should be UNAVAILABLE(-1).",
          p.getVirtualMemorySize() == UNAVAILABLE);
    Assert.assertTrue(
      "vmem (old API) for the gone-process is " + p.getCumulativeVmem()
          + " . It should be UNAVAILABLE(-1).",
          p.getCumulativeVmem() == UNAVAILABLE);
    Assert.assertTrue(p.toString().equals("[ ]"));
  }

  protected ProcfsBasedProcessTree createProcessTree(String pid) {
    return new ProcfsBasedProcessTree(pid);
  }

  protected ProcfsBasedProcessTree createProcessTree(String pid,
      String procfsRootDir, Clock clock) {
    return new ProcfsBasedProcessTree(pid, procfsRootDir, clock);
  }

  protected void destroyProcessTree(String pid) throws IOException {
    sendSignal("-"+pid, 9);
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
    String vmem = "0";
    String rssmemPage = "0";
    String utime = "0";
    String stime = "0";

    public ProcessStatInfo(String[] statEntries) {
      pid = statEntries[0];
      name = statEntries[1];
      ppid = statEntries[2];
      pgrpId = statEntries[3];
      session = statEntries[4];
      vmem = statEntries[5];
      if (statEntries.length > 6) {
        rssmemPage = statEntries[6];
      }
      if (statEntries.length > 7) {
        utime = statEntries[7];
        stime = statEntries[8];
      }
    }

    // construct a line that mimics the procfs stat file.
    // all unused numerical entries are set to 0.
    public String getStatLine() {
      return String.format("%s (%s) S %s %s %s 0 0 0"
          + " 0 0 0 0 %s %s 0 0 0 0 0 0 0 %s %s 0 0" + " 0 0 0 0 0 0 0 0"
          + " 0 0 0 0 0", pid, name, ppid, pgrpId, session, utime, stime, vmem,
        rssmemPage);
    }
  }

  public ProcessSmapMemoryInfo constructMemoryMappingInfo(String address,
      String[] entries) {
    ProcessSmapMemoryInfo info = new ProcessSmapMemoryInfo(address);
    info.setMemInfo(MemInfo.SIZE.name(), entries[0]);
    info.setMemInfo(MemInfo.RSS.name(), entries[1]);
    info.setMemInfo(MemInfo.PSS.name(), entries[2]);
    info.setMemInfo(MemInfo.SHARED_CLEAN.name(), entries[3]);
    info.setMemInfo(MemInfo.SHARED_DIRTY.name(), entries[4]);
    info.setMemInfo(MemInfo.PRIVATE_CLEAN.name(), entries[5]);
    info.setMemInfo(MemInfo.PRIVATE_DIRTY.name(), entries[6]);
    info.setMemInfo(MemInfo.REFERENCED.name(), entries[7]);
    info.setMemInfo(MemInfo.ANONYMOUS.name(), entries[8]);
    info.setMemInfo(MemInfo.ANON_HUGE_PAGES.name(), entries[9]);
    info.setMemInfo(MemInfo.SWAP.name(), entries[10]);
    info.setMemInfo(MemInfo.KERNEL_PAGE_SIZE.name(), entries[11]);
    info.setMemInfo(MemInfo.MMU_PAGE_SIZE.name(), entries[12]);
    return info;
  }

  public void createMemoryMappingInfo(ProcessTreeSmapMemInfo[] procMemInfo) {
    for (int i = 0; i < procMemInfo.length; i++) {
      // Construct 4 memory mappings per process.
      // As per min(Shared_Dirty, Pss) + Private_Clean + Private_Dirty
      // and not including r--s, r-xs, we should get 100 KB per process
      List<ProcessSmapMemoryInfo> memoryMappingList =
          procMemInfo[i].getMemoryInfoList();
      memoryMappingList.add(constructMemoryMappingInfo(
        "7f56c177c000-7f56c177d000 "
            + "rw-p 00010000 08:02 40371558                   "
            + "/grid/0/jdk1.7.0_25/jre/lib/amd64/libnio.so",
        new String[] { "4", "4", "25", "4", "25", "15", "10", "4", "0", "0",
            "0", "4", "4" }));
      memoryMappingList.add(constructMemoryMappingInfo(
        "7fb09382e000-7fb09382f000 r--s 00003000 " + "08:02 25953545",
        new String[] { "4", "4", "25", "4", "0", "15", "10", "4", "0", "0",
            "0", "4", "4" }));
      memoryMappingList.add(constructMemoryMappingInfo(
        "7e8790000-7e8b80000 r-xs 00000000 00:00 0", new String[] { "4", "4",
            "25", "4", "0", "15", "10", "4", "0", "0", "0", "4", "4" }));
      memoryMappingList.add(constructMemoryMappingInfo(
        "7da677000-7e0dcf000 rw-p 00000000 00:00 0", new String[] { "4", "4",
            "25", "4", "50", "15", "10", "4", "0", "0", "0", "4", "4" }));
    }
  }

  /**
   * A basic test that creates a few process directories and writes stat files.
   * Verifies that the cpu time and memory is correctly computed.
   *
   * @throws IOException
   *           if there was a problem setting up the fake procfs directories or
   *           files.
   */
  @Test(timeout = 30000)
  @SuppressWarnings("deprecation")
  public void testCpuAndMemoryForProcessTree() throws IOException {

    // test processes
    String[] pids = { "100", "200", "300", "400" };
    ControlledClock testClock = new ControlledClock();
    testClock.setTime(0);
    // create the fake procfs root directory.
    File procfsRootDir = new File(TEST_ROOT_DIR, "proc");

    try {
      setupProcfsRootDir(procfsRootDir);
      setupPidDirs(procfsRootDir, pids);

      // create stat objects.
      // assuming processes 100, 200, 300 are in tree and 400 is not.
      ProcessStatInfo[] procInfos = new ProcessStatInfo[4];
      procInfos[0] =
          new ProcessStatInfo(new String[]{"100", "proc1", "1", "100", "100",
              "100000", "100", "1000", "200"});
      procInfos[1] =
          new ProcessStatInfo(new String[]{"200", "process two", "100", "100",
              "100", "200000", "200", "2000", "400"});
      procInfos[2] =
          new ProcessStatInfo(new String[]{"300", "proc3", "200", "100",
              "100", "300000", "300", "3000", "600"});
      procInfos[3] =
          new ProcessStatInfo(new String[]{"400", "proc4", "1", "400", "400",
              "400000", "400", "4000", "800"});

      ProcessTreeSmapMemInfo[] memInfo = new ProcessTreeSmapMemInfo[4];
      memInfo[0] = new ProcessTreeSmapMemInfo("100");
      memInfo[1] = new ProcessTreeSmapMemInfo("200");
      memInfo[2] = new ProcessTreeSmapMemInfo("300");
      memInfo[3] = new ProcessTreeSmapMemInfo("400");
      createMemoryMappingInfo(memInfo);
      writeStatFiles(procfsRootDir, pids, procInfos, memInfo);

      // crank up the process tree class.
      Configuration conf = new Configuration();
      ProcfsBasedProcessTree processTree =
          createProcessTree("100", procfsRootDir.getAbsolutePath(), testClock);
      processTree.setConf(conf);
      // build the process tree.
      processTree.updateProcessTree();

      // verify virtual memory
      Assert.assertEquals("Virtual memory does not match", 600000L,
        processTree.getVirtualMemorySize());

      // verify rss memory
      long cumuRssMem =
          ProcfsBasedProcessTree.PAGE_SIZE > 0
              ? 600L * ProcfsBasedProcessTree.PAGE_SIZE : 
                  ResourceCalculatorProcessTree.UNAVAILABLE;
      Assert.assertEquals("rss memory does not match", cumuRssMem,
        processTree.getRssMemorySize());
      // verify old API
      Assert.assertEquals("rss memory (old API) does not match", cumuRssMem,
        processTree.getCumulativeRssmem());

      // verify cumulative cpu time
      long cumuCpuTime =
          ProcfsBasedProcessTree.JIFFY_LENGTH_IN_MILLIS > 0
              ? 7200L * ProcfsBasedProcessTree.JIFFY_LENGTH_IN_MILLIS : 0L;
      Assert.assertEquals("Cumulative cpu time does not match", cumuCpuTime,
        processTree.getCumulativeCpuTime());

      // verify CPU usage
      Assert.assertEquals("Percent CPU time should be set to -1 initially",
          -1.0, processTree.getCpuUsagePercent(),
          0.01);

      // Check by enabling smaps
      setSmapsInProceTree(processTree, true);
      // RSS=Min(shared_dirty,PSS)+PrivateClean+PrivateDirty (exclude r-xs,
      // r--s)
      Assert.assertEquals("rss memory does not match",
        (100 * KB_TO_BYTES * 3), processTree.getRssMemorySize());
      // verify old API
      Assert.assertEquals("rss memory (old API) does not match",
        (100 * KB_TO_BYTES * 3), processTree.getCumulativeRssmem());

      // test the cpu time again to see if it cumulates
      procInfos[0] =
          new ProcessStatInfo(new String[]{"100", "proc1", "1", "100", "100",
              "100000", "100", "2000", "300"});
      procInfos[1] =
          new ProcessStatInfo(new String[]{"200", "process two", "100", "100",
              "100", "200000", "200", "3000", "500"});
      writeStatFiles(procfsRootDir, pids, procInfos, memInfo);

      long elapsedTimeBetweenUpdatesMsec = 200000;
      testClock.setTime(elapsedTimeBetweenUpdatesMsec);
      // build the process tree.
      processTree.updateProcessTree();

      // verify cumulative cpu time again
      long prevCumuCpuTime = cumuCpuTime;
      cumuCpuTime =
          ProcfsBasedProcessTree.JIFFY_LENGTH_IN_MILLIS > 0
              ? 9400L * ProcfsBasedProcessTree.JIFFY_LENGTH_IN_MILLIS : 0L;
      Assert.assertEquals("Cumulative cpu time does not match", cumuCpuTime,
        processTree.getCumulativeCpuTime());

      double expectedCpuUsagePercent =
          (ProcfsBasedProcessTree.JIFFY_LENGTH_IN_MILLIS > 0) ?
              (cumuCpuTime - prevCumuCpuTime) * 100.0 /
                  elapsedTimeBetweenUpdatesMsec : 0;
      // expectedCpuUsagePercent is given by (94000L - 72000) * 100/
      //    200000;
      // which in this case is 11. Lets verify that first
      Assert.assertEquals(11, expectedCpuUsagePercent, 0.001);
      Assert.assertEquals("Percent CPU time is not correct expected " +
              expectedCpuUsagePercent, expectedCpuUsagePercent,
          processTree.getCpuUsagePercent(),
          0.01);
    } finally {
      FileUtil.fullyDelete(procfsRootDir);
    }
  }

  private void setSmapsInProceTree(ProcfsBasedProcessTree processTree,
      boolean enableFlag) {
    Configuration conf = processTree.getConf();
    if (conf == null) {
      conf = new Configuration();
    }
    conf.setBoolean(YarnConfiguration.PROCFS_USE_SMAPS_BASED_RSS_ENABLED, enableFlag);
    processTree.setConf(conf);
    processTree.updateProcessTree();
  }

  /**
   * Tests that cumulative memory is computed only for processes older than a
   * given age.
   *
   * @throws IOException
   *           if there was a problem setting up the fake procfs directories or
   *           files.
   */
  @Test(timeout = 30000)
  public void testMemForOlderProcesses() throws IOException {
    testMemForOlderProcesses(false);
    testMemForOlderProcesses(true);
  }

  @SuppressWarnings("deprecation")
  private void testMemForOlderProcesses(boolean smapEnabled) throws IOException {
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
      procInfos[0] =
          new ProcessStatInfo(new String[]{"100", "proc1", "1", "100", "100",
              "100000", "100"});
      procInfos[1] =
          new ProcessStatInfo(new String[]{"200", "process two", "100", "100",
              "100", "200000", "200"});
      procInfos[2] =
          new ProcessStatInfo(new String[]{"300", "proc3", "1", "300", "300",
              "300000", "300"});
      procInfos[3] =
          new ProcessStatInfo(new String[]{"400", "proc4", "100", "100",
              "100", "400000", "400"});
      // write smap information invariably for testing
      ProcessTreeSmapMemInfo[] memInfo = new ProcessTreeSmapMemInfo[4];
      memInfo[0] = new ProcessTreeSmapMemInfo("100");
      memInfo[1] = new ProcessTreeSmapMemInfo("200");
      memInfo[2] = new ProcessTreeSmapMemInfo("300");
      memInfo[3] = new ProcessTreeSmapMemInfo("400");
      createMemoryMappingInfo(memInfo);
      writeStatFiles(procfsRootDir, pids, procInfos, memInfo);

      // crank up the process tree class.
      ProcfsBasedProcessTree processTree =
          createProcessTree("100", procfsRootDir.getAbsolutePath(),
              SystemClock.getInstance());
      setSmapsInProceTree(processTree, smapEnabled);

      // verify virtual memory
      Assert.assertEquals("Virtual memory does not match", 700000L,
        processTree.getVirtualMemorySize());
      Assert.assertEquals("Virtual memory (old API) does not match", 700000L,
        processTree.getCumulativeVmem());

      // write one more process as child of 100.
      String[] newPids = { "500" };
      setupPidDirs(procfsRootDir, newPids);

      ProcessStatInfo[] newProcInfos = new ProcessStatInfo[1];
      newProcInfos[0] =
          new ProcessStatInfo(new String[] { "500", "proc5", "100", "100",
              "100", "500000", "500" });
      ProcessTreeSmapMemInfo[] newMemInfos = new ProcessTreeSmapMemInfo[1];
      newMemInfos[0] = new ProcessTreeSmapMemInfo("500");
      createMemoryMappingInfo(newMemInfos);
      writeStatFiles(procfsRootDir, newPids, newProcInfos, newMemInfos);

      // check memory includes the new process.
      processTree.updateProcessTree();
      Assert.assertEquals("vmem does not include new process",
        1200000L, processTree.getVirtualMemorySize());
      Assert.assertEquals("vmem (old API) does not include new process",
        1200000L, processTree.getCumulativeVmem());
      if (!smapEnabled) {
        long cumuRssMem =
            ProcfsBasedProcessTree.PAGE_SIZE > 0
                ? 1200L * ProcfsBasedProcessTree.PAGE_SIZE : 
                    ResourceCalculatorProcessTree.UNAVAILABLE;
        Assert.assertEquals("rssmem does not include new process",
          cumuRssMem, processTree.getRssMemorySize());
        // verify old API
        Assert.assertEquals("rssmem (old API) does not include new process",
          cumuRssMem, processTree.getCumulativeRssmem());
      } else {
        Assert.assertEquals("rssmem does not include new process",
          100 * KB_TO_BYTES * 4, processTree.getRssMemorySize());
        // verify old API
        Assert.assertEquals("rssmem (old API) does not include new process",
          100 * KB_TO_BYTES * 4, processTree.getCumulativeRssmem());
      }

      // however processes older than 1 iteration will retain the older value
      Assert.assertEquals(
        "vmem shouldn't have included new process", 700000L,
        processTree.getVirtualMemorySize(1));
      // verify old API
      Assert.assertEquals(
          "vmem (old API) shouldn't have included new process", 700000L,
          processTree.getCumulativeVmem(1));
      if (!smapEnabled) {
        long cumuRssMem =
            ProcfsBasedProcessTree.PAGE_SIZE > 0
                ? 700L * ProcfsBasedProcessTree.PAGE_SIZE : 
                    ResourceCalculatorProcessTree.UNAVAILABLE;
        Assert.assertEquals(
          "rssmem shouldn't have included new process", cumuRssMem,
          processTree.getRssMemorySize(1));
        // Verify old API
        Assert.assertEquals(
          "rssmem (old API) shouldn't have included new process", cumuRssMem,
          processTree.getCumulativeRssmem(1));
      } else {
        Assert.assertEquals(
          "rssmem shouldn't have included new process",
          100 * KB_TO_BYTES * 3, processTree.getRssMemorySize(1));
        // Verify old API
        Assert.assertEquals(
          "rssmem (old API) shouldn't have included new process",
          100 * KB_TO_BYTES * 3, processTree.getCumulativeRssmem(1));
      }

      // one more process
      newPids = new String[] { "600" };
      setupPidDirs(procfsRootDir, newPids);

      newProcInfos = new ProcessStatInfo[1];
      newProcInfos[0] =
          new ProcessStatInfo(new String[] { "600", "proc6", "100", "100",
              "100", "600000", "600" });
      newMemInfos = new ProcessTreeSmapMemInfo[1];
      newMemInfos[0] = new ProcessTreeSmapMemInfo("600");
      createMemoryMappingInfo(newMemInfos);
      writeStatFiles(procfsRootDir, newPids, newProcInfos, newMemInfos);

      // refresh process tree
      processTree.updateProcessTree();

      // processes older than 2 iterations should be same as before.
      Assert.assertEquals(
        "vmem shouldn't have included new processes", 700000L,
        processTree.getVirtualMemorySize(2));
      // verify old API
      Assert.assertEquals(
        "vmem (old API) shouldn't have included new processes", 700000L,
        processTree.getCumulativeVmem(2));
      if (!smapEnabled) {
        long cumuRssMem =
            ProcfsBasedProcessTree.PAGE_SIZE > 0
                ? 700L * ProcfsBasedProcessTree.PAGE_SIZE : 
                    ResourceCalculatorProcessTree.UNAVAILABLE;
        Assert.assertEquals(
          "rssmem shouldn't have included new processes",
          cumuRssMem, processTree.getRssMemorySize(2));
        // Verify old API
        Assert.assertEquals(
          "rssmem (old API) shouldn't have included new processes",
          cumuRssMem, processTree.getCumulativeRssmem(2));
      } else {
        Assert.assertEquals(
          "rssmem shouldn't have included new processes",
          100 * KB_TO_BYTES * 3, processTree.getRssMemorySize(2));
        // Verify old API
        Assert.assertEquals(
          "rssmem (old API) shouldn't have included new processes",
          100 * KB_TO_BYTES * 3, processTree.getCumulativeRssmem(2));
      }

      // processes older than 1 iteration should not include new process,
      // but include process 500
      Assert.assertEquals(
        "vmem shouldn't have included new processes", 1200000L,
        processTree.getVirtualMemorySize(1));
      // verify old API
      Assert.assertEquals(
        "vmem (old API) shouldn't have included new processes", 1200000L,
        processTree.getCumulativeVmem(1));
      if (!smapEnabled) {
        long cumuRssMem =
            ProcfsBasedProcessTree.PAGE_SIZE > 0
                ? 1200L * ProcfsBasedProcessTree.PAGE_SIZE : 
                    ResourceCalculatorProcessTree.UNAVAILABLE;
        Assert.assertEquals(
          "rssmem shouldn't have included new processes",
          cumuRssMem, processTree.getRssMemorySize(1));
        // verify old API
        Assert.assertEquals(
          "rssmem (old API) shouldn't have included new processes",
          cumuRssMem, processTree.getCumulativeRssmem(1));
      } else {
        Assert.assertEquals(
          "rssmem shouldn't have included new processes",
          100 * KB_TO_BYTES * 4, processTree.getRssMemorySize(1));
        Assert.assertEquals(
          "rssmem (old API) shouldn't have included new processes",
          100 * KB_TO_BYTES * 4, processTree.getCumulativeRssmem(1));
      }

      // no processes older than 3 iterations
      Assert.assertEquals(
          "Getting non-zero vmem for processes older than 3 iterations",
          0, processTree.getVirtualMemorySize(3));
      // verify old API
      Assert.assertEquals(
          "Getting non-zero vmem (old API) for processes older than 3 iterations",
          0, processTree.getCumulativeVmem(3));
      Assert.assertEquals(
          "Getting non-zero rssmem for processes older than 3 iterations",
          0, processTree.getRssMemorySize(3));
      // verify old API
      Assert.assertEquals(
          "Getting non-zero rssmem (old API) for processes older than 3 iterations",
          0, processTree.getCumulativeRssmem(3));
    } finally {
      FileUtil.fullyDelete(procfsRootDir);
    }
  }

  /**
   * Verifies ProcfsBasedProcessTree.checkPidPgrpidForMatch() in case of
   * 'constructProcessInfo() returning null' by not writing stat file for the
   * mock process
   *
   * @throws IOException
   *           if there was a problem setting up the fake procfs directories or
   *           files.
   */
  @Test(timeout = 30000)
  public void testDestroyProcessTree() throws IOException {
    // test process
    String pid = "100";
    // create the fake procfs root directory.
    File procfsRootDir = new File(TEST_ROOT_DIR, "proc");

    try {
      setupProcfsRootDir(procfsRootDir);

      // crank up the process tree class.
      createProcessTree(pid, procfsRootDir.getAbsolutePath(),
          SystemClock.getInstance());

      // Let us not create stat file for pid 100.
      Assert.assertTrue(ProcfsBasedProcessTree.checkPidPgrpidForMatch(pid,
        procfsRootDir.getAbsolutePath()));
    } finally {
      FileUtil.fullyDelete(procfsRootDir);
    }
  }

  /**
   * Test the correctness of process-tree dump.
   *
   * @throws IOException
   */
  @Test(timeout = 30000)
  public void testProcessTreeDump() throws IOException {

    String[] pids = { "100", "200", "300", "400", "500", "600" };

    File procfsRootDir = new File(TEST_ROOT_DIR, "proc");

    try {
      setupProcfsRootDir(procfsRootDir);
      setupPidDirs(procfsRootDir, pids);

      int numProcesses = pids.length;
      // Processes 200, 300, 400 and 500 are descendants of 100. 600 is not.
      ProcessStatInfo[] procInfos = new ProcessStatInfo[numProcesses];
      procInfos[0] =
          new ProcessStatInfo(new String[]{"100", "proc1", "1", "100", "100",
              "100000", "100", "1000", "200"});
      procInfos[1] =
          new ProcessStatInfo(new String[]{"200", "process two", "100", "100",
              "100", "200000", "200", "2000", "400"});
      procInfos[2] =
          new ProcessStatInfo(new String[]{"300", "proc3", "200", "100",
              "100", "300000", "300", "3000", "600"});
      procInfos[3] =
          new ProcessStatInfo(new String[]{"400", "proc4", "200", "100",
              "100", "400000", "400", "4000", "800"});
      procInfos[4] =
          new ProcessStatInfo(new String[]{"500", "proc5", "400", "100",
              "100", "400000", "400", "4000", "800"});
      procInfos[5] =
          new ProcessStatInfo(new String[]{"600", "proc6", "1", "1", "1",
              "400000", "400", "4000", "800"});

      ProcessTreeSmapMemInfo[] memInfos = new ProcessTreeSmapMemInfo[6];
      memInfos[0] = new ProcessTreeSmapMemInfo("100");
      memInfos[1] = new ProcessTreeSmapMemInfo("200");
      memInfos[2] = new ProcessTreeSmapMemInfo("300");
      memInfos[3] = new ProcessTreeSmapMemInfo("400");
      memInfos[4] = new ProcessTreeSmapMemInfo("500");
      memInfos[5] = new ProcessTreeSmapMemInfo("600");

      String[] cmdLines = new String[numProcesses];
      cmdLines[0] = "proc1 arg1 arg2";
      cmdLines[1] = "process two arg3 arg4";
      cmdLines[2] = "proc3 arg5 arg6";
      cmdLines[3] = "proc4 arg7 arg8";
      cmdLines[4] = "proc5 arg9 arg10";
      cmdLines[5] = "proc6 arg11 arg12";

      createMemoryMappingInfo(memInfos);
      writeStatFiles(procfsRootDir, pids, procInfos, memInfos);
      writeCmdLineFiles(procfsRootDir, pids, cmdLines);

      ProcfsBasedProcessTree processTree =
          createProcessTree("100", procfsRootDir.getAbsolutePath(),
              SystemClock.getInstance());
      // build the process tree.
      processTree.updateProcessTree();

      // Get the process-tree dump
      String processTreeDump = processTree.getProcessTreeDump();

      LOG.info("Process-tree dump follows: \n" + processTreeDump);
      Assert.assertTrue("Process-tree dump doesn't start with a proper header",
        processTreeDump.startsWith("\t|- PID PPID PGRPID SESSID CMD_NAME "
            + "USER_MODE_TIME(MILLIS) SYSTEM_TIME(MILLIS) VMEM_USAGE(BYTES) "
            + "RSSMEM_USAGE(PAGES) FULL_CMD_LINE\n"));
      for (int i = 0; i < 5; i++) {
        ProcessStatInfo p = procInfos[i];
        Assert.assertTrue(
          "Process-tree dump doesn't contain the cmdLineDump of process "
              + p.pid,
          processTreeDump.contains("\t|- " + p.pid + " " + p.ppid + " "
              + p.pgrpId + " " + p.session + " (" + p.name + ") " + p.utime
              + " " + p.stime + " " + p.vmem + " " + p.rssmemPage + " "
              + cmdLines[i]));
      }

      // 600 should not be in the dump
      ProcessStatInfo p = procInfos[5];
      Assert.assertFalse(
        "Process-tree dump shouldn't contain the cmdLineDump of process "
            + p.pid,
        processTreeDump.contains("\t|- " + p.pid + " " + p.ppid + " "
            + p.pgrpId + " " + p.session + " (" + p.name + ") " + p.utime + " "
            + p.stime + " " + p.vmem + " " + cmdLines[5]));
    } finally {
      FileUtil.fullyDelete(procfsRootDir);
    }
  }

  protected static boolean isSetsidAvailable() {
    ShellCommandExecutor shexec = null;
    boolean setsidSupported = true;
    try {
      String[] args = { "setsid", "bash", "-c", "echo $$" };
      shexec = new ShellCommandExecutor(args);
      shexec.execute();
    } catch (IOException ioe) {
      LOG.warn("setsid is not available on this machine. So not using it.");
      setsidSupported = false;
    } finally { // handle the exit code
      LOG.info("setsid exited with exit code " + shexec.getExitCode());
    }
    return setsidSupported;
  }

  /**
   * Is the root-process alive? Used only in tests.
   *
   * @return true if the root-process is alive, false otherwise.
   */
  private static boolean isAlive(String pid) {
    try {
      final String sigpid = isSetsidAvailable() ? "-" + pid : pid;
      try {
        sendSignal(sigpid, 0);
      } catch (ExitCodeException e) {
        return false;
      }
      return true;
    } catch (IOException ignored) {
    }
    return false;
  }

  private static void sendSignal(String pid, int signal) throws IOException {
    ShellCommandExecutor shexec = null;
    String[] arg = { "kill", "-" + signal, "--", pid };
    shexec = new ShellCommandExecutor(arg);
    shexec.execute();
  }

  /**
   * Is any of the subprocesses in the process-tree alive? Used only in tests.
   *
   * @return true if any of the processes in the process-tree is alive, false
   *         otherwise.
   */
  private static boolean isAnyProcessInTreeAlive(
      ProcfsBasedProcessTree processTree) {
    for (String pId : processTree.getCurrentProcessIDs()) {
      if (isAlive(pId)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Create a directory to mimic the procfs file system's root.
   *
   * @param procfsRootDir
   *          root directory to create.
   * @throws IOException
   *           if could not delete the procfs root directory
   */
  public static void setupProcfsRootDir(File procfsRootDir) throws IOException {
    // cleanup any existing process root dir.
    if (procfsRootDir.exists()) {
      Assert.assertTrue(FileUtil.fullyDelete(procfsRootDir));
    }

    // create afresh
    Assert.assertTrue(procfsRootDir.mkdirs());
  }

  /**
   * Create PID directories under the specified procfs root directory
   *
   * @param procfsRootDir
   *          root directory of procfs file system
   * @param pids
   *          the PID directories to create.
   * @throws IOException
   *           If PID dirs could not be created
   */
  public static void setupPidDirs(File procfsRootDir, String[] pids)
      throws IOException {
    for (String pid : pids) {
      File pidDir = new File(procfsRootDir, pid);
      pidDir.mkdir();
      if (!pidDir.exists()) {
        throw new IOException("couldn't make process directory under "
            + "fake procfs");
      } else {
        LOG.info("created pid dir");
      }
    }
  }

  /**
   * Write stat files under the specified pid directories with data setup in the
   * corresponding ProcessStatInfo objects
   *
   * @param procfsRootDir
   *          root directory of procfs file system
   * @param pids
   *          the PID directories under which to create the stat file
   * @param procs
   *          corresponding ProcessStatInfo objects whose data should be written
   *          to the stat files.
   * @throws IOException
   *           if stat files could not be written
   */
  public static void writeStatFiles(File procfsRootDir, String[] pids,
      ProcessStatInfo[] procs, ProcessTreeSmapMemInfo[] smaps)
      throws IOException {
    for (int i = 0; i < pids.length; i++) {
      File statFile =
          new File(new File(procfsRootDir, pids[i]),
            ProcfsBasedProcessTree.PROCFS_STAT_FILE);
      BufferedWriter bw = null;
      try {
        FileWriter fw = new FileWriter(statFile);
        bw = new BufferedWriter(fw);
        bw.write(procs[i].getStatLine());
        LOG.info("wrote stat file for " + pids[i] + " with contents: "
            + procs[i].getStatLine());
      } finally {
        // not handling exception - will throw an error and fail the test.
        if (bw != null) {
          bw.close();
        }
      }
      if (smaps != null) {
        File smapFile =
            new File(new File(procfsRootDir, pids[i]),
              ProcfsBasedProcessTree.SMAPS);
        bw = null;
        try {
          FileWriter fw = new FileWriter(smapFile);
          bw = new BufferedWriter(fw);
          bw.write(smaps[i].toString());
          bw.flush();
          LOG.info("wrote smap file for " + pids[i] + " with contents: "
              + smaps[i].toString());
        } finally {
          // not handling exception - will throw an error and fail the test.
          if (bw != null) {
            bw.close();
          }
        }
      }
    }
  }

  private static void writeCmdLineFiles(File procfsRootDir, String[] pids,
      String[] cmdLines) throws IOException {
    for (int i = 0; i < pids.length; i++) {
      File statFile =
          new File(new File(procfsRootDir, pids[i]),
            ProcfsBasedProcessTree.PROCFS_CMDLINE_FILE);
      BufferedWriter bw = null;
      try {
        bw = new BufferedWriter(new FileWriter(statFile));
        bw.write(cmdLines[i]);
        LOG.info("wrote command-line file for " + pids[i] + " with contents: "
            + cmdLines[i]);
      } finally {
        // not handling exception - will throw an error and fail the test.
        if (bw != null) {
          bw.close();
        }
      }
    }
  }
}
