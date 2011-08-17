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

package org.apache.hadoop.mapreduce.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.StringUtils;

/**
 * A Proc file-system based ProcessTree. Works only on Linux.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ProcfsBasedProcessTree extends ProcessTree {

  static final Log LOG = LogFactory
      .getLog(ProcfsBasedProcessTree.class);

  private static final String PROCFS = "/proc/";

  private static final Pattern PROCFS_STAT_FILE_FORMAT = Pattern .compile(
    "^([0-9-]+)\\s([^\\s]+)\\s[^\\s]\\s([0-9-]+)\\s([0-9-]+)\\s([0-9-]+)\\s" +
    "([0-9-]+\\s){7}([0-9]+)\\s([0-9]+)\\s([0-9-]+\\s){7}([0-9]+)\\s([0-9]+)" +
    "(\\s[0-9-]+){15}");

  static final String PROCFS_STAT_FILE = "stat";
  static final String PROCFS_CMDLINE_FILE = "cmdline";
  public static final long PAGE_SIZE;
  static {
    ShellCommandExecutor shellExecutor =
            new ShellCommandExecutor(new String[]{"getconf",  "PAGESIZE"});
    long pageSize = -1;
    try {
      shellExecutor.execute();
      pageSize = Long.parseLong(shellExecutor.getOutput().replace("\n", ""));
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
    } finally {
      PAGE_SIZE = pageSize;
    }
  }
  public static final long JIFFY_LENGTH_IN_MILLIS; // in millisecond
  static {
    ShellCommandExecutor shellExecutor =
            new ShellCommandExecutor(new String[]{"getconf",  "CLK_TCK"});
    long jiffiesPerSecond = -1;
    try {
      shellExecutor.execute();
      jiffiesPerSecond = Long.parseLong(shellExecutor.getOutput().replace("\n", ""));
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
    } finally {
      JIFFY_LENGTH_IN_MILLIS = jiffiesPerSecond != -1 ?
                     Math.round(1000D / jiffiesPerSecond) : -1;
    }
  }

  // to enable testing, using this variable which can be configured
  // to a test directory.
  private String procfsDir;
  
  private Integer pid = -1;
  private Long cpuTime = 0L;
  private boolean setsidUsed = false;
  private long sleeptimeBeforeSigkill = DEFAULT_SLEEPTIME_BEFORE_SIGKILL;

  private Map<Integer, ProcessInfo> processTree = new HashMap<Integer, ProcessInfo>();

  public ProcfsBasedProcessTree(String pid) {
    this(pid, false, DEFAULT_SLEEPTIME_BEFORE_SIGKILL);
  }

  public ProcfsBasedProcessTree(String pid, boolean setsidUsed,
                                long sigkillInterval) {
    this(pid, setsidUsed, sigkillInterval, PROCFS);
  }

  /**
   * Build a new process tree rooted at the pid.
   * 
   * This method is provided mainly for testing purposes, where
   * the root of the proc file system can be adjusted.
   * 
   * @param pid root of the process tree
   * @param setsidUsed true, if setsid was used for the root pid
   * @param sigkillInterval how long to wait between a SIGTERM and SIGKILL 
   *                        when killing a process tree
   * @param procfsDir the root of a proc file system - only used for testing. 
   */
  public ProcfsBasedProcessTree(String pid, boolean setsidUsed,
                                long sigkillInterval, String procfsDir) {
    this.pid = getValidPID(pid);
    this.setsidUsed = setsidUsed;
    sleeptimeBeforeSigkill = sigkillInterval;
    this.procfsDir = procfsDir;
  }
  
  /**
   * Sets SIGKILL interval
   * @deprecated Use {@link ProcfsBasedProcessTree#ProcfsBasedProcessTree(
   *                  String, boolean, long)} instead
   * @param interval The time to wait before sending SIGKILL
   *                 after sending SIGTERM
   */
  @Deprecated
  public void setSigKillInterval(long interval) {
    sleeptimeBeforeSigkill = interval;
  }

  /**
   * Checks if the ProcfsBasedProcessTree is available on this system.
   * 
   * @return true if ProcfsBasedProcessTree is available. False otherwise.
   */
  public static boolean isAvailable() {
    try {
      String osName = System.getProperty("os.name");
      if (!osName.startsWith("Linux")) {
        LOG.info("ProcfsBasedProcessTree currently is supported only on "
            + "Linux.");
        return false;
      }
    } catch (SecurityException se) {
      LOG.warn("Failed to get Operating System name. " + se);
      return false;
    }
    return true;
  }

  /**
   * Get the process-tree with latest state. If the root-process is not alive,
   * an empty tree will be returned.
   * 
   * @return the process-tree with latest state.
   */
  public ProcfsBasedProcessTree getProcessTree() {
    if (pid != -1) {
      // Get the list of processes
      List<Integer> processList = getProcessList();

      Map<Integer, ProcessInfo> allProcessInfo = new HashMap<Integer, ProcessInfo>();
      
      // cache the processTree to get the age for processes
      Map<Integer, ProcessInfo> oldProcs = 
              new HashMap<Integer, ProcessInfo>(processTree);
      processTree.clear();

      ProcessInfo me = null;
      for (Integer proc : processList) {
        // Get information for each process
        ProcessInfo pInfo = new ProcessInfo(proc);
        if (constructProcessInfo(pInfo, procfsDir) != null) {
          allProcessInfo.put(proc, pInfo);
          if (proc.equals(this.pid)) {
            me = pInfo; // cache 'me'
            processTree.put(proc, pInfo);
          }
        }
      }

      if (me == null) {
        return this; 
      }

      // Add each process to its parent.
      for (Map.Entry<Integer, ProcessInfo> entry : allProcessInfo.entrySet()) {
        Integer pID = entry.getKey();
        if (pID != 1) {
          ProcessInfo pInfo = entry.getValue();
          ProcessInfo parentPInfo = allProcessInfo.get(pInfo.getPpid());
          if (parentPInfo != null) {
            parentPInfo.addChild(pInfo);
          }
        }
      }

      // now start constructing the process-tree
      LinkedList<ProcessInfo> pInfoQueue = new LinkedList<ProcessInfo>();
      pInfoQueue.addAll(me.getChildren());
      while (!pInfoQueue.isEmpty()) {
        ProcessInfo pInfo = pInfoQueue.remove();
        if (!processTree.containsKey(pInfo.getPid())) {
          processTree.put(pInfo.getPid(), pInfo);
        }
        pInfoQueue.addAll(pInfo.getChildren());
      }

      // update age values and compute the number of jiffies since last update
      for (Map.Entry<Integer, ProcessInfo> procs : processTree.entrySet()) {
        ProcessInfo oldInfo = oldProcs.get(procs.getKey());
        if (procs.getValue() != null) {
          procs.getValue().updateJiffy(oldInfo);
          if (oldInfo != null) {
            procs.getValue().updateAge(oldInfo);  
          }
        }
      }

      if (LOG.isDebugEnabled()) {
        // Log.debug the ProcfsBasedProcessTree
        LOG.debug(this.toString());
      }
    }
    return this;
  }

  /**
   * Is the root-process alive?
   * 
   * @return true if the root-process is alive, false otherwise.
   */
  public boolean isAlive() {
    if (pid == -1) {
      return false;
    } else {
      return isAlive(pid.toString());
    }
  }

  /**
   * Is any of the subprocesses in the process-tree alive?
   * 
   * @return true if any of the processes in the process-tree is
   *           alive, false otherwise.
   */
  public boolean isAnyProcessInTreeAlive() {
    for (Integer pId : processTree.keySet()) {
      if (isAlive(pId.toString())) {
        return true;
      }
    }
    return false;
  }

  /** Verify that the given process id is same as its process group id.
   * @param pidStr Process id of the to-be-verified-process
   * @param procfsDir  Procfs root dir
   */
  static boolean checkPidPgrpidForMatch(String pidStr, String procfsDir) {
    Integer pId = Integer.parseInt(pidStr);
    // Get information for this process
    ProcessInfo pInfo = new ProcessInfo(pId);
    pInfo = constructProcessInfo(pInfo, procfsDir);
    if (pInfo == null) {
      // process group leader may have finished execution, but we still need to
      // kill the subProcesses in the process group.
      return true;
    }

    //make sure that pId and its pgrpId match
    if (!pInfo.getPgrpId().equals(pId)) {
      LOG.warn("Unexpected: Process with PID " + pId +
               " is not a process group leader.");
      return false;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(pId + " is a process group leader, as expected.");
    }
    return true;
  }

  /** Make sure that the given pid is a process group leader and then
   * destroy the process group.
   * @param pgrpId   Process group id of to-be-killed-processes
   * @param interval The time to wait before sending SIGKILL
   *                 after sending SIGTERM
   * @param inBackground Process is to be killed in the back ground with
   *                     a separate thread
   */
  public static void assertAndDestroyProcessGroup(String pgrpId, long interval,
                       boolean inBackground)
         throws IOException {
    // Make sure that the pid given is a process group leader
    if (!checkPidPgrpidForMatch(pgrpId, PROCFS)) {
      throw new IOException("Process with PID " + pgrpId  +
                          " is not a process group leader.");
    }
    destroyProcessGroup(pgrpId, interval, inBackground);
  }

  /**
   * Destroy the process-tree.
   */
  public void destroy() {
    destroy(true);
  }
  
  /**
   * Destroy the process-tree.
   * @param inBackground Process is to be killed in the back ground with
   *                     a separate thread
   */
  public void destroy(boolean inBackground) {
    LOG.debug("Killing ProcfsBasedProcessTree of " + pid);
    if (pid == -1) {
      return;
    }
    if (isAlive(pid.toString())) {
      if (isSetsidAvailable && setsidUsed) {
        // In this case, we know that pid got created using setsid. So kill the
        // whole processGroup.
        try {
          assertAndDestroyProcessGroup(pid.toString(), sleeptimeBeforeSigkill,
                              inBackground);
        } catch (IOException e) {
          LOG.warn(StringUtils.stringifyException(e));
        }
      }
      else {
        //TODO: Destroy all the processes in the subtree in this case also.
        // For the time being, killing only the root process.
        destroyProcess(pid.toString(), sleeptimeBeforeSigkill, inBackground);
      }
    }
  }

  private static final String PROCESSTREE_DUMP_FORMAT =
      "\t|- %d %d %d %d %s %d %d %d %d %s\n";

  /**
   * Get a dump of the process-tree.
   * 
   * @return a string concatenating the dump of information of all the processes
   *         in the process-tree
   */
  public String getProcessTreeDump() {
    StringBuilder ret = new StringBuilder();
    // The header.
    ret.append(String.format("\t|- PID PPID PGRPID SESSID CMD_NAME "
        + "USER_MODE_TIME(MILLIS) SYSTEM_TIME(MILLIS) VMEM_USAGE(BYTES) "
        + "RSSMEM_USAGE(PAGES) FULL_CMD_LINE\n"));
    for (ProcessInfo p : processTree.values()) {
      if (p != null) {
        ret.append(String.format(PROCESSTREE_DUMP_FORMAT, p.getPid(), p
            .getPpid(), p.getPgrpId(), p.getSessionId(), p.getName(), p
            .getUtime(), p.getStime(), p.getVmem(), p.getRssmemPage(), p
            .getCmdLine(procfsDir)));
      }
    }
    return ret.toString();
  }

  /**
   * Get the cumulative virtual memory used by all the processes in the
   * process-tree.
   * 
   * @return cumulative virtual memory used by the process-tree in bytes.
   */
  public long getCumulativeVmem() {
    // include all processes.. all processes will be older than 0.
    return getCumulativeVmem(0);
  }

  /**
   * Get the cumulative resident set size (rss) memory used by all the processes
   * in the process-tree.
   *
   * @return cumulative rss memory used by the process-tree in bytes. return 0
   *         if it cannot be calculated
   */
  public long getCumulativeRssmem() {
    // include all processes.. all processes will be older than 0.
    return getCumulativeRssmem(0);
  }

  /**
   * Get the cumulative virtual memory used by all the processes in the
   * process-tree that are older than the passed in age.
   * 
   * @param olderThanAge processes above this age are included in the
   *                      memory addition
   * @return cumulative virtual memory used by the process-tree in bytes,
   *          for processes older than this age.
   */
  public long getCumulativeVmem(int olderThanAge) {
    long total = 0;
    for (ProcessInfo p : processTree.values()) {
      if ((p != null) && (p.getAge() > olderThanAge)) {
        total += p.getVmem();
      }
    }
    return total;
  }
  
  /**
   * Get the cumulative resident set size (rss) memory used by all the processes
   * in the process-tree that are older than the passed in age.
   *
   * @param olderThanAge processes above this age are included in the
   *                      memory addition
   * @return cumulative rss memory used by the process-tree in bytes,
   *          for processes older than this age. return 0 if it cannot be
   *          calculated
   */
  public long getCumulativeRssmem(int olderThanAge) {
    if (PAGE_SIZE < 0) {
      return 0;
    }
    long totalPages = 0;
    for (ProcessInfo p : processTree.values()) {
      if ((p != null) && (p.getAge() > olderThanAge)) {
        totalPages += p.getRssmemPage();
      }
    }
    return totalPages * PAGE_SIZE; // convert # pages to byte
  }

  /**
   * Get the CPU time in millisecond used by all the processes in the
   * process-tree since the process-tree created
   *
   * @return cumulative CPU time in millisecond since the process-tree created
   *         return 0 if it cannot be calculated
   */
  public long getCumulativeCpuTime() {
    if (JIFFY_LENGTH_IN_MILLIS < 0) {
      return 0;
    }
    long incJiffies = 0;
    for (ProcessInfo p : processTree.values()) {
      if (p != null) {
        incJiffies += p.dtime;
      }
    }
    cpuTime += incJiffies * JIFFY_LENGTH_IN_MILLIS;
    return cpuTime;
  }

  private static Integer getValidPID(String pid) {
    Integer retPid = -1;
    try {
      retPid = Integer.parseInt(pid);
      if (retPid <= 0) {
        retPid = -1;
      }
    } catch (NumberFormatException nfe) {
      retPid = -1;
    }
    return retPid;
  }

  /**
   * Get the list of all processes in the system.
   */
  private List<Integer> getProcessList() {
    String[] processDirs = (new File(procfsDir)).list();
    List<Integer> processList = new ArrayList<Integer>();

    for (String dir : processDirs) {
      try {
        int pd = Integer.parseInt(dir);
        if ((new File(procfsDir, dir)).isDirectory()) {
          processList.add(Integer.valueOf(pd));
        }
      } catch (NumberFormatException n) {
        // skip this directory
      } catch (SecurityException s) {
        // skip this process
      }
    }
    return processList;
  }

  /**
   * Construct the ProcessInfo using the process' PID and procfs rooted at the
   * specified directory and return the same. It is provided mainly to assist
   * testing purposes.
   * 
   * Returns null on failing to read from procfs,
   *
   * @param pinfo ProcessInfo that needs to be updated
   * @param procfsDir root of the proc file system
   * @return updated ProcessInfo, null on errors.
   */
  private static ProcessInfo constructProcessInfo(ProcessInfo pinfo, 
                                                    String procfsDir) {
    ProcessInfo ret = null;
    // Read "procfsDir/<pid>/stat" file - typically /proc/<pid>/stat
    BufferedReader in = null;
    FileReader fReader = null;
    try {
      File pidDir = new File(procfsDir, String.valueOf(pinfo.getPid()));
      fReader = new FileReader(new File(pidDir, PROCFS_STAT_FILE));
      in = new BufferedReader(fReader);
    } catch (FileNotFoundException f) {
      // The process vanished in the interim!
      LOG.warn("The process " + pinfo.getPid()
          + " may have finished in the interim.");
      return ret;
    }

    ret = pinfo;
    try {
      String str = in.readLine(); // only one line
      Matcher m = PROCFS_STAT_FILE_FORMAT.matcher(str);
      boolean mat = m.find();
      if (mat) {
        // Set (name) (ppid) (pgrpId) (session) (utime) (stime) (vsize) (rss)
        pinfo.updateProcessInfo(m.group(2), Integer.parseInt(m.group(3)),
                Integer.parseInt(m.group(4)), Integer.parseInt(m.group(5)),
                Long.parseLong(m.group(7)), Long.parseLong(m.group(8)),
                Long.parseLong(m.group(10)), Long.parseLong(m.group(11)));
      } else {
        LOG.warn("Unexpected: procfs stat file is not in the expected format"
            + " for process with pid " + pinfo.getPid());
        ret = null;
      }
    } catch (IOException io) {
      LOG.warn("Error reading the stream " + io);
      ret = null;
    } finally {
      // Close the streams
      try {
        fReader.close();
        try {
          in.close();
        } catch (IOException i) {
          LOG.warn("Error closing the stream " + in);
        }
      } catch (IOException i) {
        LOG.warn("Error closing the stream " + fReader);
      }
    }

    return ret;
  }
  /**
   * Returns a string printing PIDs of process present in the
   * ProcfsBasedProcessTree. Output format : [pid pid ..]
   */
  public String toString() {
    StringBuffer pTree = new StringBuffer("[ ");
    for (Integer p : processTree.keySet()) {
      pTree.append(p);
      pTree.append(" ");
    }
    return pTree.substring(0, pTree.length()) + "]";
  }

  /**
   * 
   * Class containing information of a process.
   * 
   */
  private static class ProcessInfo {
    private Integer pid; // process-id
    private String name; // command name
    private Integer pgrpId; // process group-id
    private Integer ppid; // parent process-id
    private Integer sessionId; // session-id
    private Long vmem; // virtual memory usage
    private Long rssmemPage; // rss memory usage in # of pages
    private Long utime = 0L; // # of jiffies in user mode
    private Long stime = 0L; // # of jiffies in kernel mode
    // how many times has this process been seen alive
    private int age; 

    // # of jiffies used since last update:
    private Long dtime = 0L;
    // dtime = (utime + stime) - (utimeOld + stimeOld)
    // We need this to compute the cumulative CPU time
    // because the subprocess may finish earlier than root process

    private List<ProcessInfo> children = new ArrayList<ProcessInfo>(); // list of children

    public ProcessInfo(int pid) {
      this.pid = Integer.valueOf(pid);
      // seeing this the first time.
      this.age = 1;
    }

    public Integer getPid() {
      return pid;
    }

    public String getName() {
      return name;
    }

    public Integer getPgrpId() {
      return pgrpId;
    }

    public Integer getPpid() {
      return ppid;
    }

    public Integer getSessionId() {
      return sessionId;
    }

    public Long getVmem() {
      return vmem;
    }

    public Long getUtime() {
      return utime;
    }

    public Long getStime() {
      return stime;
    }

    public Long getDtime() {
      return dtime;
    }

    public Long getRssmemPage() { // get rss # of pages
      return rssmemPage;
    }

    public int getAge() {
      return age;
    }
    
    public boolean isParent(ProcessInfo p) {
      if (pid.equals(p.getPpid())) {
        return true;
      }
      return false;
    }

    public void updateProcessInfo(String name, Integer ppid, Integer pgrpId,
        Integer sessionId, Long utime, Long stime, Long vmem, Long rssmem) {
      this.name = name;
      this.ppid = ppid;
      this.pgrpId = pgrpId;
      this.sessionId = sessionId;
      this.utime = utime;
      this.stime = stime;
      this.vmem = vmem;
      this.rssmemPage = rssmem;
    }

    public void updateJiffy(ProcessInfo oldInfo) {
      this.dtime = (oldInfo == null ? this.utime + this.stime
              : (this.utime + this.stime) - (oldInfo.utime + oldInfo.stime));
    }

    public void updateAge(ProcessInfo oldInfo) {
      this.age = oldInfo.age + 1;
    }
    
    public boolean addChild(ProcessInfo p) {
      return children.add(p);
    }

    public List<ProcessInfo> getChildren() {
      return children;
    }

    public String getCmdLine(String procfsDir) {
      String ret = "N/A";
      if (pid == null) {
        return ret;
      }
      BufferedReader in = null;
      FileReader fReader = null;
      try {
        fReader =
            new FileReader(new File(new File(procfsDir, pid.toString()),
                PROCFS_CMDLINE_FILE));
      } catch (FileNotFoundException f) {
        // The process vanished in the interim!
        return ret;
      }

      in = new BufferedReader(fReader);

      try {
        ret = in.readLine(); // only one line
        if (ret == null) {
          ret = "N/A";
        } else {
          ret = ret.replace('\0', ' '); // Replace each null char with a space
          if (ret.equals("")) {
            // The cmdline might be empty because the process is swapped out or
            // is a zombie.
            ret = "N/A";
          }
        }
      } catch (IOException io) {
        LOG.warn("Error reading the stream " + io);
        ret = "N/A";
      } finally {
        // Close the streams
        try {
          fReader.close();
          try {
            in.close();
          } catch (IOException i) {
            LOG.warn("Error closing the stream " + in);
          }
        } catch (IOException i) {
          LOG.warn("Error closing the stream " + fReader);
        }
      }

      return ret;
    }
  }
}
