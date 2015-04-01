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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * A Proc file-system based ProcessTree. Works only on Linux.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ProcfsBasedProcessTree extends ResourceCalculatorProcessTree {

  static final Log LOG = LogFactory
      .getLog(ProcfsBasedProcessTree.class);

  private static final String PROCFS = "/proc/";

  private static final Pattern PROCFS_STAT_FILE_FORMAT = Pattern .compile(
    "^([0-9-]+)\\s([^\\s]+)\\s[^\\s]\\s([0-9-]+)\\s([0-9-]+)\\s([0-9-]+)\\s" +
    "([0-9-]+\\s){7}([0-9]+)\\s([0-9]+)\\s([0-9-]+\\s){7}([0-9]+)\\s([0-9]+)" +
    "(\\s[0-9-]+){15}");

  public static final String PROCFS_STAT_FILE = "stat";
  public static final String PROCFS_CMDLINE_FILE = "cmdline";
  public static final long PAGE_SIZE;
  public static final long JIFFY_LENGTH_IN_MILLIS; // in millisecond
  private final CpuTimeTracker cpuTimeTracker;
  private Clock clock;

  enum MemInfo {
    SIZE("Size"), RSS("Rss"), PSS("Pss"), SHARED_CLEAN("Shared_Clean"),
    SHARED_DIRTY("Shared_Dirty"), PRIVATE_CLEAN("Private_Clean"),
    PRIVATE_DIRTY("Private_Dirty"), REFERENCED("Referenced"), ANONYMOUS(
        "Anonymous"), ANON_HUGE_PAGES("AnonHugePages"), SWAP("swap"),
    KERNEL_PAGE_SIZE("kernelPageSize"), MMU_PAGE_SIZE("mmuPageSize"), INVALID(
        "invalid");

    private String name;

    private MemInfo(String name) {
      this.name = name;
    }

    public static MemInfo getMemInfoByName(String name) {
      for (MemInfo info : MemInfo.values()) {
        if (info.name.trim().equalsIgnoreCase(name.trim())) {
          return info;
        }
      }
      return INVALID;
    }
  }

  public static final String SMAPS = "smaps";
  public static final int KB_TO_BYTES = 1024;
  private static final String KB = "kB";
  private static final String READ_ONLY_WITH_SHARED_PERMISSION = "r--s";
  private static final String READ_EXECUTE_WITH_SHARED_PERMISSION = "r-xs";
  private static final Pattern ADDRESS_PATTERN = Pattern
    .compile("([[a-f]|(0-9)]*)-([[a-f]|(0-9)]*)(\\s)*([rxwps\\-]*)");
  private static final Pattern MEM_INFO_PATTERN = Pattern
    .compile("(^[A-Z].*):[\\s ]*(.*)");

  private boolean smapsEnabled;

  protected Map<String, ProcessTreeSmapMemInfo> processSMAPTree =
      new HashMap<String, ProcessTreeSmapMemInfo>();

  static {
    long jiffiesPerSecond = -1;
    long pageSize = -1;
    try {
      if(Shell.LINUX) {
        ShellCommandExecutor shellExecutorClk = new ShellCommandExecutor(
            new String[] { "getconf", "CLK_TCK" });
        shellExecutorClk.execute();
        jiffiesPerSecond = Long.parseLong(shellExecutorClk.getOutput().replace("\n", ""));

        ShellCommandExecutor shellExecutorPage = new ShellCommandExecutor(
            new String[] { "getconf", "PAGESIZE" });
        shellExecutorPage.execute();
        pageSize = Long.parseLong(shellExecutorPage.getOutput().replace("\n", ""));

      }
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
    } finally {
      JIFFY_LENGTH_IN_MILLIS = jiffiesPerSecond != -1 ?
                     Math.round(1000D / jiffiesPerSecond) : -1;
                     PAGE_SIZE = pageSize;
    }
  }

  // to enable testing, using this variable which can be configured
  // to a test directory.
  private String procfsDir;

  static private String deadPid = "-1";
  private String pid = deadPid;
  static private Pattern numberPattern = Pattern.compile("[1-9][0-9]*");
  private long cpuTime = UNAVAILABLE;

  protected Map<String, ProcessInfo> processTree =
    new HashMap<String, ProcessInfo>();

  public ProcfsBasedProcessTree(String pid) {
    this(pid, PROCFS, new SystemClock());
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf != null) {
      smapsEnabled =
          conf.getBoolean(YarnConfiguration.PROCFS_USE_SMAPS_BASED_RSS_ENABLED,
            YarnConfiguration.DEFAULT_PROCFS_USE_SMAPS_BASED_RSS_ENABLED);
    }
  }

  public ProcfsBasedProcessTree(String pid, String procfsDir) {
    this(pid, procfsDir, new SystemClock());
  }

  /**
   * Build a new process tree rooted at the pid.
   *
   * This method is provided mainly for testing purposes, where
   * the root of the proc file system can be adjusted.
   *
   * @param pid root of the process tree
   * @param procfsDir the root of a proc file system - only used for testing.
   * @param clock clock for controlling time for testing
   */
  public ProcfsBasedProcessTree(String pid, String procfsDir, Clock clock) {
    super(pid);
    this.clock = clock;
    this.pid = getValidPID(pid);
    this.procfsDir = procfsDir;
    this.cpuTimeTracker = new CpuTimeTracker(JIFFY_LENGTH_IN_MILLIS);
  }

  /**
   * Checks if the ProcfsBasedProcessTree is available on this system.
   *
   * @return true if ProcfsBasedProcessTree is available. False otherwise.
   */
  public static boolean isAvailable() {
    try {
      if (!Shell.LINUX) {
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
   * Update process-tree with latest state. If the root-process is not alive,
   * tree will be empty.
   *
   */
  @Override
  public void updateProcessTree() {
    if (!pid.equals(deadPid)) {
      // Get the list of processes
      List<String> processList = getProcessList();

      Map<String, ProcessInfo> allProcessInfo = new HashMap<String, ProcessInfo>();

      // cache the processTree to get the age for processes
      Map<String, ProcessInfo> oldProcs =
              new HashMap<String, ProcessInfo>(processTree);
      processTree.clear();

      ProcessInfo me = null;
      for (String proc : processList) {
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
        return;
      }

      // Add each process to its parent.
      for (Map.Entry<String, ProcessInfo> entry : allProcessInfo.entrySet()) {
        String pID = entry.getKey();
        if (!pID.equals("1")) {
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
      for (Map.Entry<String, ProcessInfo> procs : processTree.entrySet()) {
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
      if (smapsEnabled) {
        //Update smaps info
        processSMAPTree.clear();
        for (ProcessInfo p : processTree.values()) {
          if (p != null) {
            // Get information for each process
            ProcessTreeSmapMemInfo memInfo = new ProcessTreeSmapMemInfo(p.getPid());
            constructProcessSMAPInfo(memInfo, procfsDir);
            processSMAPTree.put(p.getPid(), memInfo);
          }
        }
      }
    }
  }

  /** Verify that the given process id is same as its process group id.
   * @return true if the process id matches else return false.
   */
  @Override
  public boolean checkPidPgrpidForMatch() {
    return checkPidPgrpidForMatch(pid, PROCFS);
  }

  public static boolean checkPidPgrpidForMatch(String _pid, String procfs) {
    // Get information for this process
    ProcessInfo pInfo = new ProcessInfo(_pid);
    pInfo = constructProcessInfo(pInfo, procfs);
    // null if process group leader finished execution; issue no warning
    // make sure that pid and its pgrpId match
    if (pInfo == null) return true;
    String pgrpId = pInfo.getPgrpId().toString();
    return pgrpId.equals(_pid);
  }

  private static final String PROCESSTREE_DUMP_FORMAT =
      "\t|- %s %s %d %d %s %d %d %d %d %s%n";

  public List<String> getCurrentProcessIDs() {
    List<String> currentPIDs = new ArrayList<String>();
    currentPIDs.addAll(processTree.keySet());
    return currentPIDs;
  }

  /**
   * Get a dump of the process-tree.
   *
   * @return a string concatenating the dump of information of all the processes
   *         in the process-tree
   */
  @Override
  public String getProcessTreeDump() {
    StringBuilder ret = new StringBuilder();
    // The header.
    ret.append(String.format("\t|- PID PPID PGRPID SESSID CMD_NAME "
        + "USER_MODE_TIME(MILLIS) SYSTEM_TIME(MILLIS) VMEM_USAGE(BYTES) "
        + "RSSMEM_USAGE(PAGES) FULL_CMD_LINE%n"));
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

  @Override
  public long getVirtualMemorySize(int olderThanAge) {
    long total = UNAVAILABLE;
    for (ProcessInfo p : processTree.values()) {
      if (p != null) {
        if (total == UNAVAILABLE ) {
          total = 0;
        }
        if (p.getAge() > olderThanAge) {
          total += p.getVmem();
        }
      }
    }
    return total;
  }
  
  @Override
  @SuppressWarnings("deprecation")
  public long getCumulativeVmem(int olderThanAge) {
    return getVirtualMemorySize(olderThanAge);
  }

  @Override
  public long getRssMemorySize(int olderThanAge) {
    if (PAGE_SIZE < 0) {
      return UNAVAILABLE;
    }
    if (smapsEnabled) {
      return getSmapBasedRssMemorySize(olderThanAge);
    }
    boolean isAvailable = false;
    long totalPages = 0;
    for (ProcessInfo p : processTree.values()) {
      if ((p != null) ) {
        if (p.getAge() > olderThanAge) {
          totalPages += p.getRssmemPage();
        }
        isAvailable = true;
      }
    }
    return isAvailable ? totalPages * PAGE_SIZE : UNAVAILABLE; // convert # pages to byte
  }
  
  @Override
  @SuppressWarnings("deprecation")
  public long getCumulativeRssmem(int olderThanAge) {
    return getRssMemorySize(olderThanAge);
  }

  /**
   * Get the resident set size (RSS) memory used by all the processes
   * in the process-tree that are older than the passed in age. RSS is
   * calculated based on SMAP information. Skip mappings with "r--s", "r-xs"
   * permissions to get real RSS usage of the process.
   *
   * @param olderThanAge
   *          processes above this age are included in the memory addition
   * @return rss memory used by the process-tree in bytes, for
   * processes older than this age. return {@link #UNAVAILABLE} if it cannot
   * be calculated.
   */
  private long getSmapBasedRssMemorySize(int olderThanAge) {
    long total = UNAVAILABLE;
    for (ProcessInfo p : processTree.values()) {
      if (p != null) {
        // set resource to 0 instead of UNAVAILABLE
        if (total == UNAVAILABLE){
          total = 0;
        }
        if (p.getAge() > olderThanAge) {
          ProcessTreeSmapMemInfo procMemInfo = processSMAPTree.get(p.getPid());
          if (procMemInfo != null) {
            for (ProcessSmapMemoryInfo info : procMemInfo.getMemoryInfoList()) {
              // Do not account for r--s or r-xs mappings
              if (info.getPermission().trim()
                .equalsIgnoreCase(READ_ONLY_WITH_SHARED_PERMISSION)
                  || info.getPermission().trim()
                    .equalsIgnoreCase(READ_EXECUTE_WITH_SHARED_PERMISSION)) {
                continue;
              }

              total +=
                  Math.min(info.sharedDirty, info.pss) + info.privateDirty
                      + info.privateClean;
              if (LOG.isDebugEnabled()) {
                LOG.debug(" total(" + olderThanAge + "): PID : " + p.getPid()
                    + ", SharedDirty : " + info.sharedDirty + ", PSS : "
                    + info.pss + ", Private_Dirty : " + info.privateDirty
                    + ", Private_Clean : " + info.privateClean + ", total : "
                    + (total * KB_TO_BYTES));
              }
            }
          }
        
          if (LOG.isDebugEnabled()) {
            LOG.debug(procMemInfo.toString());
          }
        }
      }
      
    }
    if (total > 0) {
      total *= KB_TO_BYTES; // convert to bytes
    }
    LOG.info("SmapBasedCumulativeRssmem (bytes) : " + total);
    return total; // size
  }

  @Override
  public long getCumulativeCpuTime() {
    if (JIFFY_LENGTH_IN_MILLIS < 0) {
      return UNAVAILABLE;
    }
    long incJiffies = 0;
    boolean isAvailable = false;
    for (ProcessInfo p : processTree.values()) {
      if (p != null) {
        incJiffies += p.getDtime();
        // data is available
        isAvailable = true;
      }
    }
    if (isAvailable) {
      // reset cpuTime to 0 instead of UNAVAILABLE
      if (cpuTime == UNAVAILABLE) {
        cpuTime = 0L;
      }
      cpuTime += incJiffies * JIFFY_LENGTH_IN_MILLIS;
    }
    return cpuTime;
  }

  private BigInteger getTotalProcessJiffies() {
    BigInteger totalStime = BigInteger.ZERO;
    long totalUtime = 0;
    for (ProcessInfo p : processTree.values()) {
      if (p != null) {
        totalUtime += p.getUtime();
        totalStime = totalStime.add(p.getStime());
      }
    }
    return totalStime.add(BigInteger.valueOf(totalUtime));
  }

  @Override
  public float getCpuUsagePercent() {
    BigInteger processTotalJiffies = getTotalProcessJiffies();
    cpuTimeTracker.updateElapsedJiffies(processTotalJiffies,
        clock.getTime());
    return cpuTimeTracker.getCpuTrackerUsagePercent();
  }

  private static String getValidPID(String pid) {
    if (pid == null) return deadPid;
    Matcher m = numberPattern.matcher(pid);
    if (m.matches()) return pid;
    return deadPid;
  }

  /**
   * Get the list of all processes in the system.
   */
  private List<String> getProcessList() {
    String[] processDirs = (new File(procfsDir)).list();
    List<String> processList = new ArrayList<String>();

    for (String dir : processDirs) {
      Matcher m = numberPattern.matcher(dir);
      if (!m.matches()) continue;
      try {
        if ((new File(procfsDir, dir)).isDirectory()) {
          processList.add(dir);
        }
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
    InputStreamReader fReader = null;
    try {
      File pidDir = new File(procfsDir, pinfo.getPid());
      fReader = new InputStreamReader(
          new FileInputStream(
              new File(pidDir, PROCFS_STAT_FILE)), Charset.forName("UTF-8"));
      in = new BufferedReader(fReader);
    } catch (FileNotFoundException f) {
      // The process vanished in the interim!
      return ret;
    }

    ret = pinfo;
    try {
      String str = in.readLine(); // only one line
      Matcher m = PROCFS_STAT_FILE_FORMAT.matcher(str);
      boolean mat = m.find();
      if (mat) {
        // Set (name) (ppid) (pgrpId) (session) (utime) (stime) (vsize) (rss)
        pinfo.updateProcessInfo(m.group(2), m.group(3),
                Integer.parseInt(m.group(4)), Integer.parseInt(m.group(5)),
                Long.parseLong(m.group(7)), new BigInteger(m.group(8)),
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
  @Override
  public String toString() {
    StringBuffer pTree = new StringBuffer("[ ");
    for (String p : processTree.keySet()) {
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
    private String pid; // process-id
    private String name; // command name
    private Integer pgrpId; // process group-id
    private String ppid; // parent process-id
    private Integer sessionId; // session-id
    private Long vmem; // virtual memory usage
    private Long rssmemPage; // rss memory usage in # of pages
    private Long utime = 0L; // # of jiffies in user mode
    private final BigInteger MAX_LONG = BigInteger.valueOf(Long.MAX_VALUE);
    private BigInteger stime = new BigInteger("0"); // # of jiffies in kernel mode
    // how many times has this process been seen alive
    private int age;

    // # of jiffies used since last update:
    private Long dtime = 0L;
    // dtime = (utime + stime) - (utimeOld + stimeOld)
    // We need this to compute the cumulative CPU time
    // because the subprocess may finish earlier than root process

    private List<ProcessInfo> children = new ArrayList<ProcessInfo>(); // list of children

    public ProcessInfo(String pid) {
      this.pid = pid;
      // seeing this the first time.
      this.age = 1;
    }

    public String getPid() {
      return pid;
    }

    public String getName() {
      return name;
    }

    public Integer getPgrpId() {
      return pgrpId;
    }

    public String getPpid() {
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

    public BigInteger getStime() {
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

    public void updateProcessInfo(String name, String ppid, Integer pgrpId,
        Integer sessionId, Long utime, BigInteger stime, Long vmem, Long rssmem) {
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
      if (oldInfo == null) {
        BigInteger sum = this.stime.add(BigInteger.valueOf(this.utime));
        if (sum.compareTo(MAX_LONG) > 0) {
          this.dtime = 0L;
          LOG.warn("Sum of stime (" + this.stime + ") and utime (" + this.utime
              + ") is greater than " + Long.MAX_VALUE);
        } else {
          this.dtime = sum.longValue();
        }
        return;
      }
      this.dtime = (this.utime - oldInfo.utime +
          this.stime.subtract(oldInfo.stime).longValue());
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
      InputStreamReader fReader = null;
      try {
        fReader = new InputStreamReader(
            new FileInputStream(
                new File(new File(procfsDir, pid.toString()), PROCFS_CMDLINE_FILE)),
                Charset.forName("UTF-8"));
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

  /**
   * Update memory related information
   *
   * @param pInfo
   * @param procfsDir
   */
  private static void constructProcessSMAPInfo(ProcessTreeSmapMemInfo pInfo,
      String procfsDir) {
    BufferedReader in = null;
    InputStreamReader fReader = null;
    try {
      File pidDir = new File(procfsDir, pInfo.getPid());
      File file = new File(pidDir, SMAPS);
      if (!file.exists()) {
        return;
      }
      fReader = new InputStreamReader(
          new FileInputStream(file), Charset.forName("UTF-8"));
      in = new BufferedReader(fReader);
      ProcessSmapMemoryInfo memoryMappingInfo = null;
      List<String> lines = IOUtils.readLines(in);
      for (String line : lines) {
        line = line.trim();
        try {
          Matcher address = ADDRESS_PATTERN.matcher(line);
          if (address.find()) {
            memoryMappingInfo = new ProcessSmapMemoryInfo(line);
            memoryMappingInfo.setPermission(address.group(4));
            pInfo.getMemoryInfoList().add(memoryMappingInfo);
            continue;
          }
          Matcher memInfo = MEM_INFO_PATTERN.matcher(line);
          if (memInfo.find()) {
            String key = memInfo.group(1).trim();
            String value = memInfo.group(2).replace(KB, "").trim();
            if (LOG.isDebugEnabled()) {
              LOG.debug("MemInfo : " + key + " : Value  : " + value);
            }
            memoryMappingInfo.setMemInfo(key, value);
          }
        } catch (Throwable t) {
          LOG
            .warn("Error parsing smaps line : " + line + "; " + t.getMessage());
        }
      }
    } catch (FileNotFoundException f) {
      LOG.error(f.getMessage());
    } catch (IOException e) {
      LOG.error(e.getMessage());
    } catch (Throwable t) {
      LOG.error(t.getMessage());
    } finally {
      IOUtils.closeQuietly(in);
    }
  }

  /**
   * Placeholder for process's SMAPS information
   */
  static class ProcessTreeSmapMemInfo {
    private String pid;
    private List<ProcessSmapMemoryInfo> memoryInfoList;

    public ProcessTreeSmapMemInfo(String pid) {
      this.pid = pid;
      this.memoryInfoList = new LinkedList<ProcessSmapMemoryInfo>();
    }

    public List<ProcessSmapMemoryInfo> getMemoryInfoList() {
      return memoryInfoList;
    }

    public String getPid() {
      return pid;
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      for (ProcessSmapMemoryInfo info : memoryInfoList) {
        sb.append("\n");
        sb.append(info.toString());
      }
      return sb.toString();
    }
  }

  /**
   * <pre>
   * Private Pages : Pages that were mapped only by the process
   * Shared Pages : Pages that were shared with other processes
   *
   * Clean Pages : Pages that have not been modified since they were mapped
   * Dirty Pages : Pages that have been modified since they were mapped
   *
   * Private RSS = Private Clean Pages + Private Dirty Pages
   * Shared RSS = Shared Clean Pages + Shared Dirty Pages
   * RSS = Private RSS + Shared RSS
   * PSS = The count of all pages mapped uniquely by the process, 
   *  plus a fraction of each shared page, said fraction to be 
   *  proportional to the number of processes which have mapped the page.
   * 
   * </pre>
   */
  static class ProcessSmapMemoryInfo {
    private int size;
    private int rss;
    private int pss;
    private int sharedClean;
    private int sharedDirty;
    private int privateClean;
    private int privateDirty;
    private int referenced;
    private String regionName;
    private String permission;

    public ProcessSmapMemoryInfo(String name) {
      this.regionName = name;
    }

    public String getName() {
      return regionName;
    }

    public void setPermission(String permission) {
      this.permission = permission;
    }

    public String getPermission() {
      return permission;
    }

    public int getSize() {
      return size;
    }

    public int getRss() {
      return rss;
    }

    public int getPss() {
      return pss;
    }

    public int getSharedClean() {
      return sharedClean;
    }

    public int getSharedDirty() {
      return sharedDirty;
    }

    public int getPrivateClean() {
      return privateClean;
    }

    public int getPrivateDirty() {
      return privateDirty;
    }

    public int getReferenced() {
      return referenced;
    }

    public void setMemInfo(String key, String value) {
      MemInfo info = MemInfo.getMemInfoByName(key);
      int val = 0;
      try {
        val = Integer.parseInt(value.trim());
      } catch (NumberFormatException ne) {
        LOG.error("Error in parsing : " + info + " : value" + value.trim());
        return;
      }
      if (info == null) {
        return;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("setMemInfo : memInfo : " + info);
      }
      switch (info) {
      case SIZE:
        size = val;
        break;
      case RSS:
        rss = val;
        break;
      case PSS:
        pss = val;
        break;
      case SHARED_CLEAN:
        sharedClean = val;
        break;
      case SHARED_DIRTY:
        sharedDirty = val;
        break;
      case PRIVATE_CLEAN:
        privateClean = val;
        break;
      case PRIVATE_DIRTY:
        privateDirty = val;
        break;
      case REFERENCED:
        referenced = val;
        break;
      default:
        break;
      }
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("\t").append(this.getName()).append("\n");
      sb.append("\t").append(MemInfo.SIZE.name + ":" + this.getSize())
        .append(" kB\n");
      sb.append("\t").append(MemInfo.PSS.name + ":" + this.getPss())
        .append(" kB\n");
      sb.append("\t").append(MemInfo.RSS.name + ":" + this.getRss())
        .append(" kB\n");
      sb.append("\t")
        .append(MemInfo.SHARED_CLEAN.name + ":" + this.getSharedClean())
        .append(" kB\n");
      sb.append("\t")
        .append(MemInfo.SHARED_DIRTY.name + ":" + this.getSharedDirty())
        .append(" kB\n");
      sb.append("\t")
        .append(MemInfo.PRIVATE_CLEAN.name + ":" + this.getPrivateClean())
        .append(" kB\n");
      sb.append("\t")
        .append(MemInfo.PRIVATE_DIRTY.name + ":" + this.getPrivateDirty())
        .append(" kB\n");
      sb.append("\t")
        .append(MemInfo.REFERENCED.name + ":" + this.getReferenced())
        .append(" kB\n");
      sb.append("\t")
        .append(MemInfo.PRIVATE_DIRTY.name + ":" + this.getPrivateDirty())
        .append(" kB\n");
      sb.append("\t")
        .append(MemInfo.PRIVATE_DIRTY.name + ":" + this.getPrivateDirty())
        .append(" kB\n");
      return sb.toString();
    }
  }

  /**
   * Test the {@link ProcfsBasedProcessTree}
   *
   * @param args
   */
  public static void main(String[] args) {
    if (args.length != 1) {
      System.out.println("Provide <pid of process to monitor>");
      return;
    }

    int numprocessors =
        ResourceCalculatorPlugin.getResourceCalculatorPlugin(null, null)
            .getNumProcessors();
    System.out.println("Number of processors " + numprocessors);

    System.out.println("Creating ProcfsBasedProcessTree for process " +
        args[0]);
    ProcfsBasedProcessTree procfsBasedProcessTree = new
        ProcfsBasedProcessTree(args[0]);
    procfsBasedProcessTree.updateProcessTree();

    System.out.println(procfsBasedProcessTree.getProcessTreeDump());
    System.out.println("Get cpu usage " + procfsBasedProcessTree
        .getCpuUsagePercent());

    try {
      // Sleep so we can compute the CPU usage
      Thread.sleep(500L);
    } catch (InterruptedException e) {
      // do nothing
    }

    procfsBasedProcessTree.updateProcessTree();

    System.out.println(procfsBasedProcessTree.getProcessTreeDump());
    System.out.println("Cpu usage  " + procfsBasedProcessTree
        .getCpuUsagePercent());
    System.out.println("Vmem usage in bytes " + procfsBasedProcessTree
        .getVirtualMemorySize());
    System.out.println("Rss mem usage in bytes " + procfsBasedProcessTree
        .getRssMemorySize());
  }
}
