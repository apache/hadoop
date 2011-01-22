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

/**
 * A Proc file-system based ProcessTree. Works only on Linux.
 */
public class ProcfsBasedProcessTree extends ProcessTree {

  static final Log LOG = LogFactory
      .getLog(ProcfsBasedProcessTree.class);

  private static final String PROCFS = "/proc/";
  private static final Pattern PROCFS_STAT_FILE_FORMAT = Pattern
      .compile("^([0-9-]+)\\s([^\\s]+)\\s[^\\s]\\s([0-9-]+)\\s([0-9-]+)\\s([0-9-]+)\\s([0-9-]+\\s){16}([0-9]+)(\\s[0-9-]+){16}");

  static final String PROCFS_STAT_FILE = "stat";
  static final String PROCFS_CMDLINE_FILE = "cmdline";

  // to enable testing, using this variable which can be configured
  // to a test directory.
  private String procfsDir;
  
  private Integer pid = -1;

  private Map<Integer, ProcessInfo> processTree = new HashMap<Integer, ProcessInfo>();

  public ProcfsBasedProcessTree(String pid) {
    this(pid, false);
  }
  
  public ProcfsBasedProcessTree(String pid, boolean setsidUsed) {
    this(pid,PROCFS);
  }

  public ProcfsBasedProcessTree(String pid, String procfsDir) {
    this.pid = getValidPID(pid);
    this.procfsDir = procfsDir;
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

      // update age values.
      for (Map.Entry<Integer, ProcessInfo> procs : processTree.entrySet()) {
        ProcessInfo oldInfo = oldProcs.get(procs.getKey());
        if (oldInfo != null) {
          if (procs.getValue() != null) {
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

  private static final String PROCESSTREE_DUMP_FORMAT =
      "\t|- %d %d %d %d %s %d %s\n";

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
        + "VMEM_USAGE(BYTES) FULL_CMD_LINE\n"));
    for (ProcessInfo p : processTree.values()) {
      if (p != null) {
        ret.append(String.format(PROCESSTREE_DUMP_FORMAT, p.getPid(), p
            .getPpid(), p.getPgrpId(), p.getSessionId(), p.getName(), p
            .getVmem(), p.getCmdLine(procfsDir)));
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

  private static Integer getValidPID(String pid) {
    Integer retPid = -1;
    try {
      retPid = Integer.parseInt((String) pid);
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
    // Read "procfsDir/<pid>/stat" file
    BufferedReader in = null;
    FileReader fReader = null;
    try {
      File pidDir = new File(procfsDir, String.valueOf(pinfo.getPid()));
      fReader = new FileReader(new File(pidDir, PROCFS_STAT_FILE));
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
        // Set ( name ) ( ppid ) ( pgrpId ) (session ) (vsize )
        pinfo.updateProcessInfo(m.group(2), Integer.parseInt(m.group(3)), Integer
            .parseInt(m.group(4)), Integer.parseInt(m.group(5)), Long
            .parseLong(m.group(7)));
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
    // how many times has this process been seen alive
    private int age; 
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

    public int getAge() {
      return age;
    }
    
    public void updateProcessInfo(String name, Integer ppid, Integer pgrpId,
        Integer sessionId, Long vmem) {
      this.name = name;
      this.ppid = ppid;
      this.pgrpId = pgrpId;
      this.sessionId = sessionId;
      this.vmem = vmem;
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
        ret = ret.replace('\0', ' '); // Replace each null char with a space
        if (ret.equals("")) {
          // The cmdline might be empty because the process is swapped out or is
          // a zombie.
          ret = "N/A";
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
