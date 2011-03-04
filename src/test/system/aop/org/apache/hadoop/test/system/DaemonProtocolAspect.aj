package org.apache.hadoop.test.system;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

/**
 * Default DaemonProtocolAspect which is used to provide default implementation
 * for all the common daemon methods. If a daemon requires more specialized
 * version of method, it is responsibility of the DaemonClient to introduce the
 * same in woven classes.
 * 
 */
public aspect DaemonProtocolAspect {

  private boolean DaemonProtocol.ready;
  
  @SuppressWarnings("unchecked")
  private HashMap<Object, List<ControlAction>> DaemonProtocol.actions = 
    new HashMap<Object, List<ControlAction>>();
  
  /**
   * Set if the daemon process is ready or not, concrete daemon protocol should
   * implement pointcuts to determine when the daemon is ready and use the
   * setter to set the ready state.
   * 
   * @param ready
   *          true if the Daemon is ready.
   */
  public void DaemonProtocol.setReady(boolean ready) {
    this.ready = ready;
  }

  /**
   * Checks if the daemon process is alive or not.
   * 
   * @throws IOException
   *           if daemon is not alive.
   */
  public void DaemonProtocol.ping() throws IOException {
  }

  /**
   * Checks if the daemon process is ready to accepting RPC connections after it
   * finishes initialization. <br/>
   * 
   * @return true if ready to accept connection.
   * 
   * @throws IOException
   */
  public boolean DaemonProtocol.isReady() throws IOException {
    return ready;
  }

  /**
   * Returns the process related information regarding the daemon process. <br/>
   * 
   * @return process information.
   * @throws IOException
   */
  public ProcessInfo DaemonProtocol.getProcessInfo() throws IOException {
    int activeThreadCount = Thread.activeCount();
    long currentTime = System.currentTimeMillis();
    long maxmem = Runtime.getRuntime().maxMemory();
    long freemem = Runtime.getRuntime().freeMemory();
    long totalmem = Runtime.getRuntime().totalMemory();
    Map<String, String> envMap = System.getenv();
    Properties sysProps = System.getProperties();
    Map<String, String> props = new HashMap<String, String>();
    for (Map.Entry entry : sysProps.entrySet()) {
      props.put((String) entry.getKey(), (String) entry.getValue());
    }
    ProcessInfo info = new ProcessInfoImpl(activeThreadCount, currentTime,
        freemem, maxmem, totalmem, envMap, props);
    return info;
  }

  public void DaemonProtocol.enable(List<Enum<?>> faults) throws IOException {
  }

  public void DaemonProtocol.disableAll() throws IOException {
  }

  public abstract Configuration DaemonProtocol.getDaemonConf()
    throws IOException;

  public FileStatus DaemonProtocol.getFileStatus(String path, boolean local) 
    throws IOException {
    Path p = new Path(path);
    FileSystem fs = getFS(p, local);
    p.makeQualified(fs);
    FileStatus fileStatus = fs.getFileStatus(p);
    return cloneFileStatus(fileStatus);
  }

  public FileStatus[] DaemonProtocol.listStatus(String path, boolean local) 
    throws IOException {
    Path p = new Path(path);
    FileSystem fs = getFS(p, local);
    FileStatus[] status = fs.listStatus(p);
    if (status != null) {
      FileStatus[] result = new FileStatus[status.length];
      int i = 0;
      for (FileStatus fileStatus : status) {
        result[i++] = cloneFileStatus(fileStatus);
      }
      return result;
    }
    return status;
  }

  /**
   * FileStatus object may not be serializable. Clone it into raw FileStatus 
   * object.
   */
  private FileStatus DaemonProtocol.cloneFileStatus(FileStatus fileStatus) {
    return new FileStatus(fileStatus.getLen(),
        fileStatus.isDir(),
        fileStatus.getReplication(),
        fileStatus.getBlockSize(),
        fileStatus.getModificationTime(),
        fileStatus.getAccessTime(),
        fileStatus.getPermission(),
        fileStatus.getOwner(),
        fileStatus.getGroup(),
        fileStatus.getPath());
  }

  private FileSystem DaemonProtocol.getFS(Path path, boolean local) 
    throws IOException {
    FileSystem fs = null;
    if (local) {
      fs = FileSystem.getLocal(getDaemonConf());
    } else {
      fs = path.getFileSystem(getDaemonConf());
    }
    return fs;
  }
  
  
  @SuppressWarnings("unchecked")
  public ControlAction[] DaemonProtocol.getActions(Writable key) 
    throws IOException {
    synchronized (actions) {
      List<ControlAction> actionList = actions.get(key);
      if(actionList == null) {
        return new ControlAction[0];
      } else {
        return (ControlAction[]) actionList.toArray(new ControlAction[actionList
                                                                      .size()]);
      }
    }
  }


  @SuppressWarnings("unchecked")
  public void DaemonProtocol.sendAction(ControlAction action) 
      throws IOException {
    synchronized (actions) {
      List<ControlAction> actionList = actions.get(action.getTarget());
      if(actionList == null) {
        actionList = new ArrayList<ControlAction>();
        actions.put(action.getTarget(), actionList);
      }
      actionList.add(action);
    } 
  }
 
  @SuppressWarnings("unchecked")
  public boolean DaemonProtocol.isActionPending(ControlAction action) 
    throws IOException{
    synchronized (actions) {
      List<ControlAction> actionList = actions.get(action.getTarget());
      if(actionList == null) {
        return false;
      } else {
        return actionList.contains(action);
      }
    }
  }
  
  
  @SuppressWarnings("unchecked")
  public void DaemonProtocol.removeAction(ControlAction action) 
    throws IOException {
    synchronized (actions) {
      List<ControlAction> actionList = actions.get(action.getTarget());
      if(actionList == null) {
        return;
      } else {
        actionList.remove(action);
      }
    }
  }
  
  public void DaemonProtocol.clearActions() throws IOException {
    synchronized (actions) {
      actions.clear();
    }
  }

  public String DaemonProtocol.getFilePattern() {
    //We use the environment variable HADOOP_LOGFILE to get the
    //pattern to use in the search.
    String logDir = System.getenv("HADOOP_LOG_DIR");
    String daemonLogPattern = System.getenv("HADOOP_LOGFILE");
    if(daemonLogPattern == null && daemonLogPattern.isEmpty()) {
      return "*";
    }
    return  logDir+File.separator+daemonLogPattern+"*";
  }

  public int DaemonProtocol.getNumberOfMatchesInLogFile(String pattern)
      throws IOException {
    String filePattern = getFilePattern();
    String[] cmd =
        new String[] {
            "bash",
            "-c",
            "grep -c "
                + pattern + " " + filePattern
                + " | awk -F: '{s+=$2} END {print s}'" };
    ShellCommandExecutor shexec = new ShellCommandExecutor(cmd);
    shexec.execute();
    String output = shexec.getOutput();
    return Integer.parseInt(output.replaceAll("\n", "").trim());
  }
}

