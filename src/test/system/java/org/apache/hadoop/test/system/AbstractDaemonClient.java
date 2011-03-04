package org.apache.hadoop.test.system;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.test.system.process.RemoteProcess;
/**
 * Abstract class which encapsulates the DaemonClient which is used in the 
 * system tests.<br/>
 * 
 * @param PROXY the proxy implementation of a specific Daemon 
 */
public abstract class AbstractDaemonClient<PROXY extends DaemonProtocol> {
  private Configuration conf;
  private RemoteProcess process;
  private boolean connected;

  /**
   * Create a Daemon client.<br/>
   * 
   * @param conf client to be used by proxy to connect to Daemon.
   * @param process the Daemon process to manage the particular daemon.
   * 
   * @throws IOException
   */
  public AbstractDaemonClient(Configuration conf, RemoteProcess process) 
      throws IOException {
    this.conf = conf;
    this.process = process;
  }

  public boolean isConnected() {
    return connected;
  }

  protected void setConnected(boolean connected) {
    this.connected = connected;
  }

  public abstract void connect() throws IOException;

  public abstract void disconnect() throws IOException;

  /**
   * Get the proxy to connect to a particular service Daemon.<br/>
   * 
   * @return proxy to connect to a particular service Daemon.
   */
  protected abstract PROXY getProxy();

  public Configuration getConf() {
    return conf;
  }

  public String getHostName() {
    return process.getHostName();
  }

  public boolean isReady() throws IOException {
    return getProxy().isReady();
  }

  public void kill() throws IOException {
    process.kill();
  }

  public void ping() throws IOException {
    getProxy().ping();
  }

  public void start() throws IOException {
    process.start();
  }

  public ProcessInfo getProcessInfo() throws IOException {
    return getProxy().getProcessInfo();
  }

  public void enable(List<Enum<?>> faults) throws IOException {
    getProxy().enable(faults);
  }

  public void disableAll() throws IOException {
    getProxy().disableAll();
  }

  public FileStatus getFileStatus(String path, boolean local) throws IOException {
    return getProxy().getFileStatus(path, local);
  }

  public FileStatus[] listStatus(String path, boolean local) 
    throws IOException {
    return getProxy().listStatus(path, local);
  }

  public FileStatus[] listStatus(String f, boolean local, boolean recursive) 
    throws IOException {
    List<FileStatus> status = new ArrayList<FileStatus>();
    addStatus(status, f, local, recursive);
    return status.toArray(new FileStatus[0]);
  }

  private void addStatus(List<FileStatus> status, String f, 
      boolean local, boolean recursive) 
    throws IOException {
    FileStatus[] fs = listStatus(f, local);
    if (fs != null) {
      for (FileStatus fileStatus : fs) {
        if (!f.equals(fileStatus.getPath().toString())) {
          status.add(fileStatus);
          if (recursive) {
            addStatus(status, fileStatus.getPath().toString(), local, recursive);
          }
        }
      }
    }
  }
}
