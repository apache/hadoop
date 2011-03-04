package org.apache.hadoop.test.system;

import java.io.IOException;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

  private static final Log LOG = LogFactory.getLog(AbstractDaemonClient.class);
  
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

  /**
   * Gets if the client is connected to the Daemon <br/>
   * 
   * @return true if connected.
   */
  public boolean isConnected() {
    return connected;
  }

  protected void setConnected(boolean connected) {
    this.connected = connected;
  }

  /**
   * Create an RPC proxy to the daemon <br/>
   * 
   * @throws IOException
   */
  public abstract void connect() throws IOException;

  /**
   * Disconnect the underlying RPC proxy to the daemon.<br/>
   * @throws IOException
   */
  public abstract void disconnect() throws IOException;

  /**
   * Get the proxy to connect to a particular service Daemon.<br/>
   * 
   * @return proxy to connect to a particular service Daemon.
   */
  protected abstract PROXY getProxy();

  /**
   * Gets the daemon level configuration.<br/>
   * 
   * @return configuration using which daemon is running
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * Gets the host on which Daemon is currently running. <br/>
   * 
   * @return hostname
   */
  public String getHostName() {
    return process.getHostName();
  }

  /**
   * Gets if the Daemon is ready to accept RPC connections. <br/>
   * 
   * @return true if daemon is ready.
   * @throws IOException
   */
  public boolean isReady() throws IOException {
    return getProxy().isReady();
  }

  /**
   * Kills the Daemon process <br/>
   * @throws IOException
   */
  public void kill() throws IOException {
    process.kill();
  }

  /**
   * Checks if the Daemon process is alive or not <br/>
   * 
   * @throws IOException
   */
  public void ping() throws IOException {
    getProxy().ping();
  }

  /**
   * Start up the Daemon process. <br/>
   * @throws IOException
   */
  public void start() throws IOException {
    process.start();
  }

  /**
   * Get system level view of the Daemon process.
   * 
   * @return returns system level view of the Daemon process.
   * 
   * @throws IOException
   */
  public ProcessInfo getProcessInfo() throws IOException {
    return getProxy().getProcessInfo();
  }

  /**
   * Return a file status object that represents the path.
   * @param path
   *          given path
   * @param local
   *          whether the path is local or not
   * @return a FileStatus object
   * @throws FileNotFoundException when the path does not exist;
   *         IOException see specific implementation
   */
  public FileStatus getFileStatus(String path, boolean local) throws IOException {
    return getProxy().getFileStatus(path, local);
  }

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   * 
   * @param path
   *          given path
   * @param local
   *          whether the path is local or not
   * @return the statuses of the files/directories in the given patch
   * @throws IOException
   */
  public FileStatus[] listStatus(String path, boolean local) 
    throws IOException {
    return getProxy().listStatus(path, local);
  }

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory recursive/nonrecursively depending on parameters
   * 
   * @param path
   *          given path
   * @param local
   *          whether the path is local or not
   * @param recursive 
   *          whether to recursively get the status
   * @return the statuses of the files/directories in the given patch
   * @throws IOException
   */
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

  /**
   * Gets number of times FATAL log messages where logged in Daemon logs. 
   * <br/>
   * Pattern used for searching is FATAL. <br/>
   * 
   * @return number of occurrence of fatal message.
   * @throws IOException
   */
  public int getNumberOfFatalStatementsInLog() throws IOException {
    DaemonProtocol proxy = getProxy();
    String pattern = "FATAL";
    return proxy.getNumberOfMatchesInLogFile(pattern);
  }

  /**
   * Gets number of times ERROR log messages where logged in Daemon logs. 
   * <br/>
   * Pattern used for searching is ERROR. <br/>
   * 
   * @return number of occurrence of error message.
   * @throws IOException
   */
  public int getNumberOfErrorStatementsInLog() throws IOException {
    DaemonProtocol proxy = getProxy();
    String pattern = "ERROR";
    return proxy.getNumberOfMatchesInLogFile(pattern);
  }

  /**
   * Gets number of times Warning log messages where logged in Daemon logs. 
   * <br/>
   * Pattern used for searching is WARN. <br/>
   * 
   * @return number of occurrence of warning message.
   * @throws IOException
   */
  public int getNumberOfWarnStatementsInLog() throws IOException {
    DaemonProtocol proxy = getProxy();
    String pattern = "WARN";
    return proxy.getNumberOfMatchesInLogFile(pattern);
  }

  /**
   * Gets number of time given Exception were present in log file. <br/>
   * 
   * @param e exception class.
   * @return number of exceptions in log
   * @throws IOException
   */
  public int getNumberOfExceptionsInLog(Exception e)
      throws IOException {
    DaemonProtocol proxy = getProxy();
    String pattern = e.getClass().getSimpleName();
    return proxy.getNumberOfMatchesInLogFile(pattern);
  }

  /**
   * Number of times ConcurrentModificationException present in log file. 
   * <br/>
   * @return number of times exception in log file.
   * @throws IOException
   */
  public int getNumberOfConcurrentModificationExceptionsInLog()
      throws IOException {
    return getNumberOfExceptionsInLog(new ConcurrentModificationException());
  }

  private int errorCount;
  private int fatalCount;
  private int concurrentExceptionCount;

  /**
   * Populate the initial exception counts to be used to assert once a testcase
   * is done there was no exception in the daemon when testcase was run.
   * 
   * @throws IOException
   */
  protected void populateExceptionCount() throws IOException {
    errorCount = getNumberOfErrorStatementsInLog();
    LOG.info("Number of error messages in logs : " + errorCount);
    fatalCount = getNumberOfFatalStatementsInLog();
    LOG.info("Number of fatal statement in logs : " + fatalCount);
    concurrentExceptionCount =
        getNumberOfConcurrentModificationExceptionsInLog();
    LOG.info("Number of concurrent modification in logs : "
        + concurrentExceptionCount);
  }

  /**
   * Assert if the new exceptions were logged into the log file.
   * <br/>
   * <b><i>
   * Pre-req for the method is that populateExceptionCount() has 
   * to be called before calling this method.</b></i>
   * @throws IOException
   */
  protected void assertNoExceptionsOccurred() throws IOException {
    int newerrorCount = getNumberOfErrorStatementsInLog();
    LOG.info("Number of error messages while asserting : " + newerrorCount);
    int newfatalCount = getNumberOfFatalStatementsInLog();
    LOG.info("Number of fatal messages while asserting : " + newfatalCount);
    int newconcurrentExceptionCount =
        getNumberOfConcurrentModificationExceptionsInLog();
    LOG.info("Number of concurrentmodification execption while asserting :"
        + newconcurrentExceptionCount);
    Assert.assertEquals(
        "New Error Messages logged in the log file", errorCount, newerrorCount);
    Assert.assertEquals(
        "New Fatal messages logged in the log file", fatalCount, newfatalCount);
    Assert.assertEquals(
        "New ConcurrentModificationException in log file",
        concurrentExceptionCount, newconcurrentExceptionCount);
  }
}
