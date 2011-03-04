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

package org.apache.hadoop.test.system;

import java.io.IOException;
import java.util.*;

import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.test.system.process.RemoteProcess;

import javax.management.*;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

/**
 * Abstract class which encapsulates the DaemonClient which is used in the 
 * system tests.<br/>
 * 
 * @param PROXY the proxy implementation of a specific Daemon 
 */
public abstract class AbstractDaemonClient<PROXY extends DaemonProtocol> {
  private Configuration conf;
  private Boolean jmxEnabled = null;
  private MBeanServerConnection connection;
  private int jmxPortNumber = -1;
  private RemoteProcess process;
  private boolean connected;

  private static final Log LOG = LogFactory.getLog(AbstractDaemonClient.class);
  private static final String HADOOP_JMX_DOMAIN = "Hadoop";

  /**
   * Create a Daemon client.<br/>
   * 
   * @param conf client to be used by proxy to connect to Daemon.
   * @param process the Daemon process to manage the particular daemon.
   * 
   * @throws IOException on RPC error
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
   * @throws IOException on RPC error
   */
  public abstract void connect() throws IOException;

  /**
   * Disconnect the underlying RPC proxy to the daemon.<br/>
   * @throws IOException in case of communication errors
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
   * @throws IOException on RPC error
   */
  public boolean isReady() throws IOException {
    return getProxy().isReady();
  }

  /**
   * Kills the Daemon process <br/>
   * @throws IOException on RPC error
   */
  public void kill() throws IOException {
    process.kill();
  }

  /**
   * Checks if the Daemon process is alive or not <br/>
   * @throws IOException on RPC error
   */
  public void ping() throws IOException {
    getProxy().ping();
  }

  /**
   * Start up the Daemon process. <br/>
   * @throws IOException on RPC error
   */
  public void start() throws IOException {
    process.start();
  }

  /**
   * Get system level view of the Daemon process.
   * 
   * @return returns system level view of the Daemon process.
   * 
   * @throws IOException on RPC error. 
   */
  public ProcessInfo getProcessInfo() throws IOException {
    return getProxy().getProcessInfo();
  }

  /**
   * Checks remote daemon process info to see if certain JMX sys. properties
   * are available and reckon if the JMX service is enabled on the remote side
   * @return <code>boolean</code> code indicating availability of remote JMX
   * @throws IOException is throws in case of communication errors
   */
  public boolean isJmxEnabled () throws IOException {
    if (jmxEnabled != null) return jmxEnabled;
    boolean ret = false;
    String hadoopOpts = getProcessInfo().getEnv().get("HADOOP_OPTS");
    String [] options = hadoopOpts.split(" ");
    for (String option : options) {
      if (option.equals("-Dcom.sun.management.jmxremote")) {
        ret = true;
        break;
      }
    }
    jmxEnabled = ret;
    return ret;
  }

  /**
   * Checks remote daemon process info to find remote JMX server port number
   * @return number of remote JMX server or -1 if it can't be found
   * @throws IOException is throws in case of communication errors
   * @throws IllegalArgumentException
   *    if non-integer port is set in the remote process info
   */
  public int getJmxPortNumber () throws IOException, IllegalArgumentException {
    if (jmxPortNumber != -1) return jmxPortNumber;
    boolean found=false;
    String hadoopOpts = getProcessInfo().getEnv().get("HADOOP_OPTS");
    int portNumber = 0;
    String [] options = hadoopOpts.split(" ");
    for (String option : options) {
      if (option.startsWith("-Dcom.sun.management.jmxremote.port")) {
        found = true;
        try {
          portNumber = Integer.parseInt(option.split("=")[1]);
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("JMX port number isn't integer");
        }
        break;
      }
    }
    if (!found)
      throw new IllegalArgumentException("Can't detect JMX port number");
    jmxPortNumber = portNumber;
    return jmxPortNumber;
  }

  /**
   * Return a file status object that represents the path.
   * @param path
   *          given path
   * @param local
   *          whether the path is local or not
   * @return a FileStatus object
   * @throws IOException see specific implementation
   */
  public FileStatus getFileStatus(String path, boolean local) throws IOException {
    return getProxy().getFileStatus(path, local);
  }

  /**
   * Create a file with full permissions in a file system.
   * @param path - source path where the file has to create.
   * @param fileName - file name
   * @param local - identifying the path whether its local or not.
   * @throws IOException - if an I/O error occurs.
   */
  public void createFile(String path, String fileName, 
      boolean local) throws IOException {
    getProxy().createFile(path, fileName, null, local);
  }

  /**
   * Create a file with given permissions in a file system.
   * @param path - source path where the file has to create.
   * @param fileName - file name.
   * @param permission - file permissions.
   * @param local - identifying the path whether its local or not.
   * @throws IOException - if an I/O error occurs.
   */
  public void createFile(String path, String fileName, 
     FsPermission permission,  boolean local) throws IOException {
    getProxy().createFile(path, fileName, permission, local);
  }

  /**
   * Create a folder with default permissions in a file system.
   * @param path - source path where the file has to be creating.
   * @param folderName - folder name.
   * @param local - identifying the path whether its local or not.
   * @throws IOException - if an I/O error occurs. 
   */
  public void createFolder(String path, String folderName, 
     boolean local) throws IOException {
    getProxy().createFolder(path, folderName, null, local);
  }

  /**
   * Create a folder with given permissions in a file system.
   * @param path - source path where the file has to be creating.
   * @param folderName - folder name.
   * @param permission - folder permissions.
   * @param local - identifying the path whether its local or not.
   * @throws IOException - if an I/O error occurs.
   */
  public void createFolder(String path, String folderName, 
     FsPermission permission,  boolean local) throws IOException {
    getProxy().createFolder(path, folderName, permission, local);
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
   * @throws IOException on RPC error. 
   */
  public FileStatus[] listStatus(String path, boolean local) 
    throws IOException {
    return getProxy().listStatus(path, local);
  }

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory recursive/nonrecursively depending on parameters
   * 
   * @param f
   *          given path
   * @param local
   *          whether the path is local or not
   * @param recursive 
   *          whether to recursively get the status
   * @return the statuses of the files/directories in the given patch
   * @throws IOException is thrown on RPC error. 
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
   * @param excludeExpList list of exception to exclude 
   * @return number of occurrence of fatal message.
   * @throws IOException in case of communication errors
   */
  public int getNumberOfFatalStatementsInLog(String [] excludeExpList)
      throws IOException {
    DaemonProtocol proxy = getProxy();
    String pattern = "FATAL";
    return proxy.getNumberOfMatchesInLogFile(pattern, excludeExpList);
  }

  /**
   * Gets number of times ERROR log messages where logged in Daemon logs. 
   * <br/>
   * Pattern used for searching is ERROR. <br/>
   * @param excludeExpList list of exception to exclude 
   * @return number of occurrence of error message.
   * @throws IOException is thrown on RPC error. 
   */
  public int getNumberOfErrorStatementsInLog(String[] excludeExpList) 
      throws IOException {
    DaemonProtocol proxy = getProxy();
    String pattern = "ERROR";    
    return proxy.getNumberOfMatchesInLogFile(pattern, excludeExpList);
  }

  /**
   * Gets number of times Warning log messages where logged in Daemon logs. 
   * <br/>
   * Pattern used for searching is WARN. <br/>
   * @param excludeExpList list of exception to exclude 
   * @return number of occurrence of warning message.
   * @throws IOException thrown on RPC error. 
   */
  public int getNumberOfWarnStatementsInLog(String[] excludeExpList) 
      throws IOException {
    DaemonProtocol proxy = getProxy();
    String pattern = "WARN";
    return proxy.getNumberOfMatchesInLogFile(pattern, excludeExpList);
  }

  /**
   * Gets number of time given Exception were present in log file. <br/>
   * 
   * @param e exception class.
   * @param excludeExpList list of exceptions to exclude. 
   * @return number of exceptions in log
   * @throws IOException is thrown on RPC error. 
   */
  public int getNumberOfExceptionsInLog(Exception e,
      String[] excludeExpList) throws IOException {
    DaemonProtocol proxy = getProxy();
    String pattern = e.getClass().getSimpleName();    
    return proxy.getNumberOfMatchesInLogFile(pattern, excludeExpList);
  }

  /**
   * Number of times ConcurrentModificationException present in log file. 
   * <br/>
   * @param excludeExpList list of exceptions to exclude.
   * @return number of times exception in log file.
   * @throws IOException is thrown on RPC error. 
   */
  public int getNumberOfConcurrentModificationExceptionsInLog(
      String[] excludeExpList) throws IOException {
    return getNumberOfExceptionsInLog(new ConcurrentModificationException(),
        excludeExpList);
  }

  private int errorCount;
  private int fatalCount;
  private int concurrentExceptionCount;

  /**
   * Populate the initial exception counts to be used to assert once a testcase
   * is done there was no exception in the daemon when testcase was run.
   * @param excludeExpList list of exceptions to exclude
   * @throws IOException is thrown on RPC error. 
   */
  protected void populateExceptionCount(String [] excludeExpList) 
      throws IOException {
    errorCount = getNumberOfErrorStatementsInLog(excludeExpList);
    LOG.info("Number of error messages in logs : " + errorCount);
    fatalCount = getNumberOfFatalStatementsInLog(excludeExpList);
    LOG.info("Number of fatal statement in logs : " + fatalCount);
    concurrentExceptionCount =
        getNumberOfConcurrentModificationExceptionsInLog(excludeExpList);
    LOG.info("Number of concurrent modification in logs : "
        + concurrentExceptionCount);
  }

  /**
   * Assert if the new exceptions were logged into the log file.
   * <br/>
   * <b><i>
   * Pre-req for the method is that populateExceptionCount() has 
   * to be called before calling this method.</b></i>
   * @param excludeExpList list of exceptions to exclude
   * @throws IOException is thrown on RPC error. 
   */
  protected void assertNoExceptionsOccurred(String [] excludeExpList) 
      throws IOException {
    int newerrorCount = getNumberOfErrorStatementsInLog(excludeExpList);
    LOG.info("Number of error messages while asserting :" + newerrorCount);
    int newfatalCount = getNumberOfFatalStatementsInLog(excludeExpList);
    LOG.info("Number of fatal messages while asserting : " + newfatalCount);
    int newconcurrentExceptionCount =
        getNumberOfConcurrentModificationExceptionsInLog(excludeExpList);
    LOG.info("Number of concurrentmodification exception while asserting :"
        + newconcurrentExceptionCount);
    Assert.assertEquals(
        "New Error Messages logged in the log file", errorCount, newerrorCount);
    Assert.assertEquals(
        "New Fatal messages logged in the log file", fatalCount, newfatalCount);
    Assert.assertEquals(
        "New ConcurrentModificationException in log file",
        concurrentExceptionCount, newconcurrentExceptionCount);
  }

  /**
   * Builds correct name of JMX object name from given domain, service name, type
   * @param domain JMX domain name
   * @param serviceName of the service where MBean is registered (NameNode)
   * @param typeName of the MXBean class
   * @return ObjectName for requested MXBean of <code>null</code> if one wasn't
   *    found
   * @throws java.io.IOException in if object name is malformed
   */
  protected ObjectName getJmxBeanName(String domain, String serviceName,
                                      String typeName) throws IOException {
    if (domain == null)
      domain = HADOOP_JMX_DOMAIN;

    ObjectName jmxBean;
    try {
      jmxBean = new ObjectName(domain + ":service=" + serviceName +
        ",name=" + typeName);
    } catch (MalformedObjectNameException e) {
      LOG.debug(e.getStackTrace());
      throw new IOException(e);
    }
    return jmxBean;
  }

  /**
   * Create connection with the remove JMX server at given host and port
   * @param host name of the remote JMX server host
   * @param port port number of the remote JXM server host
   * @return instance of MBeanServerConnection or <code>null</code> if one
   *    hasn't been established
   * @throws IOException in case of comminication errors
   */
  protected MBeanServerConnection establishJmxConnection(String host, int port)
    throws IOException {
    if (connection != null) return connection;
    String urlPattern = null;
    try {
      urlPattern = "service:jmx:rmi:///jndi/rmi://" +
        host + ":" + port +
        "/jmxrmi";
      JMXServiceURL url = new JMXServiceURL(urlPattern);
      JMXConnector connector = JMXConnectorFactory.connect(url, null);
      connection = connector.getMBeanServerConnection();
    } catch (java.net.MalformedURLException badURLExc) {
      LOG.debug("bad url: " + urlPattern, badURLExc);
      throw new IOException(badURLExc);
    }
    return connection;
  }

  Hashtable<String, ObjectName> jmxObjectNames =
    new Hashtable<String, ObjectName>();

  /**
   * Method implements all logic for receiving a bean's attribute.
   * If any initializations such as establishing bean server connections, etc.
   * are need it will do it.
   * @param serviceName name of the service where MBean is registered (NameNode)
   * @param type name of the MXBean class
   * @param attributeName name of the attribute to be retrieved
   * @return Object value of the attribute or <code>null</code> if not found
   * @throws IOException is thrown in case of any errors
   */
  protected Object getJmxAttribute (String serviceName,
                                    String type,
                                    String attributeName)
    throws IOException {
    Object retAttribute = null;
    String domain = null;
    if (isJmxEnabled()) {
      try {
        MBeanServerConnection conn =
          establishJmxConnection(getHostName(), getJmxPortNumber());
        for (String d : conn.getDomains()) {
          if (d != null && d.startsWith(HADOOP_JMX_DOMAIN))
            domain = d;
        }
        if (!jmxObjectNames.containsKey(type))
          jmxObjectNames.put(type, getJmxBeanName(domain, serviceName, type));
        retAttribute =
          conn.getAttribute(jmxObjectNames.get(type), attributeName);
      } catch (MBeanException e) {
        LOG.debug(e.getStackTrace());
        throw new IOException(e);
      } catch (AttributeNotFoundException e) {
        LOG.warn(e.getStackTrace());
        throw new IOException(e);
      } catch (InstanceNotFoundException e) {
        LOG.warn(e.getStackTrace());
        throw new IOException(e);
      } catch (ReflectionException e) {
        LOG.debug(e.getStackTrace());
        throw new IOException(e);
      }
    }
    return retAttribute;
  }

  /**
   * This method has to be implemented by appropriate concrete daemon client
   * e.g. DNClient, NNClient, etc.
   * Concrete implementation has to provide names of the service and bean type
   * @param attributeName name of the attribute to be retrieved
   * @return Object value of the given attribute
   * @throws IOException is thrown in case of communication errors
   */
  public abstract Object getDaemonAttribute (String attributeName)
    throws IOException;
}
