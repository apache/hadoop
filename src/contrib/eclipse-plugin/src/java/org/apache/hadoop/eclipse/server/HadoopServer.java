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

package org.apache.hadoop.eclipse.server;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;
import java.util.logging.Logger;

import javax.net.SocketFactory;

import org.apache.hadoop.eclipse.JSchUtilities;
import org.apache.hadoop.eclipse.launch.SWTUserInfo;
import org.apache.hadoop.eclipse.servers.ServerRegistry;
import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.core.runtime.IStatus;
import org.eclipse.core.runtime.Status;
import org.eclipse.core.runtime.jobs.Job;
import org.eclipse.debug.core.DebugPlugin;
import org.eclipse.debug.core.ILaunchConfiguration;
import org.eclipse.debug.core.ILaunchConfigurationType;
import org.eclipse.debug.core.ILaunchConfigurationWorkingCopy;
import org.eclipse.debug.core.model.ILaunchConfigurationDelegate;
import org.eclipse.debug.ui.DebugUITools;
import org.eclipse.swt.widgets.Display;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

/**
 * Methods for defining and interacting with a Hadoop MapReduce server
 */

public class HadoopServer {

  private static final int JOB_TRACKER_PORT = 50030;

  private PingJob ping;

  protected static final long PING_DELAY = 1500;

  /**
   * Location of Hadoop jars on the server
   */
  private String installPath;

  /**
   * User name to use to connect to the server
   */
  private String userName;

  /**
   * Host name of the hadoop server
   */
  private String hostName;

  private String password;

  // state and status - transient
  private transient String state = "";

  private transient Map<String, HadoopJob> jobs =
      Collections.synchronizedMap(new TreeMap<String, HadoopJob>());

  private transient List<JarModule> jars =
      Collections.synchronizedList(new ArrayList<JarModule>());

  /**
   * User-defined name for the server (set from Eclipse)
   */
  private String name;

  /**
   * Host name of the tunneling machine
   */
  private String tunnelHostName;

  /**
   * User name to use to connect to the tunneling machine
   */
  private String tunnelUserName;

  private String tunnelPassword;

  static Logger log = Logger.getLogger(HadoopServer.class.getName());

  public HadoopServer(String uri, String name) {
    this.name = name;

    String[] hostInfo = uri.split(":");
    String[] loginInfo = hostInfo[0].split("@");

    installPath = hostInfo[1];
    userName = loginInfo[0];
    hostName = loginInfo[1];
  }

  public HadoopServer(String uri, String name, String tunnelVia,
      String tunnelUserName) {
    this(uri, name);
    this.tunnelHostName = tunnelVia;
    this.tunnelUserName = tunnelUserName;
  }

  /**
   * Create an SSH session with no timeout
   * 
   * @return Session object with no timeout
   * @throws JSchException
   */
  public Session createSessionNoTimeout() throws JSchException {
    return createSession(0);
  }

  /**
   * Create an SSH session with no timeout
   * 
   * @return Session object with no timeout
   * @throws JSchException
   */
  public Session createSession() throws JSchException {
    return createSession(0);
  }

  /**
   * Creates a SSH session with a specified timeout
   * 
   * @param timeout the amount of time before the session expires
   * @return Returns the created session object representing the SSH session.
   * @throws JSchException
   */
  public Session createSession(int timeout) throws JSchException {
    if (tunnelHostName == null) {
      Session session =
          JSchUtilities.createJSch().getSession(userName, hostName, 22);
      session.setUserInfo(new SWTUserInfo() {
        @Override
        public String getPassword() {
          return HadoopServer.this.password;
        }

        @Override
        public void setPassword(String pass) {
          HadoopServer.this.password = pass;
        }

      });
      if (!session.isConnected()) {
        try {
          session.connect();
        } catch (JSchException jse) {
          // Reset password in case the authentication failed
          if (jse.getMessage().equals("Auth fail"))
            this.password = null;
          throw jse;
        }
      }

      return session;
    } else {
      createSshTunnel();

      Session session =
          JSchUtilities.createJSch().getSession(userName, "localhost",
              tunnelPort);
      session.setUserInfo(new SWTUserInfo() {
        @Override
        public String getPassword() {
          return HadoopServer.this.password;
        }

        @Override
        public void setPassword(String pass) {
          HadoopServer.this.password = pass;
        }
      });
      if (!session.isConnected()) {
        try {
          session.connect();
        } catch (JSchException jse) {
          // Reset password in case the authentication failed
          if (jse.getMessage().equals("Auth fail"))
            this.password = null;
          throw jse;
        }
      }
      if (timeout > -1) {
        session.setTimeout(timeout);
      }
      return session;
    }
  }

  private Session createTunnel(int port) throws JSchException {
    Session tunnel;

    tunnelPort = -1;
    for (int i = 0; !((i > 4) || (tunnelPort > -1)); i++) {
      try {
        Socket socket = SocketFactory.getDefault().createSocket();
        socket.bind(null);
        tunnelPort = socket.getLocalPort();
        socket.close();
      } catch (IOException e) {
        // ignore, retry
      }
    }

    if (tunnelPort == -1) {
      throw new JSchException("No free local port found to bound to");
    }

    tunnel =
        JSchUtilities.createJSch().getSession(tunnelUserName,
            tunnelHostName, 22);
    tunnel.setTimeout(0);
    tunnel.setPortForwardingL(tunnelPort, hostName, port);

    tunnel.setUserInfo(new SWTUserInfo() {
      @Override
      public String getPassword() {
        return HadoopServer.this.tunnelPassword;
      }

      @Override
      public void setPassword(String password) {
        HadoopServer.this.tunnelPassword = password;
      }
    });

    try {
      tunnel.connect();
    } catch (JSchException jse) {
      // Reset password in case the authentication failed
      if (jse.getMessage().equals("Auth fail"))
        this.tunnelPassword = null;
      throw jse;
    }

    return tunnel;
  }

  private void createSshTunnel() throws JSchException {
    if ((sshTunnel != null) && sshTunnel.isConnected()) {
      sshTunnel.disconnect();
    }

    sshTunnel = createTunnel(22);
  }

  private void createHttpTunnel(int port) throws JSchException {
    if ((httpTunnel == null) || !httpTunnel.isConnected()) {
      httpTunnel = createTunnel(port);
    }
  }

  /**
   * Return the effective host name to use to contact the server. The
   * effective host name might be "localhost" if we setup a tunnel.
   * 
   * @return the effective host name to use contact the server
   */
  public String getEffectiveHostName() {
    if ((tunnelHostName != null) && (tunnelHostName.length() > 0)) {
      return "localhost";
    }

    return this.hostName;
  }
  
  public String getHostName() {
    return this.hostName;
  }

  public void setHostname(String hostname) {
    this.hostName = hostname;
  }

  /**
   * Gets the path where the hadoop jars are stored.
   * 
   * @return String containing the path to the hadoop jars.
   */
  public String getInstallPath() {
    return installPath;
  }

  /**
   * Sets the path where the hadoop jars are stored.
   * 
   * @param path The directory where the hadoop jars are stored.
   */
  public void setPath(String path) {
    this.installPath = path;
  }

  public String getUserName() {
    return userName;
  }

  public void setUser(String user) {
    this.userName = user;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    log.fine("Server password set to " + password);
    this.password = password;
  }

  @Override
  public String toString() {
    return this.userName + "@" + this.hostName + ":" + this.installPath;
  }

  public String getName() {
    return this.name;
  }

  /**
   * Returns the URL for the Job Tracker (default is port 50030)
   * 
   * @return URL for the Job Tracker
   * @throws MalformedURLException
   */
  public URL getJobTrackerUrl() throws MalformedURLException {
    if (tunnelHostName == null) {
      return new URL("http://" + getEffectiveHostName() + ":"
          + JOB_TRACKER_PORT + "/jobtracker.jsp");
    } else {
      try {
        createHttpTunnel(JOB_TRACKER_PORT);

        String port = httpTunnel.getPortForwardingL()[0].split(":")[0];
        return new URL("http://localhost:" + port + "/jobtracker.jsp");
      } catch (JSchException e) {
        // / BUG(jz) -- need to display error here
        return null;
      }
    }
  }

  public String getState() {
    return state;
  }

  public Object[] getChildren() {
    /*
     * List all elements that should be present in the Server window (all
     * servers and all jobs running on each servers)
     */
    checkPingJobRunning();
    Collection<Object> collection =
        new ArrayList<Object>(this.jobs.values());
    collection.addAll(jars);
    return collection.toArray();
  }

  private synchronized void checkPingJobRunning() {
    if (ping == null) {
      ping = new PingJob();
      ping.setSystem(true);
      ping.schedule();
    }
  }

  private HashSet<IJobListener> jobListeners = new HashSet<IJobListener>();

  private Session sshTunnel;

  private Session httpTunnel;

  private int tunnelPort;

  private int id;

  public void addJobListener(IJobListener l) {
    jobListeners.add(l);
  }

  protected void fireJobChanged(HadoopJob job) {
    for (IJobListener listener : jobListeners) {
      listener.jobChanged(job);
    }
  }

  protected void fireJobAdded(HadoopJob job) {
    for (IJobListener listener : jobListeners) {
      listener.jobAdded(job);
    }
  }

  protected void fireJarPublishStart(JarModule jar) {
    for (IJobListener listener : jobListeners) {
      listener.publishStart(jar);
    }
  }

  protected void fireJarPublishDone(JarModule jar) {
    for (IJobListener listener : jobListeners) {
      listener.publishDone(jar);
    }
  }

  public void runJar(JarModule jar, IProgressMonitor monitor) {
    log.fine("Run Jar: " + jar);
    ILaunchConfigurationType launchConfigType =
        DebugPlugin.getDefault().getLaunchManager()
            .getLaunchConfigurationType(
                "org.apache.hadoop.eclipse.launch.StartServer");

    jars.add(jar);
    fireJarPublishStart(jar);

    try {
      ILaunchConfiguration[] matchingConfigs =
          DebugPlugin.getDefault().getLaunchManager()
              .getLaunchConfigurations(launchConfigType);
      ILaunchConfiguration launchConfig = null;

      // TODO(jz) allow choosing correct config, for now we're always
      // going to use the first
      if (matchingConfigs.length == 1) {
        launchConfig = matchingConfigs[0];
      } else {
        launchConfig =
            launchConfigType
                .newInstance(null, DebugPlugin.getDefault()
                    .getLaunchManager()
                    .generateUniqueLaunchConfigurationNameFrom(
                        "Run Hadoop Jar"));
      }

      ILaunchConfigurationWorkingCopy copy =
          launchConfig
              .copy("Run " + jar.getName() + " on " + this.getName());

      // COMMENTED(jz) - perform the jarring in the launch delegate now
      // copy.setAttribute("hadoop.jar",
      // jar.buildJar(monitor).toString());

      copy.setAttribute("hadoop.jarrable", jar.toMemento());
      copy.setAttribute("hadoop.host", this.getEffectiveHostName());
      copy.setAttribute("hadoop.user", this.getUserName());
      copy.setAttribute("hadoop.serverid", this.id);
      copy.setAttribute("hadoop.path", this.getInstallPath());
      ILaunchConfiguration saved = copy.doSave();

      // NOTE(jz) became deprecated in 3.3, replaced with getDelegates
      // (plural) method,
      // as this new method is marked experimental leaving as-is for now
      ILaunchConfigurationDelegate delegate =
          launchConfigType.getDelegate("run");
      // only support run for now
      DebugUITools.launch(saved, "run");
    } catch (CoreException e) {
      // TODO(jz) autogen
      e.printStackTrace();
    } finally {
      jars.remove(jar);
      fireJarPublishDone(jar);
    }
  }

  public class PingJob extends Job {
    public PingJob() {
      super("Get MapReduce server status");
    }

    @Override
    protected IStatus run(IProgressMonitor monitor) {
      HttpURLConnection connection = null;

      try {
        connection = (HttpURLConnection) getJobTrackerUrl().openConnection();
        connection.connect();

        String previousState = state;

        if (connection.getResponseCode() == 200) {
          state = "Started";

          StringBuffer string = new StringBuffer();
          byte[] buffer = new byte[1024];
          InputStream in =
              new BufferedInputStream(connection.getInputStream());
          int bytes = 0;
          while ((bytes = in.read(buffer)) != -1) {
            string.append(new String(buffer, 0, bytes));
          }

          HadoopJob[] jobData = getJobData(string.toString());
          for (int i = 0; i < jobData.length; i++) {
            HadoopJob job = jobData[i];
            if (jobs.containsKey((job.getId()))) {
              updateJob(job);
            } else {
              addJob(job);
            }
          }
        } else {
          state = "Stopped";
        }

        if (!state.equals(previousState)) {
          ServerRegistry.getInstance().stateChanged(HadoopServer.this);
        }
      } catch (Exception e) {
        state = "Stopped (Connection Error)";
      }

      schedule(PING_DELAY);
      return Status.OK_STATUS;
    }
  }

  private void updateJob(final HadoopJob data) {
    jobs.put(data.getId(), data);
    // TODO(jz) only if it has changed
    Display.getDefault().syncExec(new Runnable() {
      public void run() {
        fireJobChanged(data);
      }
    });
  }

  private void addJob(final HadoopJob data) {
    jobs.put(data.getId(), data);

    Display.getDefault().syncExec(new Runnable() {
      public void run() {
        fireJobAdded(data);
      }
    });
  }

  /**
   * Parse the job tracker data to display currently running and completed
   * jobs.
   * 
   * @param jobTrackerHtml The HTML returned from the Job Tracker port
   * @return an array of Strings that contain job status info
   */
  public HadoopJob[] getJobData(String jobTrackerHtml) {
    try {
      Vector<HadoopJob> jobsVector = new Vector<HadoopJob>();

      BufferedReader in =
          new BufferedReader(new StringReader(jobTrackerHtml));

      String inputLine;

      boolean completed = false;
      while ((inputLine = in.readLine()) != null) {
        // stop once we reach failed jobs (which are after running and
        // completed jobs)
        if (inputLine.indexOf("Failed Jobs") != -1) {
          break;
        }

        if (inputLine.indexOf("Completed Jobs") != -1) {
          completed = true;
        }

        // skip lines without data (stored in a table)
        if (!inputLine.startsWith("<tr><td><a")) {
          // log.debug (" > " + inputLine, verbose);
          continue;
        }

        HadoopJob jobData = new HadoopJob(HadoopServer.this);

        String[] values = inputLine.split("</td><td>");

        String jobId = values[0].trim();
        String realJobId =
            jobId.substring(jobId.lastIndexOf("_") + 1, jobId
                .lastIndexOf("_") + 5);
        String name = values[2].trim();
        if (name.equals("&nbsp;")) {
          name = "(untitled)";
        }
        jobData.name = name + "(" + realJobId + ")";
        jobData.jobId = "job_" + realJobId;
        jobData.completed = completed;

        jobData.mapPercentage = values[3].trim();
        jobData.totalMaps = values[4].trim();
        jobData.completedMaps = values[5].trim();
        jobData.reducePercentage = values[6].trim();
        jobData.totalReduces = values[7].trim();
        jobData.completedReduces =
            values[8].substring(0, values[8].indexOf("<")).trim();

        jobsVector.addElement(jobData);
      }

      in.close();

      // convert vector to array
      HadoopJob[] jobArray = new HadoopJob[jobsVector.size()];
      for (int j = 0; j < jobsVector.size(); j++) {
        jobArray[j] = jobsVector.elementAt(j);
      }

      return jobArray;
    } catch (Exception e) {
      e.printStackTrace();
    }

    return null;
  }

  public void dispose() {
    if ((sshTunnel != null) && sshTunnel.isConnected()) {
      sshTunnel.disconnect();
    }

    if ((httpTunnel != null) && httpTunnel.isConnected()) {
      httpTunnel.disconnect();
    }
  }

  public String getTunnelHostName() {
    return tunnelHostName;
  }

  public String getTunnelUserName() {
    return tunnelUserName;
  }

  public void setId(int i) {
    this.id = i;
  }

  public void setName(String newName) {
    this.name = newName;
  }

  public void setURI(String newURI) {
    String[] hostInfo = newURI.split(":");
    String[] loginInfo = hostInfo[0].split("@");

    installPath = hostInfo[1];
    userName = loginInfo[0];
    hostName = loginInfo[1];
  }
  
  public void setTunnel(String tunnelHostName, String tunnelUserName) {
    this.tunnelHostName = tunnelHostName;
    this.tunnelUserName = tunnelUserName;
  }
  
  /**
   * Returns whether this server uses SSH tunneling or not
   * @return whether this server uses SSH tunneling or not
   */
  public boolean useTunneling() {
    return (this.tunnelHostName != null);
  }
  
}
