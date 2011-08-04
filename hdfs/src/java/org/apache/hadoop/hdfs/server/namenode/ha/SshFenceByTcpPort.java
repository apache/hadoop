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
package org.apache.hadoop.hdfs.server.namenode.ha;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import com.google.common.annotations.VisibleForTesting;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

/**
 * This fencing implementation sshes to the target node and uses <code>fuser</code>
 * to kill the process listening on the NameNode's TCP port. This is
 * more accurate than using "jps" since it doesn't require parsing,
 * and will work even if there are multiple NameNodes running on the
 * same machine.<p>
 * It returns a successful status code if:
 * <ul>
 * <li><code>fuser</code> indicates it successfully killed a process, <em>or</em>
 * <li><code>nc -z</code> indicates that nothing is listening on the target port
 * </ul>
 * <p>
 * This fencing mechanism is configured as following in the fencing method
 * list:
 * <code>sshfence([username@]nnhost[:ssh-port][, target-nn-port])</code>
 * where the first argument specifies the username, host, and port to ssh
 * into, and the second argument specifies the port on which the target
 * NN process is listening on.
 * <p>
 * For example, <code>sshfence(other-nn, 8020)<code> will SSH into
 * <code>other-nn<code> as the current user on the standard SSH port,
 * then kill whatever process is listening on port 8020.
 * <p>
 * If no <code>target-nn-port</code> is specified, it is assumed that the
 * target NameNode is listening on the same port as the local NameNode.
 * <p>
 * In order to achieve passwordless SSH, the operator must also configure
 * <code>dfs.namenode.ha.fencing.ssh.private-key-files<code> to point to an
 * SSH key that has passphrase-less access to the given username and host.
 */
public class SshFenceByTcpPort extends Configured
  implements FenceMethod {

  static final Log LOG = LogFactory.getLog(
      SshFenceByTcpPort.class);
  
  static final String CONF_CONNECT_TIMEOUT_KEY =
    "dfs.namenode.ha.fencing.ssh.connect-timeout";
  private static final int CONF_CONNECT_TIMEOUT_DEFAULT =
    30*1000;
  static final String CONF_IDENTITIES_KEY =
    "dfs.namenode.ha.fencing.ssh.private-key-files";

  /**
   * Verify that the arguments are parseable and that the host
   * can be resolved.
   */
  @Override
  public void checkArgs(String argStr) throws BadFencingConfigurationException {
    Args args = new Args(argStr);
    try {
      InetAddress.getByName(args.host);
    } catch (UnknownHostException e) {
      throw new BadFencingConfigurationException(
          "Unknown host: " + args.host);
    }
  }

  @Override
  public boolean tryFence(String argsStr)
      throws BadFencingConfigurationException {
    Args args = new Args(argsStr);
    
    Session session;
    try {
      session = createSession(args);
    } catch (JSchException e) {
      LOG.warn("Unable to create SSH session", e);
      return false;
    }

    LOG.info("Connecting to " + args.host + "...");
    
    try {
      session.connect(getSshConnectTimeout());
    } catch (JSchException e) {
      LOG.warn("Unable to connect to " + args.host
          + " as user " + args.user, e);
      return false;
    }
    LOG.info("Connected to " + args.host);

    int targetPort = args.targetPort != null ?
        args.targetPort : getDefaultNNPort();
    try {
      return doFence(session, targetPort);
    } catch (JSchException e) {
      LOG.warn("Unable to achieve fencing on remote host", e);
      return false;
    } finally {
      session.disconnect();
    }
  }


  private Session createSession(Args args) throws JSchException {
    JSch jsch = new JSch();
    for (String keyFile : getKeyFiles()) {
      jsch.addIdentity(keyFile);
    }
    JSch.setLogger(new LogAdapter());

    Session session = jsch.getSession(args.user, args.host, args.sshPort);
    session.setConfig("StrictHostKeyChecking", "no");
    return session;
  }

  private boolean doFence(Session session, int nnPort) throws JSchException {
    try {
      LOG.info("Looking for process running on port " + nnPort);
      int rc = execCommand(session,
          "PATH=$PATH:/sbin:/usr/sbin fuser -v -k -n tcp " + nnPort);
      if (rc == 0) {
        LOG.info("Successfully killed process that was " +
            "listening on port " + nnPort);
        // exit code 0 indicates the process was successfully killed.
        return true;
      } else if (rc == 1) {
        // exit code 1 indicates either that the process was not running
        // or that fuser didn't have root privileges in order to find it
        // (eg running as a different user)
        LOG.info(
            "Indeterminate response from trying to kill NameNode. " +
            "Verifying whether it is running using nc...");
        rc = execCommand(session, "nc -z localhost 8020");
        if (rc == 0) {
          // the NN is still listening - we are unable to fence
          LOG.warn("Unable to fence NN - it is running but we cannot kill it");
          return false;
        } else {
          LOG.info("Verified that the NN is down.");
          return true;          
        }
      } else {
        // other 
      }
      LOG.info("rc: " + rc);
      return rc == 0;
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while trying to fence via ssh", e);
      return false;
    } catch (IOException e) {
      LOG.warn("Unknown failure while trying to fence via ssh", e);
      return false;
    }
  }
  
  /**
   * Execute a command through the ssh session, pumping its
   * stderr and stdout to our own logs.
   */
  private int execCommand(Session session, String cmd)
      throws JSchException, InterruptedException, IOException {
    LOG.debug("Running cmd: " + cmd);
    ChannelExec exec = null;
    try {
      exec = (ChannelExec)session.openChannel("exec");
      exec.setCommand(cmd);
      exec.setInputStream(null);
      exec.connect();
      

      // Pump stdout of the command to our WARN logs
      StreamPumper outPumper = new StreamPumper(LOG, cmd + " via ssh",
          exec.getInputStream(), StreamPumper.StreamType.STDOUT);
      outPumper.start();
      
      // Pump stderr of the command to our WARN logs
      StreamPumper errPumper = new StreamPumper(LOG, cmd + " via ssh",
          exec.getErrStream(), StreamPumper.StreamType.STDERR);
      errPumper.start();
      
      outPumper.join();
      errPumper.join();
      return exec.getExitStatus();
    } finally {
      cleanup(exec);
    }
  }

  private static void cleanup(ChannelExec exec) {
    if (exec != null) {
      try {
        exec.disconnect();
      } catch (Throwable t) {
        LOG.warn("Couldn't disconnect ssh channel", t);
      }
    }
  }

  private int getSshConnectTimeout() {
    return getConf().getInt(
        CONF_CONNECT_TIMEOUT_KEY, CONF_CONNECT_TIMEOUT_DEFAULT);
  }

  private Collection<String> getKeyFiles() {
    return getConf().getTrimmedStringCollection(CONF_IDENTITIES_KEY);
  }
  
  private int getDefaultNNPort() {
    return NameNode.getAddress(getConf()).getPort();
  }

  /**
   * Container for the parsed arg line for this fencing method.
   */
  @VisibleForTesting
  static class Args {
    private static final Pattern USER_HOST_PORT_RE = Pattern.compile(
      "(?:(.+?)@)?([^:]+?)(?:\\:(\\d+))?");

    private static final int DEFAULT_SSH_PORT = 22;

    final String user;
    final String host;
    final int sshPort;
    
    final Integer targetPort;
    
    public Args(String args) throws BadFencingConfigurationException {
      if (args == null) {
        throw new BadFencingConfigurationException(
            "Must specify args for ssh fencing configuration");
      }
      String[] argList = args.split(",\\s*");
      if (argList.length > 2 || argList.length == 0) {
        throw new BadFencingConfigurationException(
            "Incorrect number of arguments: " + args);
      }
      
      // Parse SSH destination.
      String sshDestArg = argList[0];
      Matcher m = USER_HOST_PORT_RE.matcher(sshDestArg);
      if (!m.matches()) {
        throw new BadFencingConfigurationException(
            "Unable to parse SSH destination: "+ sshDestArg);
      }
      if (m.group(1) != null) {
        user = m.group(1);
      } else {
        user = System.getProperty("user.name");
      }
      
      host = m.group(2);

      if (m.group(3) != null) {
        sshPort = parseConfiggedPort(m.group(3));
      } else {
        sshPort = DEFAULT_SSH_PORT;
      }
      
      // Parse target port.
      if (argList.length > 1) {
        targetPort = parseConfiggedPort(argList[1]);
      } else {
        targetPort = null;
      }
    }

    private Integer parseConfiggedPort(String portStr)
        throws BadFencingConfigurationException {
      try {
        return Integer.valueOf(portStr);
      } catch (NumberFormatException nfe) {
        throw new BadFencingConfigurationException(
            "Port number '" + portStr + "' invalid");
      }
    }
  }

  /**
   * Adapter from JSch's logger interface to our log4j
   */
  private static class LogAdapter implements com.jcraft.jsch.Logger {
    static final Log LOG = LogFactory.getLog(
        SshFenceByTcpPort.class.getName() + ".jsch");

    public boolean isEnabled(int level) {
      switch (level) {
      case com.jcraft.jsch.Logger.DEBUG:
        return LOG.isDebugEnabled();
      case com.jcraft.jsch.Logger.INFO:
        return LOG.isInfoEnabled();
      case com.jcraft.jsch.Logger.WARN:
        return LOG.isWarnEnabled();
      case com.jcraft.jsch.Logger.ERROR:
        return LOG.isErrorEnabled();
      case com.jcraft.jsch.Logger.FATAL:
        return LOG.isFatalEnabled();
      default:
        return false;
      }
    }
      
    public void log(int level, String message) {
      switch (level) {
      case com.jcraft.jsch.Logger.DEBUG:
        LOG.debug(message);
        break;
      case com.jcraft.jsch.Logger.INFO:
        LOG.info(message);
        break;
      case com.jcraft.jsch.Logger.WARN:
        LOG.warn(message);
        break;
      case com.jcraft.jsch.Logger.ERROR:
        LOG.error(message);
        break;
      case com.jcraft.jsch.Logger.FATAL:
        LOG.fatal(message);
        break;
      }
    }
  }
}
