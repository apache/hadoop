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

import com.jcraft.jsch.*;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * Remote Execution of commands  on a remote machine.
 */

public class SSHRemoteExecution implements RemoteExecution {

  static final Log LOG = LogFactory.getLog(SSHRemoteExecution.class);
  static final int SSH_PORT = 22;
  static final String DEFAULT_IDENTITY="id_dsa";
  static final String DEFAULT_KNOWNHOSTS="known_hosts";
  static final String FS = System.getProperty("file.separator");
  static final String LS = System.getProperty("line.separator");
  private int exitCode;
  private StringBuffer output;
  private String commandString;

  final StringBuffer errorMessage = new StringBuffer();
  public SSHRemoteExecution() throws Exception {
  }

  protected String getHomeDir() {
    String currentUser=System.getProperty("user.name");
    String userHome=System.getProperty("user.home");

    return userHome.substring(0, userHome.indexOf(currentUser)-1);
  }

  /**
   * Execute command at remote host under given user
   * @param remoteHostName remote host name
   * @param user is the name of the user to be login under;
   *   current user will be used if this is set to <code>null</code>
   * @param command to be executed remotely
   * @param identityFile is the name of alternative identity file; default
   *   is ~user/.ssh/id_dsa
   * @param portNumber remote SSH daemon port number, default is 22
   * @throws Exception in case of errors
   */
  public void executeCommand (String remoteHostName, String user,
          String  command, String identityFile, int portNumber) throws Exception {
    commandString = command;
    String sessionUser = System.getProperty("user.name");
    String userHome=System.getProperty("user.home");
    if (user != null) {
      sessionUser = user;
      userHome = getHomeDir() + FS + user;
    }
    String dotSSHDir = userHome + FS + ".ssh";
    String sessionIdentity = dotSSHDir + FS + DEFAULT_IDENTITY;
    if (identityFile != null) {
      sessionIdentity = identityFile;
    }

    JSch jsch = new JSch();

    Session session = jsch.getSession(sessionUser, remoteHostName, portNumber);
    jsch.setKnownHosts(dotSSHDir + FS + DEFAULT_KNOWNHOSTS);
    jsch.addIdentity(sessionIdentity);

    Properties config = new Properties();
    config.put("StrictHostKeyChecking", "no");
    session.setConfig(config);

    session.connect(30000);   // making a connection with timeout.

    Channel channel=session.openChannel("exec");
    ((ChannelExec)channel).setCommand(command);
    channel.setInputStream(null);

    final BufferedReader errReader =
            new BufferedReader(
              new InputStreamReader(((ChannelExec)channel).getErrStream()));
    BufferedReader inReader =
            new BufferedReader(new InputStreamReader(channel.getInputStream()));

    channel.connect();
    Thread errorThread = new Thread() {
      @Override
      public void run() {
        try {
          String line = errReader.readLine();
          while((line != null) && !isInterrupted()) {
            errorMessage.append(line);
            errorMessage.append(LS);
            line = errReader.readLine();
          }
        } catch(IOException ioe) {
          LOG.warn("Error reading the error stream", ioe);
        }
      }
    };

    try {
      errorThread.start();
    } catch (IllegalStateException e) {
      LOG.debug(e);
    }
    try {
      parseExecResult(inReader);
      String line = inReader.readLine();
      while (line != null) {
        line = inReader.readLine();
      }

      if(channel.isClosed()) {
        exitCode = channel.getExitStatus();
        LOG.debug("exit-status: " + exitCode);
      }
      try {
        // make sure that the error thread exits
        errorThread.join();
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted while reading the error stream", ie);
      }
    } catch (Exception ie) {
      throw new IOException(ie.toString());
    }
    finally {
      try {
        inReader.close();
      } catch (IOException ioe) {
        LOG.warn("Error while closing the input stream", ioe);
      }
      try {
        errReader.close();
      } catch (IOException ioe) {
        LOG.warn("Error while closing the error stream", ioe);
      }
      channel.disconnect();
      session.disconnect();
    }
  }

  /**
   * Execute command at remote host under given username
   * Default identity is ~/.ssh/id_dsa key will be used
   * Default known_hosts file is ~/.ssh/known_hosts will be used
   * @param remoteHostName remote host name
   * @param user is the name of the user to be login under;
   *   if equals to <code>null</code> then current user name will be used
   * @param command to be executed remotely
   */
  @Override
  public void executeCommand (String remoteHostName, String user,
          String  command) throws Exception {
    executeCommand(remoteHostName, user, command, null, SSH_PORT);
  }

  @Override
  public int getExitCode() {
    return exitCode;
  }

  protected void parseExecResult(BufferedReader lines) throws IOException {
    output = new StringBuffer();
    char[] buf = new char[512];
    int nRead;
    while ( (nRead = lines.read(buf, 0, buf.length)) > 0 ) {
      output.append(buf, 0, nRead);
    }
  }

  /** Get the output of the ssh command.*/
  @Override
  public String getOutput() {
    return (output == null) ? "" : output.toString();
  }

  /** Get the String representation of ssh command */
  @Override
  public String getCommandString() {
    return commandString;
  }
}
