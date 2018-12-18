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

package org.apache.hadoop.yarn.server.nodemanager.webapp;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ShellContainerCommand;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerExecContext;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.security.HadoopKerberosName;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Web socket for establishing interactive command shell connection through
 * Node Manage to container executor.
 */
@InterfaceAudience.LimitedPrivate({ "HDFS", "MapReduce", "YARN" })
@InterfaceStability.Unstable

@WebSocket
public class ContainerShellWebSocket {
  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerShellWebSocket.class);
  private static Context nmContext;

  private final ContainerExecutor exec;
  private IOStreamPair pair;

  public ContainerShellWebSocket() {
    exec = nmContext.getContainerExecutor();
  }

  public static void init(Context nm) {
    ContainerShellWebSocket.nmContext = nm;
  }

  @OnWebSocketMessage
  public void onText(Session session, String message) throws IOException {

    try {
      byte[] buffer = new byte[4000];
      if (session.isOpen()) {
        if (!message.equals("1{}")) {
          // Send keystroke to process input
          byte[] payload;
          payload = message.getBytes(Charset.forName("UTF-8"));
          if (payload != null) {
            pair.out.write(payload);
            pair.out.flush();
          }
        }
        // Render process output
        int no = pair.in.available();
        pair.in.read(buffer, 0, Math.min(no, buffer.length));
        String formatted = new String(buffer, Charset.forName("UTF-8"))
            .replaceAll("\n", "\r\n");
        session.getRemote().sendString(formatted);
      }
    } catch (IOException e) {
      onClose(session, 1001, "Shutdown");
    }

  }

  @OnWebSocketConnect
  public void onConnect(Session session) {
    try {
      URI containerURI = session.getUpgradeRequest().getRequestURI();
      String command = "bash";
      String[] containerPath = containerURI.getPath().split("/");
      String cId = containerPath[2];
      if (containerPath.length==4) {
        for (ShellContainerCommand c : ShellContainerCommand.values()) {
          if (c.name().equalsIgnoreCase(containerPath[3])) {
            command = containerPath[3].toLowerCase();
          }
        }
      }
      Container container = nmContext.getContainers().get(ContainerId
          .fromString(cId));
      if (!checkAuthorization(session, container)) {
        session.close(1008, "Forbidden");
        return;
      }
      if (checkInsecureSetup()) {
        session.close(1003, "Nonsecure mode is unsupported.");
        return;
      }
      LOG.info(session.getRemoteAddress().getHostString() + " connected!");
      LOG.info(
          "Making interactive connection to running docker container with ID: "
              + cId);
      ContainerExecContext execContext = new ContainerExecContext
          .Builder()
          .setContainer(container)
          .setNMLocalPath(nmContext.getLocalDirsHandler())
          .setShell(command)
          .build();
      pair = exec.execContainer(execContext);
    } catch (Exception e) {
      LOG.error("Failed to establish WebSocket connection with Client", e);
    }

  }

  @OnWebSocketClose
  public void onClose(Session session, int status, String reason) {
    try {
      LOG.info(session.getRemoteAddress().getHostString() + " closed!");
      String exit = "exit\r\n";
      pair.out.write(exit.getBytes(Charset.forName("UTF-8")));
      pair.out.flush();
      pair.in.close();
      pair.out.close();
    } catch (IOException e) {
    } finally {
      session.close();
    }
  }

  /**
   * Check if user is authorized to access container.
   * @param session websocket session
   * @param container instance of container to access
   * @return true if user is allowed to access container.
   * @throws IOException
   */
  protected boolean checkAuthorization(Session session, Container container)
      throws IOException {
    boolean authorized = true;
    String user = "";
    if (UserGroupInformation.isSecurityEnabled()) {
      user = new HadoopKerberosName(session.getUpgradeRequest()
          .getUserPrincipal().getName()).getShortName();
    } else {
      Map<String, List<String>> parameters = session.getUpgradeRequest()
          .getParameterMap();
      if (parameters.containsKey("user.name")) {
        List<String> users = parameters.get("user.name");
        user = users.get(0);
      }
    }
    boolean isAdmin = false;
    if (nmContext.getApplicationACLsManager().areACLsEnabled()) {
      UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
      isAdmin = nmContext.getApplicationACLsManager().isAdmin(ugi);
    }
    String containerUser = container.getUser();
    if (!user.equals(containerUser) && !isAdmin) {
      authorized = false;
    }
    return authorized;
  }

  private boolean checkInsecureSetup() {
    boolean kerberos = UserGroupInformation.isSecurityEnabled();
    boolean limitUsers = nmContext.getConf()
        .getBoolean(YarnConfiguration.NM_NONSECURE_MODE_LIMIT_USERS, true);
    if (kerberos) {
      return false;
    }
    return limitUsers;
  }
}
