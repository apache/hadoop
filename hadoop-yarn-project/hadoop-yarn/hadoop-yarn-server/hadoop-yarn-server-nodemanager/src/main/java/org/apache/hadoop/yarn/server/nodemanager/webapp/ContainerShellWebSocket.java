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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerExecContext;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
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

  private final ContainerExecutor exec = new LinuxContainerExecutor();

  private IOStreamPair pair;

  @OnWebSocketMessage
  public void onText(Session session, String message) throws IOException {
    LOG.info("Message received: " + message);

    try {
      byte[] buffer = new byte[4000];
      if (session.isOpen()) {
        int ni = message.length();
        if (ni > 0) {
          pair.out.write(message.getBytes(Charset.forName("UTF-8")));
          pair.out.flush();
        }
        int no = pair.in.available();
        pair.in.read(buffer, 0, Math.min(no, buffer.length));
        String formatted = new String(buffer, Charset.forName("UTF-8"))
            .replaceAll("\n", "\r\n");
        session.getRemote().sendString(formatted);
      }
    } catch (Exception e) {
      LOG.error("Failed to parse WebSocket message from Client", e);
    }

  }

  @OnWebSocketConnect
  public void onConnect(Session session) {
    LOG.info(session.getRemoteAddress().getHostString() + " connected!");

    try {
      URI containerURI = session.getUpgradeRequest().getRequestURI();
      String[] containerPath = containerURI.getPath().split("/");
      String cId = containerPath[2];
      LOG.info(
          "Making interactive connection to running docker container with ID: "
              + cId);
      ContainerExecContext execContext = new ContainerExecContext
          .Builder()
          .setContainer(cId)
          .build();
      pair = exec.execContainer(execContext);
    } catch (Exception e) {
      LOG.error("Failed to establish WebSocket connection with Client", e);
    }

  }

  @OnWebSocketClose
  public void onClose(Session session, int status, String reason) {
    LOG.info(session.getRemoteAddress().getHostString() + " closed!");
  }

}