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

package org.apache.hadoop.yarn.client.api;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.eclipse.jetty.websocket.api.Callback;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketOpen;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.impl.LineReaderImpl;
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

  private Session mySession;
  private Terminal terminal;
  private LineReader reader;
  private boolean sttySet = false;

  @OnWebSocketMessage
  public void onText(Session session, String message) throws IOException {
    if (!sttySet) {
      session.sendText("stty -echo", Callback.NOOP);
      session.sendText("\r", Callback.NOOP);
      sttySet = true;
    }
    terminal.output().write(message.getBytes(StandardCharsets.UTF_8));
    terminal.output().flush();
  }

  @OnWebSocketOpen
  public void onConnect(Session s) {
    initTerminal(s);
    LOG.info("{} connected!", s.getRemoteSocketAddress());
  }

  @OnWebSocketClose
  public void onClose(Session session, int status, String reason) {
    if (status==1000) {
      LOG.info("{} closed, status: {}", session.getRemoteSocketAddress(), status);
    } else {
      LOG.warn("{} closed, status:" +
              " {} Reason: {}.", session.getRemoteSocketAddress(), status, reason);
    }
  }

  public void run() {
    try {
      Reader consoleReader = new Reader();
      Thread inputThread = new Thread(consoleReader, "consoleReader");
      inputThread.start();
      while (mySession.isOpen()) {
        if (consoleReader.hasData()) {
          String message = consoleReader.read();
          mySession.sendText(message, Callback.NOOP);
          mySession.sendText("\r", Callback.NOOP);
        }
        String message = "1{}";
        mySession.sendText(message, Callback.NOOP);
        Thread.sleep(100);
      }
      inputThread.join();
    } catch (InterruptedException e) {
      mySession.disconnect();
    }
  }

  protected void initTerminal(final Session session) {
    try {
      this.mySession = session;
      try {
        terminal = TerminalBuilder.builder()
            .system(true)
            .build();
      } catch (IOException t) {
        terminal = TerminalBuilder.builder()
            .system(false)
            .streams(System.in, (OutputStream) System.out)
            .build();
      }
      reader = LineReaderBuilder.builder()
          .terminal(terminal)
          .build();
    } catch (IOException e) {
      session.close(1002, e.getMessage(), Callback.NOOP);
    }
  }

  class Reader implements Runnable {
    private StringBuilder sb = new StringBuilder();
    private boolean hasData = false;

    public String read() {
      try {
        return sb.toString();
      } finally {
        hasData = false;
        sb.setLength(0);
      }
    }

    public boolean hasData() {
      return hasData;
    }

    @Override
    public void run() {
      while (true) {
        int c = ((LineReaderImpl) reader).readCharacter();
        if (c == 10 || c == 13) {
          hasData = true;
          continue;
        }
        sb.append(new String(Character.toChars(c)));
      }
    }
  }
}
