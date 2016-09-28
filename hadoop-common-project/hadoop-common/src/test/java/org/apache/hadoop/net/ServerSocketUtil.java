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

package org.apache.hadoop.net;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ServerSocketUtil {

  private static final Log LOG = LogFactory.getLog(ServerSocketUtil.class);

  /**
   * Port scan & allocate is how most other apps find ports
   * 
   * @param port given port
   * @param retries number of retires
   * @return
   * @throws IOException
   */
  public static int getPort(int port, int retries) throws IOException {
    Random rand = new Random();
    int tryPort = port;
    int tries = 0;
    while (true) {
      if (tries > 0) {
        tryPort = port + rand.nextInt(65535 - port);
      }
      LOG.info("Using port " + tryPort);
      try (ServerSocket s = new ServerSocket(tryPort)) {
        return tryPort;
      } catch (IOException e) {
        tries++;
        if (tries >= retries) {
          LOG.info("Port is already in use; giving up");
          throw e;
        } else {
          LOG.info("Port is already in use; trying again");
        }
      }
    }
  }

  /**
   * Find the specified number of unique ports available.
   * The ports are all closed afterwards,
   * so other network services started may grab those same ports.
   *
   * @param numPorts number of required port nubmers
   * @return array of available port numbers
   * @throws IOException
   */
  public static int[] getPorts(int numPorts) throws IOException {
    ServerSocket[] sockets = new ServerSocket[numPorts];
    int[] ports = new int[numPorts];
    for (int i = 0; i < numPorts; i++) {
      ServerSocket sock = new ServerSocket(0);
      sockets[i] = sock;
      ports[i] = sock.getLocalPort();
    }
    for (ServerSocket sock : sockets) {
      sock.close();
    }
    return ports;
  }
}
