/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.service.monitor.probe;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstance;
import org.apache.hadoop.yarn.service.utils.ServiceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;

/**
 * Probe for a port being open.
 */
public class PortProbe extends Probe {
  protected static final Logger log = LoggerFactory.getLogger(PortProbe.class);
  private final int port;
  private final int timeout;

  public PortProbe(int port, int timeout) {
    super("Port probe of " + port + " for " + timeout + "ms", null);
    this.port = port;
    this.timeout = timeout;
  }

  public static PortProbe create(Map<String, String> props)
      throws IOException {
    int port = getPropertyInt(props, PORT_PROBE_PORT, null);

    if (port >= 65536) {
      throw new IOException(PORT_PROBE_PORT + " " + port + " is out of " +
          "range");
    }

    int timeout = getPropertyInt(props, PORT_PROBE_CONNECT_TIMEOUT,
        PORT_PROBE_CONNECT_TIMEOUT_DEFAULT);

    return new PortProbe(port, timeout);
  }

  /**
   * Try to connect to the (host,port); a failure to connect within
   * the specified timeout is a failure.
   * @param instance role instance
   * @return the outcome
   */
  @Override
  public ProbeStatus ping(ComponentInstance instance) {
    ProbeStatus status = new ProbeStatus();

    if (instance.getContainerStatus() == null || ServiceUtils
        .isEmpty(instance.getContainerStatus().getIPs())) {
      status.fail(this, new IOException(
          instance.getCompInstanceName() + ": IP is not available yet"));
      return status;
    }

    String ip = instance.getContainerStatus().getIPs().get(0);
    InetSocketAddress sockAddr = new InetSocketAddress(ip, port);
    Socket socket = new Socket();
    try {
      if (log.isDebugEnabled()) {
        log.debug(instance.getCompInstanceName() + ": Connecting " + sockAddr
            .toString() + ", timeout=" + MonitorUtils
            .millisToHumanTime(timeout));
      }
      socket.connect(sockAddr, timeout);
      status.succeed(this);
    } catch (Throwable e) {
      String error =
          instance.getCompInstanceName() + ": Probe " + sockAddr + " failed";
      log.debug(error, e);
      status.fail(this, new IOException(error, e));
    } finally {
      IOUtils.closeSocket(socket);
    }
    return status;
  }
}
