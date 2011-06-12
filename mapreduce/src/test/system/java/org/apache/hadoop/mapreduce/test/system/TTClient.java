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

package org.apache.hadoop.mapreduce.test.system;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.TaskTrackerStatus;
import org.apache.hadoop.mapreduce.server.tasktracker.TTConfig;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.test.system.process.RemoteProcess;

/**
 * TaskTracker client for system tests. Assumption of the class is that the
 * configuration key is set for the configuration key : {@code
 * TTConfig.TT_REPORT_ADDRESS}is set, only the port portion of the
 * address is used.
 */
public class TTClient extends MRDaemonClient<TTProtocol> {

  TTProtocol proxy;
  private static final String SYSTEM_TEST_FILE = "system-test.xml";

  public TTClient(Configuration conf, RemoteProcess daemon) 
      throws IOException {
    super(conf, daemon);
  }

  @Override
  public synchronized void connect() throws IOException {
    if (isConnected()) {
      return;
    }
    String sockAddrStr = getConf().get(TTConfig.TT_REPORT_ADDRESS);
    if (sockAddrStr == null) {
      throw new IllegalArgumentException(
          "TaskTracker report address is not set");
    }
    String[] splits = sockAddrStr.split(":");
    if (splits.length != 2) {
      throw new IllegalArgumentException(TTConfig.TT_REPORT_ADDRESS
        + " is not correctly configured or "
        + SYSTEM_TEST_FILE + " hasn't been found.");
    }
    String port = splits[1];
    String sockAddr = getHostName() + ":" + port;
    InetSocketAddress bindAddr = NetUtils.createSocketAddr(sockAddr);
    proxy = (TTProtocol) RPC.getProxy(TTProtocol.class, TTProtocol.versionID,
        bindAddr, getConf());
    setConnected(true);
  }

  @Override
  public synchronized void disconnect() throws IOException {
    RPC.stopProxy(proxy);
  }

  @Override
  public synchronized TTProtocol getProxy() {
    return proxy;
  }

  /**
   * Gets the last sent status to the {@link JobTracker}. <br/>
   * 
   * @return the task tracker status.
   * @throws IOException
   */
  public TaskTrackerStatus getStatus() throws IOException {
    return getProxy().getStatus();
  }

}
