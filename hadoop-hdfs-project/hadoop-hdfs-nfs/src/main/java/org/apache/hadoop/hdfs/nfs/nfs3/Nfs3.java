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
package org.apache.hadoop.hdfs.nfs.nfs3;

import java.io.IOException;
import java.net.DatagramSocket;

import org.apache.hadoop.hdfs.nfs.conf.NfsConfigKeys;
import org.apache.hadoop.hdfs.nfs.conf.NfsConfiguration;
import org.apache.hadoop.hdfs.nfs.mount.Mountd;
import org.apache.hadoop.nfs.nfs3.Nfs3Base;
import org.apache.hadoop.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * Nfs server. Supports NFS v3 using {@link RpcProgramNfs3}.
 * Currently Mountd program is also started inside this class.
 * Only TCP server is supported and UDP is not supported.
 */
public class Nfs3 extends Nfs3Base {
  private Mountd mountd;
  
  public Nfs3(NfsConfiguration conf) throws IOException {
    this(conf, null, true);
  }
  
  public Nfs3(NfsConfiguration conf, DatagramSocket registrationSocket,
      boolean allowInsecurePorts) throws IOException {
    super(new RpcProgramNfs3(conf, registrationSocket, allowInsecurePorts), conf);
    mountd = new Mountd(conf, registrationSocket, allowInsecurePorts);
  }

  public Mountd getMountd() {
    return mountd;
  }
  
  @VisibleForTesting
  public void startServiceInternal(boolean register) throws IOException {
    mountd.start(register); // Start mountd
    start(register);
  }
  
  static void startService(String[] args,
      DatagramSocket registrationSocket) throws IOException {
    StringUtils.startupShutdownMessage(Nfs3.class, args, LOG);
    NfsConfiguration conf = new NfsConfiguration();
    boolean allowInsecurePorts = conf.getBoolean(
        NfsConfigKeys.DFS_NFS_PORT_MONITORING_DISABLED_KEY,
        NfsConfigKeys.DFS_NFS_PORT_MONITORING_DISABLED_DEFAULT);
    final Nfs3 nfsServer = new Nfs3(conf, registrationSocket,
        allowInsecurePorts);
    nfsServer.startServiceInternal(true);
  }
  
  public static void main(String[] args) throws IOException {
    startService(args, null);
  }
}
