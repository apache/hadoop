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

package org.apache.hadoop.ipc;

import java.lang.reflect.Method;
import java.io.IOException;
import java.net.InetSocketAddress;
import javax.net.SocketFactory;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.conf.Configuration;

/** An RPC implementation. */
interface RpcEngine {

  /** Construct a client-side proxy object. */
  Object getProxy(Class protocol,
                  long clientVersion, InetSocketAddress addr,
                  UserGroupInformation ticket, Configuration conf,
                  SocketFactory factory) throws IOException;

  /** Stop this proxy. */
  void stopProxy(Object proxy);

  /** Expert: Make multiple, parallel calls to a set of servers. */
  Object[] call(Method method, Object[][] params, InetSocketAddress[] addrs,
                UserGroupInformation ticket, Configuration conf)
    throws IOException, InterruptedException;

  /** Construct a server for a protocol implementation instance. */
  RPC.Server getServer(Class protocol, Object instance, String bindAddress,
                       int port, int numHandlers, boolean verbose,
                       Configuration conf, 
                       SecretManager<? extends TokenIdentifier> secretManager
                       ) throws IOException;

}
