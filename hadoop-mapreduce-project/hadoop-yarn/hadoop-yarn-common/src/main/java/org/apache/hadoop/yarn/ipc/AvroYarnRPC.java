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

package org.apache.hadoop.yarn.ipc;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.SocketServer;
import org.apache.avro.ipc.SocketTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.YarnException;

/*
 * This uses Avro's simple Socket based RPC. Can be replaced with Netty based
 * when Yarn is upgraded to Avro 1.4.
 */
public class AvroYarnRPC extends YarnRPC {

  @Override
  public Object getProxy(Class protocol,
      InetSocketAddress addr, Configuration conf) {
    try {
      return SpecificRequestor.getClient(protocol, new SocketTransceiver(addr));
    } catch (IOException e) {
      throw new YarnException(e);
    }
  }

  @Override
  public Server getServer(Class protocol, Object instance,
      InetSocketAddress addr, Configuration conf,
      SecretManager<? extends TokenIdentifier> secretManager,
      int numHandlers) {
    try {
      return new SocketServer(new SpecificResponder(protocol, instance),
            addr);
    } catch (IOException e) {
      throw new YarnException(e);
    }
  }

}
