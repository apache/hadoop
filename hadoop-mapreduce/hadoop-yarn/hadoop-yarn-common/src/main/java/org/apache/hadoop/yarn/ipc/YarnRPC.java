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

import java.net.InetSocketAddress;

import org.apache.avro.ipc.Server;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.YarnException;

/**
 * Abstraction to get the RPC implementation for Yarn.
 */
public abstract class YarnRPC {
  private static final Log LOG = LogFactory.getLog(YarnRPC.class);
  
  public static final String RPC_CLASSNAME 
      = "org.apache.hadoop.yarn.ipc.YarnRPC.classname";

  //use the default as Hadoop RPC
  public static final String DEFAULT_RPC 
      = "org.apache.hadoop.yarn.ipc.HadoopYarnProtoRPC";

  public abstract Object getProxy(Class protocol, InetSocketAddress addr,
      Configuration conf);

  public abstract Server getServer(Class protocol, Object instance,
      InetSocketAddress addr, Configuration conf,
      SecretManager<? extends TokenIdentifier> secretManager,
      int numHandlers);

  public static YarnRPC create(Configuration conf) {
    LOG.info("Creating YarnRPC for " + conf.get(RPC_CLASSNAME));
    String clazzName = conf.get(RPC_CLASSNAME);
    if (clazzName == null) {
      clazzName = DEFAULT_RPC;
    }
    try {
      return (YarnRPC) Class.forName(clazzName).newInstance();
    } catch (Exception e) {
      throw new YarnException(e);
    }
  }

}
