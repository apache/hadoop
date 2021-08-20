/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.common.blockaliasmap.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.aliasmap.InMemoryAliasMapProtocol;
import org.apache.hadoop.hdfs.server.aliasmap.InMemoryLevelDBAliasMapServer;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;

import static org.apache.hadoop.hdfs.NameNodeProxies.createNNProxyWithInMemoryAliasMapProtocol;

/**
 * A client used by the Namenode to connect to the
 * {@link InMemoryLevelDBAliasMapServer} running local to it.
 */
public class NamenodeInMemoryAliasMapClient
    extends InMemoryLevelDBAliasMapClient {

  NamenodeInMemoryAliasMapClient() {
    super();
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    InetSocketAddress address =
        InMemoryLevelDBAliasMapServer.getServerAddress(conf);
    aliasMaps = new ArrayList<>();
    try {
      InMemoryAliasMapProtocol nnProxy =
          createNNProxyWithInMemoryAliasMapProtocol(address, conf,
              UserGroupInformation.getCurrentUser(), null);
      if (nnProxy != null) {
        aliasMaps.add(nnProxy);
      } else {
        throw new IllegalStateException("Unable to connect to the " +
            "InMemoryLevelDBAliasMapServer at " + address);
      }
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
