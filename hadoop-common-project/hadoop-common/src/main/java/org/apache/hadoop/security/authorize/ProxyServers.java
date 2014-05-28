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

package org.apache.hadoop.security.authorize;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;

public class ProxyServers {
  public static final String CONF_HADOOP_PROXYSERVERS = "hadoop.proxyservers";
  private static volatile Collection<String> proxyServers;

  public static void refresh() {
    refresh(new Configuration());
  }

  public static void refresh(Configuration conf){
    Collection<String> tempServers = new HashSet<String>();
    // trusted proxy servers such as http proxies
    for (String host : conf.getTrimmedStrings(CONF_HADOOP_PROXYSERVERS)) {
      InetSocketAddress addr = new InetSocketAddress(host, 0);
      if (!addr.isUnresolved()) {
        tempServers.add(addr.getAddress().getHostAddress());
      }
    }
    proxyServers = tempServers;
  }

  public static boolean isProxyServer(String remoteAddr) { 
    if (proxyServers == null) {
      refresh(); 
    }
    return proxyServers.contains(remoteAddr);
  }
}
