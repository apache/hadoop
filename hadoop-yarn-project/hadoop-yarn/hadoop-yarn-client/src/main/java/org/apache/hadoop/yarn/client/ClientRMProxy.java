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

package org.apache.hadoop.yarn.client;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;

public class ClientRMProxy<T> extends RMProxy<T>{

  private static final Log LOG = LogFactory.getLog(ClientRMProxy.class);

  public static <T> T createRMProxy(final Configuration conf,
      final Class<T> protocol) throws IOException {
    InetSocketAddress rmAddress = getRMAddress(conf, protocol);
    return createRMProxy(conf, protocol, rmAddress);
  }

  private static InetSocketAddress getRMAddress(Configuration conf, Class<?> protocol) {
    if (protocol == ApplicationClientProtocol.class) {
      return conf.getSocketAddr(YarnConfiguration.RM_ADDRESS,
          YarnConfiguration.DEFAULT_RM_ADDRESS,
          YarnConfiguration.DEFAULT_RM_PORT);
    } else if (protocol == ResourceManagerAdministrationProtocol.class) {
      return conf.getSocketAddr(
          YarnConfiguration.RM_ADMIN_ADDRESS,
          YarnConfiguration.DEFAULT_RM_ADMIN_ADDRESS,
          YarnConfiguration.DEFAULT_RM_ADMIN_PORT);
    } else if (protocol == ApplicationMasterProtocol.class) {
      return conf.getSocketAddr(
          YarnConfiguration.RM_SCHEDULER_ADDRESS,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);
    } else {
      String message = "Unsupported protocol found when creating the proxy " +
          "connection to ResourceManager: " +
          ((protocol != null) ? protocol.getClass().getName() : "null");
      LOG.error(message);
      throw new IllegalStateException(message);
    }
  }
}
