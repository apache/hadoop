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

import java.net.InetSocketAddress;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;

@Public
@Unstable
public class NMProxy extends ServerProxy {

  public static <T> T createNMProxy(final Configuration conf,
      final Class<T> protocol, final UserGroupInformation ugi,
      final YarnRPC rpc, final InetSocketAddress serverAddress) {

    RetryPolicy retryPolicy =
        createRetryPolicy(conf,
          YarnConfiguration.CLIENT_NM_CONNECT_MAX_WAIT_MS,
          YarnConfiguration.DEFAULT_CLIENT_NM_CONNECT_MAX_WAIT_MS,
          YarnConfiguration.CLIENT_NM_CONNECT_RETRY_INTERVAL_MS,
          YarnConfiguration.DEFAULT_CLIENT_NM_CONNECT_RETRY_INTERVAL_MS);
    Configuration confClone = new Configuration(conf);
    confClone.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 0);
    confClone.setInt(CommonConfigurationKeysPublic.
            IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY, 0);
    return createRetriableProxy(confClone, protocol, ugi, rpc, serverAddress,
      retryPolicy);
  }
}