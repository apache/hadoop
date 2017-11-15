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

package org.apache.hadoop.yarn.service.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.client.ServerProxy;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.service.conf.YarnServiceConf;

import java.net.InetSocketAddress;

import static org.apache.hadoop.io.retry.RetryPolicies.TRY_ONCE_THEN_FAIL;

public class ClientAMProxy extends ServerProxy{

  public static <T> T createProxy(final Configuration conf,
      final Class<T> protocol, final UserGroupInformation ugi,
      final YarnRPC rpc, final InetSocketAddress serverAddress) {
    Configuration confClone = new Configuration(conf);
    confClone.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 0);
    confClone.setInt(CommonConfigurationKeysPublic.
        IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY, 0);
    RetryPolicy retryPolicy;

    if (conf.getLong(YarnServiceConf.CLIENT_AM_RETRY_MAX_WAIT_MS, 0) == 0) {
      // by default no retry
      retryPolicy = TRY_ONCE_THEN_FAIL;
    } else {
      retryPolicy =
          createRetryPolicy(conf, YarnServiceConf.CLIENT_AM_RETRY_MAX_WAIT_MS,
              15 * 60 * 1000, YarnServiceConf.CLIENT_AM_RETRY_MAX_INTERVAL_MS,
              2 * 1000);
    }
    return createRetriableProxy(confClone, protocol, ugi, rpc, serverAddress,
        retryPolicy);
  }
}
