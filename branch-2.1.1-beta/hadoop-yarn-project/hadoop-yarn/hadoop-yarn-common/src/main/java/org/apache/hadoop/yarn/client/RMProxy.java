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
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.ipc.YarnRPC;

import com.google.common.annotations.VisibleForTesting;

@InterfaceAudience.Public
@InterfaceStability.Evolving
@SuppressWarnings("unchecked")
public class RMProxy<T> {

  private static final Log LOG = LogFactory.getLog(RMProxy.class);

  public static <T> T createRMProxy(final Configuration conf,
      final Class<T> protocol, InetSocketAddress rmAddress) throws IOException {
    RetryPolicy retryPolicy = createRetryPolicy(conf);
    T proxy = RMProxy.<T>getProxy(conf, protocol, rmAddress);
    LOG.info("Connecting to ResourceManager at " + rmAddress);
    return (T) RetryProxy.create(protocol, proxy, retryPolicy);
  }

  private static <T> T getProxy(final Configuration conf,
      final Class<T> protocol, final InetSocketAddress rmAddress)
      throws IOException {
    return UserGroupInformation.getCurrentUser().doAs(
      new PrivilegedAction<T>() {

        @Override
        public T run() {
          return (T) YarnRPC.create(conf).getProxy(protocol, rmAddress, conf);
        }
      });
  }

  @Private
  @VisibleForTesting
  public static RetryPolicy createRetryPolicy(Configuration conf) {
    long rmConnectWaitMS =
        conf.getInt(
            YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS,
            YarnConfiguration.DEFAULT_RESOURCEMANAGER_CONNECT_MAX_WAIT_MS);
    long rmConnectionRetryIntervalMS =
        conf.getLong(
            YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS,
            YarnConfiguration
            .DEFAULT_RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS);

    if (rmConnectionRetryIntervalMS < 0) {
      throw new YarnRuntimeException("Invalid Configuration. " +
          YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS +
          " should not be negative.");
    }

    boolean waitForEver = (rmConnectWaitMS == -1);

    if (waitForEver) {
      return  RetryPolicies.RETRY_FOREVER;
    } else {
      if (rmConnectWaitMS < 0) {
        throw new YarnRuntimeException("Invalid Configuration. "
            + YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS
            + " can be -1, but can not be other negative numbers");
      }

      // try connect once
      if (rmConnectWaitMS < rmConnectionRetryIntervalMS) {
        LOG.warn(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS
            + " is smaller than "
            + YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS
            + ". Only try connect once.");
        rmConnectWaitMS = 0;
      }
    }

    RetryPolicy retryPolicy =
        RetryPolicies.retryUpToMaximumTimeWithFixedSleep(rmConnectWaitMS,
            rmConnectionRetryIntervalMS,
            TimeUnit.MILLISECONDS);

    Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap =
        new HashMap<Class<? extends Exception>, RetryPolicy>();
    exceptionToPolicyMap.put(ConnectException.class, retryPolicy);
    //TO DO: after HADOOP-9576,  IOException can be changed to EOFException
    exceptionToPolicyMap.put(IOException.class, retryPolicy);

    return RetryPolicies.retryByException(RetryPolicies.TRY_ONCE_THEN_FAIL,
      exceptionToPolicyMap);
  }
}
