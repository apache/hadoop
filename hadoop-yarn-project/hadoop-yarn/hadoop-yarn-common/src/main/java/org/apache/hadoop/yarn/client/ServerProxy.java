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

import java.io.EOFException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.NoRouteToHostException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.net.ConnectTimeoutException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.exceptions.NMNotYetReadyException;
import org.apache.hadoop.yarn.ipc.YarnRPC;

import com.google.common.base.Preconditions;

@Public
@Unstable
public class ServerProxy {

  protected static RetryPolicy createRetryPolicy(Configuration conf,
      String maxWaitTimeStr, long defMaxWaitTime,
      String connectRetryIntervalStr, long defRetryInterval) {
    long maxWaitTime = conf.getLong(maxWaitTimeStr, defMaxWaitTime);
    long retryIntervalMS =
        conf.getLong(connectRetryIntervalStr, defRetryInterval);
    if (maxWaitTime == -1) {
      // wait forever.
      return RetryPolicies.RETRY_FOREVER;
    }

    Preconditions.checkArgument(maxWaitTime > 0, "Invalid Configuration. "
        + maxWaitTimeStr + " should be a positive value.");
    Preconditions.checkArgument(retryIntervalMS > 0, "Invalid Configuration. "
        + connectRetryIntervalStr + "should be a positive value.");

    RetryPolicy retryPolicy =
        RetryPolicies.retryUpToMaximumTimeWithFixedSleep(maxWaitTime,
          retryIntervalMS, TimeUnit.MILLISECONDS);

    Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap =
        new HashMap<Class<? extends Exception>, RetryPolicy>();
    exceptionToPolicyMap.put(EOFException.class, retryPolicy);
    exceptionToPolicyMap.put(ConnectException.class, retryPolicy);
    exceptionToPolicyMap.put(NoRouteToHostException.class, retryPolicy);
    exceptionToPolicyMap.put(UnknownHostException.class, retryPolicy);
    exceptionToPolicyMap.put(RetriableException.class, retryPolicy);
    exceptionToPolicyMap.put(SocketException.class, retryPolicy);
    exceptionToPolicyMap.put(NMNotYetReadyException.class, retryPolicy);

    return RetryPolicies.retryByException(RetryPolicies.TRY_ONCE_THEN_FAIL,
      exceptionToPolicyMap);
  }

  @SuppressWarnings("unchecked")
  protected static <T> T createRetriableProxy(final Configuration conf,
      final Class<T> protocol, final UserGroupInformation user,
      final YarnRPC rpc, final InetSocketAddress serverAddress,
      RetryPolicy retryPolicy) {
    T proxy = user.doAs(new PrivilegedAction<T>() {
      @Override
      public T run() {
        return (T) rpc.getProxy(protocol, serverAddress, conf);
      }
    });
    return (T) RetryProxy.create(protocol, proxy, retryPolicy);
  }
}