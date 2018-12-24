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
package org.apache.hadoop.hdfs.server.namenode.ha;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.NameNodeProxiesClient;
import org.apache.hadoop.ipc.AlignmentContext;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClientHAProxyFactory<T> implements HAProxyFactory<T> {

  private AlignmentContext alignmentContext;

  public void setAlignmentContext(AlignmentContext alignmentContext) {
    this.alignmentContext = alignmentContext;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T createProxy(Configuration conf, InetSocketAddress nnAddr,
      Class<T> xface, UserGroupInformation ugi, boolean withRetries,
      AtomicBoolean fallbackToSimpleAuth) throws IOException {
    if (alignmentContext != null) {
      return (T) NameNodeProxiesClient.createProxyWithAlignmentContext(
        nnAddr, conf, ugi, false, fallbackToSimpleAuth, alignmentContext);
    }
    return (T) NameNodeProxiesClient.createNonHAProxyWithClientProtocol(
      nnAddr, conf, ugi, false, fallbackToSimpleAuth);
  }

  @Override
  public T createProxy(Configuration conf, InetSocketAddress nnAddr,
      Class<T> xface, UserGroupInformation ugi, boolean withRetries)
      throws IOException {
    return createProxy(conf, nnAddr, xface, ugi, withRetries, null);
  }
}
