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
package org.apache.hadoop.yarn.applications.tensorflow.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.yarn.applications.tensorflow.api.protocolrecords.GetClusterSpecRequest;
import org.apache.hadoop.yarn.applications.tensorflow.api.protocolrecords.GetClusterSpecResponse;
import org.apache.hadoop.yarn.client.RMProxy;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;

import java.io.IOException;
import java.net.InetSocketAddress;

public class TFClient {
  private RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  private TensorflowCluster tensorflow;

  public TFClient(Configuration conf, InetSocketAddress server) throws IOException {
    RetryPolicy retryPolicy = RMProxy.createRetryPolicy(conf, false);
    TensorflowCluster proxy = RMProxy.createRMProxy(conf, TensorflowCluster.class, server);
    this.tensorflow = (TensorflowCluster) RetryProxy.create(
      TensorflowCluster.class, proxy, retryPolicy);
  }

  public String getClusterSpec() throws IOException, YarnException {
    GetClusterSpecResponse response =
      this.tensorflow.getClusterSpec(recordFactory.newRecordInstance(GetClusterSpecRequest.class));
    return response.getClusterSpec();
  }

  public static void main(String[] args) throws IOException, YarnException {
    InetSocketAddress address = new InetSocketAddress("localhost", 9001);
    TFClient client = new TFClient(new Configuration(), address);
    System.out.println(client.getClusterSpec());
  }
}
