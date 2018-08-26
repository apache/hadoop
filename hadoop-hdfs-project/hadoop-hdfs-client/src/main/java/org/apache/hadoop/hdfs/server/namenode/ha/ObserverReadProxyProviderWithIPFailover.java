/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.ha;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;

/**
 * ObserverReadProxyProvider with IPFailoverProxyProvider
 * as the failover method.
 */
public class
ObserverReadProxyProviderWithIPFailover<T extends ClientProtocol>
extends ObserverReadProxyProvider<T> {

  public ObserverReadProxyProviderWithIPFailover(
      Configuration conf, URI uri, Class<T> xface,
      HAProxyFactory<T> factory) throws IOException {
    super(conf, uri, xface, factory,
        new IPFailoverProxyProvider<T>(conf, uri, xface,factory));
  }
}