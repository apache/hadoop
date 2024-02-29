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

import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;

/**
 * A {@link org.apache.hadoop.io.retry.FailoverProxyProvider} implementation
 * to support automatic msync-ing when using routers.
 * <p>
 * This constructs a wrapper proxy of ConfiguredFailoverProxyProvider,
 * and allows to configure logical names for nameservices.
 */
public class RouterObserverReadConfiguredFailoverProxyProvider<T>
    extends RouterObserverReadProxyProvider<T> {

  @VisibleForTesting
  static final Logger LOG =
      LoggerFactory.getLogger(RouterObserverReadConfiguredFailoverProxyProvider.class);

  public RouterObserverReadConfiguredFailoverProxyProvider(Configuration conf, URI uri,
      Class<T> xface, HAProxyFactory<T> factory) {
    super(conf, uri, xface, factory,
        new ConfiguredFailoverProxyProvider<>(conf, uri, xface, factory));
  }
}
