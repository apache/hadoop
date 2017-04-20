/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.slider.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.impl.zk.RegistryOperationsService;
import org.apache.hadoop.registry.server.services.MicroZookeeperService;
import org.apache.slider.common.tools.SliderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * Test ZK cluster.
 */
public class MicroZKCluster implements Closeable {
  private static final Logger LOG =
      LoggerFactory.getLogger(MicroZKCluster.class);

  public static final String HOSTS = "127.0.0.1";
  private MicroZookeeperService zkService;
  private String zkBindingString;
  private final Configuration conf;
  private RegistryOperations registryOperations;

  MicroZKCluster() {
    this(SliderUtils.createConfiguration());
  }

  MicroZKCluster(Configuration conf) {
    this.conf = conf;
  }

  String getZkBindingString() {
    return zkBindingString;
  }

  void createCluster(String name) {
    zkService = new MicroZookeeperService(name);

    zkService.init(conf);
    zkService.start();
    zkBindingString = zkService.getConnectionString();
    LOG.info("Created {}", this);
    registryOperations = new RegistryOperationsService(
        "registry",
        zkService);
    registryOperations.init(conf);
    registryOperations.start();
  }

  @Override
  public void close() throws IOException {
    if (registryOperations != null) {
      registryOperations.stop();
    }
    if (zkService != null) {
      zkService.stop();
    }
  }

  @Override
  public String toString() {
    return "Micro ZK cluster as " + zkBindingString;
  }


}
