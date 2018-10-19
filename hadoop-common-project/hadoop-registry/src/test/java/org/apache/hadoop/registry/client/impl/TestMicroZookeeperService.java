/*
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

package org.apache.hadoop.registry.client.impl;

import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.registry.conf.RegistryConfiguration;
import org.apache.hadoop.registry.server.services.MicroZookeeperService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;

import java.io.IOException;

/**
 * Simple tests to look at the micro ZK service itself
 */
public class TestMicroZookeeperService extends Assert {

  private MicroZookeeperService zookeeper;

  @Rule
  public final Timeout testTimeout = new Timeout(10000);
  @Rule
  public TestName methodName = new TestName();

  @After
  public void destroyZKServer() throws IOException {

    ServiceOperations.stop(zookeeper);
  }

  @Test
  public void testTempDirSupport() throws Throwable {
    Configuration conf = new RegistryConfiguration();
    zookeeper = new MicroZookeeperService("t1");
    zookeeper.init(conf);
    zookeeper.start();
    zookeeper.stop();
  }

}
