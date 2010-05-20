/**
 * Copyright 2009 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.jruby.embed.ScriptingContainer;
import org.jruby.embed.PathType;

/**
 *
 * @author scoundrel
 */
public class TestShell {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static ScriptingContainer jruby = new ScriptingContainer();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Start mini cluster
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt("hbase.client.retries.number", 6);
    TEST_UTIL.startMiniCluster();

    // Configure jruby runtime
    List<String> loadPaths = new ArrayList();
    loadPaths.add("src/main/ruby");
    loadPaths.add("src/test/ruby");
    jruby.getProvider().setLoadPaths(loadPaths);
    jruby.put("$TEST_CLUSTER", TEST_UTIL);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRunShellTests() throws IOException {
    // Start all ruby tests
    jruby.runScriptlet(PathType.ABSOLUTE, "src/test/ruby/tests_runner.rb");
  }
}
