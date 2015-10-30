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
package org.apache.hadoop.tracing;

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.util.LinkedList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tracing.SpanReceiverInfo.ConfigurationPair;
import org.apache.htrace.core.HTraceConfiguration;
import org.junit.Test;

public class TestTraceUtils {
  private static String TEST_PREFIX = "test.prefix.htrace.";

  @Test
  public void testWrappedHadoopConf() {
    String key = "sampler";
    String value = "ProbabilitySampler";
    Configuration conf = new Configuration();
    conf.set(TEST_PREFIX + key, value);
    HTraceConfiguration wrapped = TraceUtils.wrapHadoopConf(TEST_PREFIX, conf);
    assertEquals(value, wrapped.get(key));
  }

  @Test
  public void testExtraConfig() {
    String key = "test.extra.config";
    String oldValue = "old value";
    String newValue = "new value";
    Configuration conf = new Configuration();
    conf.set(TEST_PREFIX + key, oldValue);
    LinkedList<ConfigurationPair> extraConfig =
        new LinkedList<ConfigurationPair>();
    extraConfig.add(new ConfigurationPair(TEST_PREFIX + key, newValue));
    HTraceConfiguration wrapped = TraceUtils.wrapHadoopConf(TEST_PREFIX, conf, extraConfig);
    assertEquals(newValue, wrapped.get(key));
  }

  /**
   * Test tracing the globber.  This is a regression test for HDFS-9187.
   */
  @Test
  public void testTracingGlobber() throws Exception {
    // Bypass the normal FileSystem object creation path by just creating an
    // instance of a subclass.
    FileSystem fs = new LocalFileSystem();
    fs.initialize(new URI("file:///"), new Configuration());
    fs.globStatus(new Path("/"));
    fs.close();
  }
}
