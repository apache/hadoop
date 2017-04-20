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

package org.apache.slider.common.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.slider.utils.YarnMiniClusterTestBase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

/**
 * Test config helper loading configuration from HDFS.
 */
public class TestConfigHelperHDFS extends YarnMiniClusterTestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestConfigHelperHDFS.class);

  @Test
  public void testConfigHelperHDFS() throws Throwable {
    YarnConfiguration config = getConfiguration();
    createMiniHDFSCluster("testConfigHelperHDFS", config);

    Configuration conf = new Configuration(false);
    conf.set("key", "value");
    URI fsURI = new URI(getFsDefaultName());
    Path root = new Path(fsURI);
    Path confPath = new Path(root, "conf.xml");
    FileSystem dfs = FileSystem.get(fsURI, config);
    ConfigHelper.saveConfig(dfs, confPath, conf);
    //load time
    Configuration loaded = ConfigHelper.loadConfiguration(dfs, confPath);
    LOG.info(ConfigHelper.dumpConfigToString(loaded));
    assertEquals("value", loaded.get("key"));
  }

}
