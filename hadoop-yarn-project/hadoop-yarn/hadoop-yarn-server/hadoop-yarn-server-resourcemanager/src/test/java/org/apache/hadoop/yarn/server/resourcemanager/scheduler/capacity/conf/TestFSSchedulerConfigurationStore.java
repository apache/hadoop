/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.YarnConfigurationStore.LogMutation;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;


/**
 * Tests {@link FSSchedulerConfigurationStore}.
 */
public class TestFSSchedulerConfigurationStore {
  private static final String TEST_USER = "test";
  private FSSchedulerConfigurationStore configurationStore;
  private Configuration conf;
  private File testSchedulerConfigurationDir;

  @Before
  public void setUp() throws Exception {
    configurationStore = new FSSchedulerConfigurationStore();
    testSchedulerConfigurationDir = new File(
        TestFSSchedulerConfigurationStore.class.getResource("").getPath()
            + FSSchedulerConfigurationStore.class.getSimpleName());
    testSchedulerConfigurationDir.mkdirs();

    conf = new Configuration();
    conf.set(YarnConfiguration.SCHEDULER_CONFIGURATION_FS_PATH,
        testSchedulerConfigurationDir.getAbsolutePath());
  }

  private void writeConf(Configuration config) throws IOException {
    FileSystem fileSystem = FileSystem.get(new Configuration(config));
    String schedulerConfigurationFile = YarnConfiguration.CS_CONFIGURATION_FILE
        + "." + System.currentTimeMillis();
    FSDataOutputStream outputStream = fileSystem.create(
        new Path(testSchedulerConfigurationDir.getAbsolutePath(),
            schedulerConfigurationFile));
    config.writeXml(outputStream);
    outputStream.close();
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(testSchedulerConfigurationDir);
  }

  @Test
  public void confirmMutationWithValid() throws Exception {
    conf.setInt(
        YarnConfiguration.SCHEDULER_CONFIGURATION_FS_MAX_VERSION, 2);
    conf.set("a", "a");
    conf.set("b", "b");
    conf.set("c", "c");
    writeConf(conf);
    configurationStore.initialize(conf, conf, null);
    Configuration storeConf = configurationStore.retrieve();
    compareConfig(conf, storeConf);

    Configuration expectConfig = new Configuration(conf);
    expectConfig.unset("a");
    expectConfig.set("b", "bb");

    prepareParameterizedLogMutation(configurationStore, true,
        "a", null, "b", "bb");
    storeConf = configurationStore.retrieve();
    assertNull(storeConf.get("a"));
    assertEquals("bb", storeConf.get("b"));
    assertEquals("c", storeConf.get("c"));

    compareConfig(expectConfig, storeConf);

    prepareParameterizedLogMutation(configurationStore, true,
        "a", null, "b", "bbb");
    storeConf = configurationStore.retrieve();
    assertNull(storeConf.get("a"));
    assertEquals("bbb", storeConf.get("b"));
    assertEquals("c", storeConf.get("c"));
  }

  @Test
  public void confirmMutationWithInvalid() throws Exception {
    conf.set("a", "a");
    conf.set("b", "b");
    conf.set("c", "c");
    writeConf(conf);
    configurationStore.initialize(conf, conf, null);
    Configuration storeConf = configurationStore.retrieve();
    compareConfig(conf, storeConf);

    prepareParameterizedLogMutation(configurationStore, false,
        "a", null, "b", "bb");
    storeConf = configurationStore.retrieve();

    compareConfig(conf, storeConf);
  }

  @Test
  public void testFileSystemClose() throws Exception {
    MiniDFSCluster hdfsCluster = null;
    FileSystem fs;
    Path path = new Path("/tmp/confstore");
    try {
      HdfsConfiguration hdfsConfig = new HdfsConfiguration();
      hdfsCluster = new MiniDFSCluster.Builder(hdfsConfig)
          .numDataNodes(1).build();

      fs = hdfsCluster.getFileSystem();
      if (!fs.exists(path)) {
        fs.mkdirs(path);
      }

      FSSchedulerConfigurationStore configStore =
          new FSSchedulerConfigurationStore();
      hdfsConfig.set(YarnConfiguration.SCHEDULER_CONFIGURATION_FS_PATH,
          path.toString());
      configStore.initialize(hdfsConfig, hdfsConfig, null);

      // Close the FileSystem object and validate
      fs.close();

      try {
        prepareParameterizedLogMutation(configStore, true,
            "testkey", "testvalue");
      } catch (IOException e) {
        if (e.getMessage().contains("Filesystem closed")) {
          fail("FSSchedulerConfigurationStore failed to handle " +
              "FileSystem close");
        } else {
          fail("Should not get any exceptions");
        }
      }
    } finally {
      assert hdfsCluster != null;
      fs = hdfsCluster.getFileSystem();
      if (fs.exists(path)) {
        fs.delete(path, true);
      }
      hdfsCluster.shutdown();
    }
  }

  @Test
  public void testFormatConfiguration() throws Exception {
    Configuration schedulerConf = new Configuration();
    schedulerConf.set("a", "a");
    writeConf(schedulerConf);
    configurationStore.initialize(conf, conf, null);
    Configuration storedConfig = configurationStore.retrieve();
    assertEquals("a", storedConfig.get("a"));
    configurationStore.format();
    try {
      configurationStore.retrieve();
      fail("Expected an IOException with message containing \"no capacity " +
          "scheduler file in\" to be thrown");
    } catch (IOException e) {
      assertThat(e.getMessage(),
          CoreMatchers.containsString("no capacity scheduler file in"));
    }
  }

  @Test
  public void retrieve() throws Exception {
    Configuration schedulerConf = new Configuration();
    schedulerConf.set("a", "a");
    schedulerConf.setLong("long", 1L);
    schedulerConf.setBoolean("boolean", true);
    writeConf(schedulerConf);

    configurationStore.initialize(conf, conf, null);
    Configuration storedConfig = configurationStore.retrieve();

    compareConfig(schedulerConf, storedConfig);
  }

  @Test
  public void checkVersion() {
    try {
      configurationStore.checkVersion();
    } catch (Exception e) {
      fail("checkVersion throw exception");
    }
  }

  private void compareConfig(Configuration schedulerConf,
      Configuration storedConfig) {
    for (Map.Entry<String, String> entry : schedulerConf) {
      assertEquals(entry.getKey(), schedulerConf.get(entry.getKey()),
          storedConfig.get(entry.getKey()));
    }

    for (Map.Entry<String, String> entry : storedConfig) {
      assertEquals(entry.getKey(), storedConfig.get(entry.getKey()),
          schedulerConf.get(entry.getKey()));
    }
  }

  private void prepareParameterizedLogMutation(
      FSSchedulerConfigurationStore configStore,
      boolean validityFlag, String... values) throws Exception {
    Map<String, String> updates = new HashMap<>();
    String key;
    String value;

    if (values.length % 2 != 0) {
      throw new IllegalArgumentException("The number of parameters should be " +
          "even.");
    }

    for (int i = 1; i <= values.length; i += 2) {
      key = values[i - 1];
      value = values[i];
      updates.put(key, value);
    }

    LogMutation logMutation = new LogMutation(updates, TEST_USER);
    configStore.logMutation(logMutation);
    configStore.confirmMutation(logMutation, validityFlag);
  }
}