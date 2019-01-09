/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ENABLED;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ServicePlugin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class for {@link HddsDatanodeService}.
 */
public class TestHddsDatanodeService {
  private File testDir;
  private Configuration conf;
  private HddsDatanodeService service;
  private String[] args = new String[] {};

  @Before
  public void setUp() {
    testDir = GenericTestUtils.getRandomizedTestDir();
    conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_ENABLED, true);
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getPath());
    conf.setClass(OzoneConfigKeys.HDDS_DATANODE_PLUGINS_KEY, MockService.class,
        ServicePlugin.class);

    String volumeDir = testDir + "/disk1";
    conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, volumeDir);
  }

  @After
  public void tearDown() {
    FileUtil.fullyDelete(testDir);
  }

  @Test
  public void testStartup() throws IOException {
    service = HddsDatanodeService.createHddsDatanodeService(args, conf);
    service.start(null);
    service.join();

    assertNotNull(service.getDatanodeDetails());
    assertNotNull(service.getDatanodeDetails().getHostName());
    assertFalse(service.getDatanodeStateMachine().isDaemonStopped());

    service.stop();
    service.close();
  }

  static class MockService implements ServicePlugin {

    @Override
    public void close() throws IOException {
      // Do nothing
    }

    @Override
    public void start(Object arg0) {
      // Do nothing
    }

    @Override
    public void stop() {
      // Do nothing
    }
  }
}
