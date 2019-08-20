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

package org.apache.hadoop.ozone.web;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ServicePlugin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test class for {@link OzoneHddsDatanodeService}.
 */
public class TestOzoneHddsDatanodeService {
  private File testDir;
  private OzoneConfiguration conf;

  @Before
  public void setUp() {
    testDir = GenericTestUtils.getRandomizedTestDir();
    String volumeDir = testDir + "/disk1";

    conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.OZONE_SCM_NAMES, "localhost");
    conf.setBoolean(OzoneConfigKeys.OZONE_ENABLED, true);
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getPath());
    conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, volumeDir);
  }

  @After
  public void tearDown() {
    FileUtil.fullyDelete(testDir);
  }

  @Test
  public void testStartup() throws IOException {
    OzoneHddsDatanodeService ozoneHddsService = new OzoneHddsDatanodeService();
    String[] args = new String[] {};
    HddsDatanodeService hddsServiceSpy =
        Mockito.spy(HddsDatanodeService.createHddsDatanodeService(args));
    hddsServiceSpy.setConfiguration(conf);

    DatanodeDetails.Builder builder = DatanodeDetails.newBuilder();
    builder.setUuid(UUID.randomUUID().toString());
    DatanodeDetails datanodeDetails = builder.build();
    Mockito.doReturn(datanodeDetails).when(hddsServiceSpy).getDatanodeDetails();

    ozoneHddsService.start(hddsServiceSpy);

    assertNotNull(ozoneHddsService.getHandler());
    assertNotNull(ozoneHddsService.getObjectStoreRestHttpServer());

    ozoneHddsService.close();
  }

  @Test
  public void testStartupFail() throws IOException {
    OzoneHddsDatanodeService ozoneHddsService = new OzoneHddsDatanodeService();
    ServicePlugin mockService = new MockService();
    ozoneHddsService.start(mockService);

    assertNull(ozoneHddsService.getHandler());
    assertNull(ozoneHddsService.getObjectStoreRestHttpServer());

    ozoneHddsService.close();
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
