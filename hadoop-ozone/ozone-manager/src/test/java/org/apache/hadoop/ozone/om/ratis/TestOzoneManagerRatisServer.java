/*
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

package org.apache.hadoop.ozone.om.ratis;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.ratis.util.LifeCycle;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test OM Ratis server.
 */
public class TestOzoneManagerRatisServer {

  private Configuration conf;
  private OzoneManagerRatisServer omRatisServer;
  private String omID;

  @Before
  public void init() {
    conf = new OzoneConfiguration();
    omID = UUID.randomUUID().toString();
    final String path = GenericTestUtils.getTempPath(omID);
    Path metaDirPath = Paths.get(path, "om-meta");
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDirPath.toString());
  }

  @After
  public void shutdown() {
    if (omRatisServer != null) {
      omRatisServer.stop();
    }
  }

  /**
   * Start a OM Ratis Server and checks its state.
   */
  @Test
  public void testStartOMRatisServer() throws Exception {
    omRatisServer = OzoneManagerRatisServer.newOMRatisServer(omID, conf);
    omRatisServer.start();
    Assert.assertEquals("Ratis Server should be in running state",
        LifeCycle.State.RUNNING, omRatisServer.getServerState());
  }
}
