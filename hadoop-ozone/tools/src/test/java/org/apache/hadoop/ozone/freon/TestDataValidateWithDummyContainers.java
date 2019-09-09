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

package org.apache.hadoop.ozone.freon;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScrubberConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests Freon with MiniOzoneCluster and ChunkManagerDummyImpl.
 * Data validation is disabled in RandomKeyGenerator.
 */
public class TestDataValidateWithDummyContainers
    extends TestDataValidate {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestDataValidate.class);
  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   */
  @BeforeClass
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    ContainerScrubberConfiguration sc =
        conf.getObject(ContainerScrubberConfiguration.class);
    sc.setEnabled(false);
    conf.setBoolean(HddsConfigKeys.HDDS_CONTAINER_PERSISTDATA, false);
    conf.setBoolean(OzoneConfigKeys.OZONE_UNSAFEBYTEOPERATIONS_ENABLED,
        false);
    startCluster(conf);
  }

  /**
   * Write validation is not supported for non-persistent containers.
   * This test is a no-op.
   */
  @Test
  @Override
  public void validateWriteTest() throws Exception {
    LOG.info("Skipping validateWriteTest for non-persistent containers.");
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterClass
  public static void shutdown() {
    shutdownCluster();
  }
}
