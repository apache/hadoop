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

package org.apache.hadoop.ozone.genconf;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;


/**
 * Tests GenerateOzoneRequiredConfigurations.
 */
public class TestGenerateOzoneRequiredConfigurations {
  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true and
   * OZONE_HANDLER_TYPE_KEY = "distributed"
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(1).build();
    cluster.waitForClusterToBeReady();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Tests a valid path and generates ozone-site.xml.
   * @throws Exception
   */
  @Test
  public void generateConfigurationsSuccess() throws Exception {
    String[] args = new String[]{"-output", "."};
    GenerateOzoneRequiredConfigurations.main(args);

    Assert.assertEquals("Path is valid",
        true, GenerateOzoneRequiredConfigurations.isValidPath(args[1]));

    Assert.assertEquals("Permission is valid",
        true, GenerateOzoneRequiredConfigurations.canWrite(args[1]));

    Assert.assertEquals("Config file generated",
        0, GenerateOzoneRequiredConfigurations.generateConfigurations(args[1]));
  }

  /**
   * Test to avoid generating ozone-site.xml when invalid permission.
   * @throws Exception
   */
  @Test
  public void generateConfigurationsFailure() throws Exception {
    String[] args = new String[]{"-output", "/"};
    GenerateOzoneRequiredConfigurations.main(args);

    Assert.assertEquals("Path is valid",
        true, GenerateOzoneRequiredConfigurations.isValidPath(args[1]));

    Assert.assertEquals("Invalid permission",
        false, GenerateOzoneRequiredConfigurations.canWrite(args[1]));

    Assert.assertEquals("Config file not generated",
        1, GenerateOzoneRequiredConfigurations.generateConfigurations(args[1]));
  }
}
