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

package org.apache.hadoop.yarn.csi.client;

import csi.v0.Csi;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

/**
 * Test class for CSI client.
 */
public class TestCsiClient {

  private static File testRoot = null;
  private static String domainSocket = null;
  private static FakeCsiDriver driver = null;

  @BeforeClass
  public static void setUp() throws IOException {
    File testDir = GenericTestUtils.getTestDir();
    testRoot = Files
        .createTempDirectory(testDir.toPath(), "test").toFile();
    File socketPath = new File(testRoot, "csi.sock");
    FileUtils.forceMkdirParent(socketPath);
    domainSocket = "unix://" + socketPath.getAbsolutePath();
    driver = new FakeCsiDriver(domainSocket);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (testRoot != null) {
      FileUtils.deleteDirectory(testRoot);
    }
  }

  @Before
  public void beforeMethod() {
    // Skip tests on non-linux systems
    String osName = System.getProperty("os.name").toLowerCase();
    Assume.assumeTrue(osName.contains("nix") || osName.contains("nux"));
  }

  @Test
  public void testIdentityService() throws IOException {
    try {
      driver.start();
      CsiClient client = new CsiClientImpl(domainSocket);
      Csi.GetPluginInfoResponse response = client.getPluginInfo();
      Assert.assertEquals("fake-csi-identity-service", response.getName());
    } finally {
      driver.stop();
    }
  }
}
