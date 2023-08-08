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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test class for CSI client.
 */
public class TestCsiClient {
  private static final Logger LOG = LoggerFactory.getLogger(TestCsiClient.class);
  private static File testRoot = null;
  private static String domainSocket = null;
  private static FakeCsiDriver driver = null;

  @BeforeAll
  public static void setUp() throws IOException {
    // Use /tmp to fix bind failure caused by the long file name
    File tmpDir = new File(System.getProperty("java.io.tmpdir"));
    testRoot = Files
        .createTempDirectory(tmpDir.toPath(), "test").toFile();
    File socketPath = new File(testRoot, "csi.sock");
    FileUtils.forceMkdirParent(socketPath);
    domainSocket = "unix://" + socketPath.getAbsolutePath();
    LOG.info("Create unix domain socket: {}", domainSocket);
    driver = new FakeCsiDriver(domainSocket);
  }

  @AfterAll
  public static void tearDown() throws IOException {
    if (testRoot != null) {
      FileUtils.deleteDirectory(testRoot);
    }
  }

  @BeforeEach
  public void beforeMethod() {
    // Skip tests on non-linux systems
    String osName = System.getProperty("os.name").toLowerCase();
    Assumptions.assumeTrue(osName.contains("nix") || osName.contains("nux"));
  }

  @Test
  void testIdentityService() throws IOException {
    try {
      driver.start();
      CsiClient client = new CsiClientImpl(domainSocket);
      Csi.GetPluginInfoResponse response = client.getPluginInfo();
      assertEquals("fake-csi-identity-service", response.getName());
    } finally {
      driver.stop();
    }
  }
}
