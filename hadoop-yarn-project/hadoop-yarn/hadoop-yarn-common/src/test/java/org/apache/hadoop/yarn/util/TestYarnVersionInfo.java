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

package org.apache.hadoop.yarn.util;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A JUnit test to test {@link YarnVersionInfo}
 */
public class TestYarnVersionInfo {

  /**
   * Test the yarn version info routines.
   * @throws IOException
   */
  @Test
  void versionInfoGenerated() throws IOException {

    // can't easily know what the correct values are going to be so just
    // make sure they aren't Unknown
    assertNotEquals("Unknown", YarnVersionInfo.getVersion(), "getVersion returned Unknown");
    assertNotEquals("Unknown", YarnVersionInfo.getUser(), "getUser returned Unknown");
    assertNotEquals("Unknown", YarnVersionInfo.getSrcChecksum(), "getSrcChecksum returned Unknown");

    // these could be Unknown if the VersionInfo generated from code not in svn or git
    // so just check that they return something
    assertNotNull(YarnVersionInfo.getUrl(), "getUrl returned null");
    assertNotNull(YarnVersionInfo.getRevision(), "getRevision returned null");
    assertNotNull(YarnVersionInfo.getBranch(), "getBranch returned null");

    assertTrue(YarnVersionInfo.getBuildVersion().contains("source checksum"),
        "getBuildVersion check doesn't contain: source checksum");

  }
}
