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
 *
 */

package org.apache.hadoop.fs.adl.live;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import static org.junit.Assume.*;
import org.junit.Before;

import java.io.IOException;

/**
 * Test Base contract tests on Adl file system.
 */
public class TestAdlFileSystemContractLive extends FileSystemContractBaseTest {
  private FileSystem adlStore;

  @Before
  public void setUp() throws Exception {
    skipTestCheck();
    adlStore = AdlStorageConfiguration.createStorageConnector();
    if (AdlStorageConfiguration.isContractTestEnabled()) {
      fs = adlStore;
    }
    assumeNotNull(fs);
  }

  @After
  public void tearDown() throws Exception {
    if (AdlStorageConfiguration.isContractTestEnabled()) {
      cleanup();
    }
    super.tearDown();
  }

  private void cleanup() throws IOException {
    adlStore.delete(new Path("/test"), true);
  }

  private void skipTestCheck() {
    assumeTrue(AdlStorageConfiguration.isContractTestEnabled());
  }
}