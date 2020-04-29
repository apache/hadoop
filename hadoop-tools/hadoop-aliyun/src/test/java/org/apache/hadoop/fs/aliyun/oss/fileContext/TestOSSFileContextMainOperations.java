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

package org.apache.hadoop.fs.aliyun.oss.fileContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContextMainOperationsBaseTest;
import org.apache.hadoop.fs.aliyun.oss.AliyunOSSTestUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

/**
 * OSS implementation of FileContextMainOperationsBaseTest.
 */
public class TestOSSFileContextMainOperations
    extends FileContextMainOperationsBaseTest {

  @Before
  public void setUp() throws IOException, Exception {
    Configuration conf = new Configuration();
    fc = AliyunOSSTestUtils.createTestFileContext(conf);
    super.setUp();
  }

  @Override
  protected boolean listCorruptedBlocksSupported() {
    return false;
  }

  @Test
  @Ignore
  public void testCreateFlagAppendExistingFile() throws IOException {
    // append not supported, so test removed
  }

  @Test
  @Ignore
  public void testCreateFlagCreateAppendExistingFile() throws IOException {
    // append not supported, so test removed
  }

  @Test
  @Ignore
  public void testSetVerifyChecksum() throws IOException {
    // checksums ignored, so test ignored
  }

  @Test
  @Ignore
  public void testBuilderCreateAppendExistingFile() throws IOException {
    // append not supported, so test removed
  }
}