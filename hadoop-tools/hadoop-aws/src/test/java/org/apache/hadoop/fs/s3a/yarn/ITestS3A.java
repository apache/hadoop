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
 */
package org.apache.hadoop.fs.s3a.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.EnumSet;
import org.apache.hadoop.fs.s3a.S3ATestUtils;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * S3A tests through the {@link FileContext} API.
 */
public class ITestS3A {
  private FileContext fc;

  @Rule
  public final Timeout testTimeout = new Timeout(90000);

  @Before
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    fc = S3ATestUtils.createTestFileContext(conf);
  }

  @After
  public void tearDown() throws Exception {
    if (fc != null) {
      fc.delete(getTestPath(), true);
    }
  }

  protected Path getTestPath() {
    return S3ATestUtils.createTestPath(new Path("/tests3afc"));
  }

  @Test
  public void testS3AStatus() throws Exception {
    FsStatus fsStatus = fc.getFsStatus(null);
    assertNotNull(fsStatus);
    assertTrue("Used capacity should be positive: " + fsStatus.getUsed(),
        fsStatus.getUsed() >= 0);
    assertTrue("Remaining capacity should be positive: " + fsStatus
            .getRemaining(),
        fsStatus.getRemaining() >= 0);
    assertTrue("Capacity should be positive: " + fsStatus.getCapacity(),
        fsStatus.getCapacity() >= 0);
  }

  @Test
  public void testS3ACreateFileInSubDir() throws Exception {
    Path dirPath = getTestPath();
    fc.mkdir(dirPath, FileContext.DIR_DEFAULT_PERM, true);
    Path filePath = new Path(dirPath, "file");
    try (FSDataOutputStream file = fc.create(filePath, EnumSet.of(CreateFlag
        .CREATE))) {
      file.write(666);
    }
  }
}
