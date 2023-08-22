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

package org.apache.hadoop.fs.s3a;

import java.io.File;
import java.net.URI;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.s3a.performance.AbstractS3ACostTest;

import static org.apache.hadoop.fs.s3a.Constants.BUFFER_DIR;
import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_BLOCK_DEFAULT_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_BLOCK_SIZE_KEY;
import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_ENABLED_KEY;
import static org.apache.hadoop.io.IOUtils.cleanupWithLogger;

/**
 * Test the cache file behaviour with prefetching input stream.
 */
public class ITestS3APrefetchingCacheFiles extends AbstractS3ACostTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3APrefetchingCacheFiles.class);

  private Path testFile;
  private FileSystem fs;
  private int prefetchBlockSize;
  private Configuration conf;

  public ITestS3APrefetchingCacheFiles() {
    super(true);
  }

  @Before
  public void setUp() throws Exception {
    super.setup();
    // Sets BUFFER_DIR by calling S3ATestUtils#prepareTestConfiguration
    conf = createConfiguration();
    String testFileUri = S3ATestUtils.getCSVTestFile(conf);

    testFile = new Path(testFileUri);
    prefetchBlockSize = conf.getInt(PREFETCH_BLOCK_SIZE_KEY, PREFETCH_BLOCK_DEFAULT_SIZE);
    fs = getFileSystem();
    fs.initialize(new URI(testFileUri), conf);
  }

  @Override
  public Configuration createConfiguration() {
    Configuration configuration = super.createConfiguration();
    S3ATestUtils.removeBaseAndBucketOverrides(configuration, PREFETCH_ENABLED_KEY);
    configuration.setBoolean(PREFETCH_ENABLED_KEY, true);
    return configuration;
  }

  @Override
  public synchronized void teardown() throws Exception {
    super.teardown();
    File tmpFileDir = new File(conf.get(BUFFER_DIR));
    File[] tmpFiles = tmpFileDir.listFiles();
    if (tmpFiles != null) {
      for (File filePath : tmpFiles) {
        String path = filePath.getPath();
        if (path.endsWith(".bin") && path.contains("fs-cache-")) {
          filePath.delete();
        }
      }
    }
    cleanupWithLogger(LOG, fs);
    fs = null;
    testFile = null;
  }

  /**
   * Test to verify the existence of the cache file.
   * Tries to perform inputStream read and seek ops to make the prefetching take place and
   * asserts whether file with .bin suffix is present. It also verifies certain file stats.
   */
  @Test
  public void testCacheFileExistence() throws Throwable {
    describe("Verify that FS cache files exist on local FS");
    skipIfClientSideEncryption();

    try (FSDataInputStream in = fs.open(testFile)) {
      byte[] buffer = new byte[prefetchBlockSize];

      in.read(buffer, 0, prefetchBlockSize - 10240);
      in.seek(prefetchBlockSize * 2);
      in.read(buffer, 0, prefetchBlockSize);

      File tmpFileDir = new File(conf.get(BUFFER_DIR));
      assertTrue("The dir to keep cache files must exist", tmpFileDir.exists());
      File[] tmpFiles = tmpFileDir
          .listFiles((dir, name) -> name.endsWith(".bin") && name.contains("fs-cache-"));
      boolean isCacheFileForBlockFound = tmpFiles != null && tmpFiles.length > 0;
      if (!isCacheFileForBlockFound) {
        LOG.warn("No cache files found under " + tmpFileDir);
      }
      assertTrue("File to cache block data must exist", isCacheFileForBlockFound);

      for (File tmpFile : tmpFiles) {
        Path path = new Path(tmpFile.getAbsolutePath());
        try (FileSystem localFs = FileSystem.getLocal(conf)) {
          FileStatus stat = localFs.getFileStatus(path);
          ContractTestUtils.assertIsFile(path, stat);
          assertEquals("File length not matching with prefetchBlockSize", prefetchBlockSize,
              stat.getLen());
          assertEquals("User permissions should be RW", FsAction.READ_WRITE,
              stat.getPermission().getUserAction());
          assertEquals("Group permissions should be NONE", FsAction.NONE,
              stat.getPermission().getGroupAction());
          assertEquals("Other permissions should be NONE", FsAction.NONE,
              stat.getPermission().getOtherAction());
        }
      }
    }
  }

}
