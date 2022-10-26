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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.performance.AbstractS3ACostTest;

import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_BLOCK_DEFAULT_SIZE;
import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_BLOCK_SIZE_KEY;
import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_ENABLED_KEY;
import static org.apache.hadoop.io.IOUtils.cleanupWithLogger;

/**
 * Test the cache file behaviour with prefetching input stream.
 */
public class ITestS3APrefetchingCacheFiles extends AbstractS3ACostTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3APrefetchingInputStream.class);

  private Path testFile;
  private FileSystem fs;
  private int prefetchBlockSize;

  public ITestS3APrefetchingCacheFiles() {
    super(true);
  }

  @Before
  public void setUp() throws Exception {
    super.setup();
    Configuration conf = getConfiguration();
    String testFileUri = S3ATestUtils.getCSVTestFile(conf);

    testFile = new Path(testFileUri);
    prefetchBlockSize = conf.getInt(PREFETCH_BLOCK_SIZE_KEY, PREFETCH_BLOCK_DEFAULT_SIZE);
    fs = new S3AFileSystem();
    fs.initialize(new URI(testFileUri), getConfiguration());
  }

  @Override
  public Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    S3ATestUtils.removeBaseAndBucketOverrides(conf, PREFETCH_ENABLED_KEY);
    conf.setBoolean(PREFETCH_ENABLED_KEY, true);
    return conf;
  }

  @Override
  public synchronized void teardown() throws Exception {
    super.teardown();
    File tmpFileDir = new File(new File("target", "build"), "test");
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

  @Test
  public void testCacheFileExistence() throws Throwable {
    describe("Verify that FS cache files exist on local FS");

    try (FSDataInputStream in = fs.open(testFile)) {
      byte[] buffer = new byte[prefetchBlockSize];

      in.read(buffer, 0, prefetchBlockSize - 10240);
      in.seek(prefetchBlockSize * 2);
      in.read(buffer, 0, prefetchBlockSize);

      File tmpFileDir = new File(new File("target", "build"), "test");
      assertTrue("The dir to keep cache files must exist", tmpFileDir.exists());
      boolean isCacheFileForBlockFound = false;
      File[] tmpFiles = tmpFileDir.listFiles();
      if (tmpFiles != null) {
        for (File filePath : tmpFiles) {
          String path = filePath.getPath();
          if (path.endsWith(".bin") && path.contains("fs-cache-")) {
            isCacheFileForBlockFound = true;
            break;
          }
        }
      } else {
        LOG.warn("No cache files found under " + tmpFileDir);
      }
      assertTrue("File to cache block data must exist", isCacheFileForBlockFound);
    }
  }

}
