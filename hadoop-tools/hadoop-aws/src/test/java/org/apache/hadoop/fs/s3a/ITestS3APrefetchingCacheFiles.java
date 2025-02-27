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
import java.util.UUID;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.s3a.performance.AbstractS3ACostTest;

import static org.apache.hadoop.fs.s3a.Constants.BUFFER_DIR;
import static org.apache.hadoop.fs.s3a.Constants.ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_BLOCK_SIZE_KEY;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.enablePrefetching;
import static org.apache.hadoop.fs.s3a.test.PublicDatasetTestUtils.getExternalData;
import static org.apache.hadoop.fs.s3a.test.PublicDatasetTestUtils.isUsingDefaultExternalDataFile;
import static org.apache.hadoop.io.IOUtils.cleanupWithLogger;

/**
 * Test the cache file behaviour with prefetching input stream.
 */
public class ITestS3APrefetchingCacheFiles extends AbstractS3ACostTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3APrefetchingCacheFiles.class);

  /** use a small file size so small source files will still work. */
  public static final int BLOCK_SIZE = 128 * 1024;

  public static final int PREFETCH_OFFSET = 10240;

  private Path testFile;

  /** The FS with the external file. */
  private FileSystem fs;

  private int prefetchBlockSize;
  private Configuration conf;

  private String bufferDir;

  @Before
  public void setUp() throws Exception {
    super.setup();
    // Sets BUFFER_DIR by calling S3ATestUtils#prepareTestConfiguration
    conf = createConfiguration();

    testFile = getExternalData(conf);
    prefetchBlockSize = conf.getInt(PREFETCH_BLOCK_SIZE_KEY, BLOCK_SIZE);
    fs = FileSystem.get(testFile.toUri(), conf);
  }

  @Override
  public Configuration createConfiguration() {
    Configuration configuration = super.createConfiguration();
    if (isUsingDefaultExternalDataFile(configuration)) {
      S3ATestUtils.removeBaseAndBucketOverrides(configuration,
          ENDPOINT);
    }
    enablePrefetching(configuration);
    // use a small block size unless explicitly set in the test config.
    configuration.setInt(PREFETCH_BLOCK_SIZE_KEY, BLOCK_SIZE);
    // patch buffer dir with a unique path for test isolation.
    final String bufferDirBase = configuration.get(BUFFER_DIR);
    bufferDir = bufferDirBase + "/" + UUID.randomUUID();
    configuration.set(BUFFER_DIR, bufferDir);
    return configuration;
  }

  @Override
  public synchronized void teardown() throws Exception {
    super.teardown();
    if (bufferDir != null) {
      new File(bufferDir).delete();
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

      // read a bit less than a block
      in.readFully(0, buffer, 0, prefetchBlockSize - PREFETCH_OFFSET);
      // read at least some of a second block
      in.read(prefetchBlockSize * 2, buffer, 0, prefetchBlockSize);


      File tmpFileDir = new File(conf.get(BUFFER_DIR));
      final LocalFileSystem localFs = FileSystem.getLocal(conf);
      Path bufferDirPath = new Path(tmpFileDir.toURI());
      ContractTestUtils.assertIsDirectory(localFs, bufferDirPath);
      File[] tmpFiles = tmpFileDir
          .listFiles((dir, name) -> name.endsWith(".bin") && name.contains("fs-cache-"));
      Assertions.assertThat(tmpFiles)
          .describedAs("Cache files not found under %s", tmpFileDir)
          .isNotEmpty();


      for (File tmpFile : tmpFiles) {
        Path path = new Path(tmpFile.getAbsolutePath());
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
