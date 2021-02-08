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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.s3a.auth.ITestRestrictedReadAccess;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.mapred.LocatedFileStatusFetcher;

import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.s3a.Constants.AUTHORITATIVE_PATH;
import static org.apache.hadoop.fs.s3a.Constants.METADATASTORE_AUTHORITATIVE;
import static org.apache.hadoop.fs.s3a.Constants.S3_METADATA_STORE_IMPL;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.disableFilesystemCaching;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestBucketName;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBucketOverrides;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.assertThatStatisticCounter;
import static org.apache.hadoop.fs.statistics.IOStatisticAssertions.extractStatistics;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OBJECT_LIST_REQUEST;
import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.LIST_STATUS_NUM_THREADS;

/**
 * Test the LocatedFileStatusFetcher can do.
 * This is related to HADOOP-16458.
 * There's basic tests in ITestS3AFSMainOperations; this
 * is see if we can create better corner cases.
 * <p></p>
 * Much of the class is based on tests in {@link ITestRestrictedReadAccess},
 * but whereas that tests failure paths, this looks at the performance
 * of successful invocations.
 */
@RunWith(Parameterized.class)
public class ITestLocatedFileStatusFetcher extends AbstractS3ATestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestLocatedFileStatusFetcher.class);


  /**
   * Parameterization.
   */
  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {"raw", false},
        {"nonauth", true}
    });
  }

  /** Filter to select everything. */
  private static final PathFilter EVERYTHING = t -> true;

  /** Filter to select .txt files. */
  private static final PathFilter TEXT_FILE =
      path -> path.toUri().toString().endsWith(".txt");

  /** The same path filter used in FileInputFormat. */
  private static final PathFilter HIDDEN_FILE_FILTER =
      (p) -> {
        String n = p.getName();
        return !n.startsWith("_") && !n.startsWith(".");
      };

  /**
   * Text found in LocatedFileStatusFetcher exception when the glob
   * returned "null".
   */
  private static final String DOES_NOT_EXIST = "does not exist";

  /**
   * Text found in LocatedFileStatusFetcher exception when
   * the glob returned an empty list.
   */
  private static final String MATCHES_0_FILES = "matches 0 files";

  /**
   * Text used in files.
   */
  public static final byte[] HELLO = "hello".getBytes(StandardCharsets.UTF_8);

  /**
   * How many list calls are expected in a run which collects them: {@value}.
   */
  private static final int EXPECTED_LIST_COUNT = 4;

  private final String name;

  private final boolean s3guard;

  private Path basePath;

  private Path emptyDir;

  private Path emptyFile;

  private Path subDir;

  private Path subdirFile;

  private Path subDir2;

  private Path subdir2File1;

  private Path subdir2File2;

  private Configuration listConfig;

  public ITestLocatedFileStatusFetcher(final String name,
      final boolean s3guard) {
    this.name = name;
    this.s3guard = s3guard;
  }

  @Override
  public Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    String bucketName = getTestBucketName(conf);

    removeBaseAndBucketOverrides(bucketName, conf,
        METADATASTORE_AUTHORITATIVE,
        AUTHORITATIVE_PATH);
    removeBucketOverrides(bucketName, conf,
        S3_METADATA_STORE_IMPL);
    if (!s3guard) {
      removeBaseAndBucketOverrides(bucketName, conf,
          S3_METADATA_STORE_IMPL);
    }
    conf.setBoolean(METADATASTORE_AUTHORITATIVE, false);
    disableFilesystemCaching(conf);
    return conf;
  }
  @Override
  public void setup() throws Exception {
    super.setup();
    S3AFileSystem fs
        = getFileSystem();
    // avoiding the parameterization to steer clear of accidentally creating
    // patterns; a timestamp is used to ensure tombstones from previous runs
    // do not interfere
    basePath = path("ITestLocatedFileStatusFetcher-" + name
        + "-" + System.currentTimeMillis() / 1000);

    // define the paths and create them.
    describe("Creating test directories and files");

    // an empty directory directory under the noReadDir
    emptyDir = new Path(basePath, "emptyDir");
    fs.mkdirs(emptyDir);

    // an empty file directory under the noReadDir
    emptyFile = new Path(basePath, "emptyFile.txt");
    touch(fs, emptyFile);

    // a subdirectory
    subDir = new Path(basePath, "subDir");

    // and a file in that subdirectory
    subdirFile = new Path(subDir, "subdirFile.txt");
    createFile(fs, subdirFile, true, HELLO);
    subDir2 = new Path(subDir, "subDir2");
    subdir2File1 = new Path(subDir2, "subdir2File1.txt");
    subdir2File2 = new Path(subDir2, "subdir2File2.txt");
    createFile(fs, subdir2File1, true, HELLO);
    createFile(fs, subdir2File2, true, HELLO);
    listConfig = new Configuration(getConfiguration());
  }


  /**
   * Assert that the fetcher stats logs the expected number of calls.
   * @param fetcher fetcher
   * @param expectedListCount expected number of list calls
   */
  private void assertListCount(final LocatedFileStatusFetcher fetcher,
      final int expectedListCount) {
    IOStatistics iostats = extractStatistics(fetcher);
    LOG.info("Statistics of fetcher: {}", iostats);
    assertThatStatisticCounter(iostats,
        OBJECT_LIST_REQUEST)
        .describedAs("stats of %s", iostats)
        .isEqualTo(expectedListCount);
  }

  /**
   * Run a located file status fetcher against the directory tree.
   */
  @Test
  public void testSingleThreadedLocatedFileStatus() throws Throwable {

    describe("LocatedFileStatusFetcher operations");
    // use the same filter as FileInputFormat; single thread.

    listConfig.setInt(LIST_STATUS_NUM_THREADS, 1);
    LocatedFileStatusFetcher fetcher =
        new LocatedFileStatusFetcher(
            listConfig,
            new Path[]{basePath},
            true,
            HIDDEN_FILE_FILTER,
            true);
    Iterable<FileStatus> stats = fetcher.getFileStatuses();
    Assertions.assertThat(stats)
        .describedAs("result of located scan")
        .flatExtracting(FileStatus::getPath)
        .containsExactlyInAnyOrder(
            emptyFile,
            subdirFile,
            subdir2File1,
            subdir2File2);
    assertListCount(fetcher, EXPECTED_LIST_COUNT);
  }

  /**
   * Run a located file status fetcher against the directory tree.
   */
  @Test
  public void testLocatedFileStatusFourThreads() throws Throwable {

    // four threads and the text filter.
    int threads = 4;
    describe("LocatedFileStatusFetcher with %d", threads);
    listConfig.setInt(LIST_STATUS_NUM_THREADS, threads);
    LocatedFileStatusFetcher fetcher =
        new LocatedFileStatusFetcher(
            listConfig,
            new Path[]{basePath},
            true,
            EVERYTHING,
            true);
    Iterable<FileStatus> stats = fetcher.getFileStatuses();
    IOStatistics iostats = extractStatistics(fetcher);
    LOG.info("Statistics of fetcher: {}", iostats);
    Assertions.assertThat(stats)
        .describedAs("result of located scan")
        .isNotNull()
        .flatExtracting(FileStatus::getPath)
        .containsExactlyInAnyOrder(
            emptyFile,
            subdirFile,
            subdir2File1,
            subdir2File2);
    assertListCount(fetcher, EXPECTED_LIST_COUNT);
  }

  /**
   * Run a located file status fetcher against a file.
   */
  @Test
  public void testLocatedFileStatusScanFile() throws Throwable {
    // pass in a file as the base of the scan.
    describe("LocatedFileStatusFetcher with file %s", subdirFile);
    listConfig.setInt(LIST_STATUS_NUM_THREADS, 16);
    LocatedFileStatusFetcher fetcher
        = new LocatedFileStatusFetcher(
        listConfig,
        new Path[]{subdirFile},
        true,
        TEXT_FILE,
        true);
    Iterable<FileStatus> stats = fetcher.getFileStatuses();
    Assertions.assertThat(stats)
        .describedAs("result of located scan")
        .isNotNull()
        .flatExtracting(FileStatus::getPath)
        .containsExactly(subdirFile);
    IOStatistics ioStatistics = fetcher.getIOStatistics();
    Assertions.assertThat(ioStatistics)
        .describedAs("IO statistics of %s", fetcher)
        .isNull();
  }

}
