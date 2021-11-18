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

package org.apache.hadoop.fs.contract;

import java.nio.charset.StandardCharsets;

import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.EtagSource;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.CommonPathCapabilities.ETAGS_AVAILABLE;
import static org.apache.hadoop.fs.CommonPathCapabilities.ETAGS_PRESERVED_IN_RENAME;

/**
 * For filesystems which support etags, validate correctness
 * of their implementation.
 */
public abstract class AbstractContractEtagTest extends
    AbstractFSContractTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractContractEtagTest.class);

  /**
   * basic consistency across operations, as well as being non-empty.
   */
  @Test
  public void testEtagConsistencyAcrossListAndHead() throws Throwable {
    describe("Etag values must be non-empty and consistent across LIST and HEAD Calls.");
    final Path path = methodPath();
    final FileSystem fs = getFileSystem();

    Assertions.assertThat(fs.hasPathCapability(path, ETAGS_AVAILABLE))
        .describedAs("path capability %s of %s",
            ETAGS_AVAILABLE, path)
        .isTrue();

    ContractTestUtils.touch(fs, path);

    final FileStatus st = fs.getFileStatus(path);
    final String etag = etagFromStatus(st);
    LOG.info("etag of empty file is \"{}\"", etag);

    final FileStatus[] statuses = fs.listStatus(path);
    Assertions.assertThat(statuses)
        .describedAs("List(%s)", path)
        .hasSize(1);
    final FileStatus lsStatus = statuses[0];
    Assertions.assertThat(etagFromStatus(lsStatus))
        .describedAs("etag of list status (%s) compared to HEAD value of %s", lsStatus, st)
        .isEqualTo(etag);
  }

  /**
   * Get an etag from a FileStatus which MUST BE
   * an implementation of EtagSource and
   * whose etag MUST NOT BE null/empty.
   * @param st the status
   * @return the etag
   */
  String etagFromStatus(FileStatus st) {
    Assertions.assertThat(st)
        .describedAs("FileStatus %s", st)
        .isInstanceOf(EtagSource.class);
    final String etag = ((EtagSource) st).getEtag();
    Assertions.assertThat(etag)
        .describedAs("Etag of %s", st)
        .isNotBlank();
    return etag;
  }

  /**
   * Overwritten data has different etags.
   */
  @Test
  public void testEtagsOfDifferentDataDifferent() throws Throwable {
    describe("Verify that two different blocks of data written have different tags");

    final Path path = methodPath();
    final FileSystem fs = getFileSystem();
    Path src = new Path(path, "src");

    ContractTestUtils.createFile(fs, src, true,
        "data1234".getBytes(StandardCharsets.UTF_8));
    final FileStatus srcStatus = fs.getFileStatus(src);
    final String srcTag = etagFromStatus(srcStatus);
    LOG.info("etag of file 1 is \"{}\"", srcTag);

    // now overwrite with data of same length
    // (ensure that path or length aren't used exclusively as tag)
    ContractTestUtils.createFile(fs, src, true,
        "1234data".getBytes(StandardCharsets.UTF_8));

    // validate
    final String tag2 = etagFromStatus(fs.getFileStatus(src));
    LOG.info("etag of file 2 is \"{}\"", tag2);

    Assertions.assertThat(tag2)
        .describedAs("etag of updated file")
        .isNotEqualTo(srcTag);
  }

  /**
   * If supported, rename preserves etags.
   */
  @Test
  public void testEtagConsistencyAcrossRename() throws Throwable {
    describe("Verify that when a file is renamed, the etag remains unchanged");
    final Path path = methodPath();
    final FileSystem fs = getFileSystem();
    Assume.assumeTrue(
        "Filesystem does not declare that etags are preserved across renames",
        fs.hasPathCapability(path, ETAGS_PRESERVED_IN_RENAME));
    Path src = new Path(path, "src");
    Path dest = new Path(path, "dest");

    ContractTestUtils.createFile(fs, src, true,
        "sample data".getBytes(StandardCharsets.UTF_8));
    final FileStatus srcStatus = fs.getFileStatus(src);
    LOG.info("located file status string value " + srcStatus);

    final String srcTag = etagFromStatus(srcStatus);
    LOG.info("etag of short file is \"{}\"", srcTag);

    Assertions.assertThat(srcTag)
        .describedAs("Etag of %s", srcStatus)
        .isNotBlank();

    // rename
    fs.rename(src, dest);

    // validate
    FileStatus destStatus = fs.getFileStatus(dest);
    final String destTag = etagFromStatus(destStatus);
    Assertions.assertThat(destTag)
        .describedAs("etag of list status (%s) compared to HEAD value of %s",
            destStatus, srcStatus)
        .isEqualTo(srcTag);
  }

  /**
   * For effective use of etags, listLocatedStatus SHOULD return status entries
   * with consistent values.
   * This ensures that listing during query planning can collect and use the etags.
   */
  @Test
  public void testLocatedStatusAlsoHasEtag() throws Throwable {
    describe("verify that listLocatedStatus() and listFiles() are etag sources");
    final Path path = methodPath();
    final FileSystem fs = getFileSystem();
    Path src = new Path(path, "src");
    ContractTestUtils.createFile(fs, src, true,
        "sample data".getBytes(StandardCharsets.UTF_8));
    final FileStatus srcStatus = fs.getFileStatus(src);
    final String srcTag = etagFromStatus(srcStatus);
    final LocatedFileStatus entry = fs.listLocatedStatus(path).next();
    LOG.info("located file status string value " + entry);
    final String listTag = etagFromStatus(entry);
    Assertions.assertThat(listTag)
        .describedAs("etag of listLocatedStatus (%s) compared to HEAD value of %s",
            entry, srcStatus)
        .isEqualTo(srcTag);

    final LocatedFileStatus entry2 = fs.listFiles(path, false).next();
    Assertions.assertThat(etagFromStatus(entry2))
        .describedAs("etag of listFiles (%s) compared to HEAD value of %s",
            entry, srcStatus)
        .isEqualTo(srcTag);
  }
}
