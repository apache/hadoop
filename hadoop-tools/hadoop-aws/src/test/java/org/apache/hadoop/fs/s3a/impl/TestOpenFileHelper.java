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

package org.apache.hadoop.fs.s3a.impl;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.ObjectAssert;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.impl.OpenFileParameters;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AInputPolicy;
import org.apache.hadoop.fs.s3a.S3ALocatedFileStatus;
import org.apache.hadoop.test.HadoopTestBase;

import static java.util.Collections.singleton;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_FADVISE_ADAPTIVE;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_FADVISE_RANDOM;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_LENGTH;
import static org.apache.hadoop.fs.s3a.Constants.INPUT_FADVISE;
import static org.apache.hadoop.fs.s3a.Constants.READAHEAD_RANGE;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Unit tests for {@link S3AOpenFileOperation}.
 */
public class TestOpenFileHelper extends HadoopTestBase {

  private static final ChangeDetectionPolicy CHANGE_POLICY
      = ChangeDetectionPolicy.createPolicy(ChangeDetectionPolicy.Mode.Server,
      ChangeDetectionPolicy.Source.None, false);

  private static final long READ_AHEAD_RANGE = 16;

  private static final String USERNAME = "hadoop";

  public static final S3AInputPolicy INPUT_POLICY = S3AInputPolicy.Sequential;

  public static final String TESTFILE = "s3a://bucket/name";

  private static final Path TESTPATH = new Path(TESTFILE);

  private S3AOpenFileOperation helper;

  @Before
  public void setup() {
    helper = new S3AOpenFileOperation(INPUT_POLICY,
        CHANGE_POLICY, READ_AHEAD_RANGE, USERNAME);
  }

  @Test
  public void testSimpleFile() throws Throwable {
    ObjectAssert<S3AOpenFileOperation.OpenFileInformation>
        asst = assertFI(helper.openSimpleFile());

    asst.extracting(f -> f.getChangePolicy())
        .isEqualTo(CHANGE_POLICY);
    asst.extracting(f -> f.getInputPolicy())
        .isEqualTo(INPUT_POLICY);
    asst.extracting(f -> f.getReadAheadRange())
        .isEqualTo(READ_AHEAD_RANGE);
  }

  /**
   * Initiate an assert from an open file information instance.
   * @param fi file info
   * @return an assert stream.
   */
  private ObjectAssert<S3AOpenFileOperation.OpenFileInformation> assertFI(
      final S3AOpenFileOperation.OpenFileInformation fi) {
    return Assertions.assertThat(fi)
        .describedAs("File Information %s", fi);
  }

  public ObjectAssert<S3AOpenFileOperation.OpenFileInformation> assertOpenFile(
      final String key, final String option) throws IOException {
    return assertFI(prepareToOpenFile(params(key, option)));
  }

  @Test
  public void testUnknownMandatoryOption() throws Throwable {

    String key = "unknown";
    intercept(IllegalArgumentException.class, key, () ->
        prepareToOpenFile(params(key, "undefined")));
  }

  @Test
  public void testSeekRandomIOPolicy() throws Throwable {

    // ask for random IO
    String option = FS_OPTION_OPENFILE_FADVISE_RANDOM;

    // is picked up
    assertOpenFile(INPUT_FADVISE, option).extracting(f -> f.getInputPolicy())
        .isEqualTo(S3AInputPolicy.Random);
    // and as neither status nor length was set: no file status
    assertOpenFile(INPUT_FADVISE, option).extracting(f -> f.getStatus())
        .isNull();
  }


  @Test
  public void testSeekPolicyAdaptive() throws Throwable {

    // when caller asks for adaptive, they get "normal"
    ObjectAssert<S3AOpenFileOperation.OpenFileInformation> asst =
        assertOpenFile(INPUT_FADVISE, FS_OPTION_OPENFILE_FADVISE_ADAPTIVE);

    asst.extracting(f -> f.getInputPolicy())
        .isEqualTo(S3AInputPolicy.Normal);
  }

  /**
   * Verify that an unknown seek policy falls back to "normal".
   */
  @Test
  public void testUnknownSeekPolicy() throws Throwable {

    // fall back to the normal seek policy.

    assertOpenFile(INPUT_FADVISE, "undefined")
        .extracting(f -> f.getInputPolicy())
        .isEqualTo(S3AInputPolicy.Normal);
  }

  /**
   * Verify readahead range is picked up.
   */
  @Test
  public void testReadahead() throws Throwable {

    // readahead range option

    assertOpenFile(READAHEAD_RANGE, "4096")
        .extracting(f -> f.getReadAheadRange())
        .isEqualTo(4096L);
  }

  @Test
  public void testStatusWithValidFilename() throws Throwable {
    Path p = new Path("file:///tmp/" + TESTPATH.getName());
    ObjectAssert<S3AOpenFileOperation.OpenFileInformation> asst =
        assertFI(prepareToOpenFile(
            params(FS_OPTION_OPENFILE_LENGTH, "32")
                .withStatus(status(p, 4096))));
    asst.extracting(f -> f.getStatus().getVersionId())
        .isEqualTo("version");
    asst.extracting(f -> f.getStatus().getETag())
        .isEqualTo("etag");
    asst.extracting(f -> f.getStatus().getLen())
        .isEqualTo(4096L);
  }

  /**
   * Verify S3ALocatedFileStatus is handled.
   */
  @Test
  public void testLocatedStatus() throws Throwable {
    Path p = new Path("file:///tmp/" + TESTPATH.getName());
    ObjectAssert<S3AOpenFileOperation.OpenFileInformation> asst =
        assertFI(
            prepareToOpenFile(
                params(FS_OPTION_OPENFILE_LENGTH, "32")
                    .withStatus(
                        new S3ALocatedFileStatus(
                            status(p, 4096), null))));
    asst.extracting(f -> f.getStatus().getVersionId())
        .isEqualTo("version");
    asst.extracting(f -> f.getStatus().getETag())
        .isEqualTo("etag");
    asst.extracting(f -> f.getStatus().getLen())
        .isEqualTo(4096L);
  }

  /**
   * Callers cannot supply a directory status when opening a file.
   */
  @Test
  public void testDirectoryStatus() throws Throwable {
    intercept(FileNotFoundException.class, TESTFILE, () ->
        prepareToOpenFile(
            params(INPUT_FADVISE, "normal")
                .withStatus(new S3AFileStatus(true, TESTPATH, USERNAME))));
  }

  /**
   * File name must match the path argument to openFile().
   */
  @Test
  public void testStatusWithInconsistentFilename() throws Throwable {
    intercept(IllegalArgumentException.class, TESTFILE, () ->
        prepareToOpenFile(params(INPUT_FADVISE, "normal")
            .withStatus(new S3AFileStatus(true,
                new Path(TESTFILE + "-"), USERNAME))));
  }

  public S3AOpenFileOperation.OpenFileInformation prepareToOpenFile(
      final OpenFileParameters parameters)
      throws IOException {
    return helper.prepareToOpenFile(TESTPATH,
        parameters, 0);
  }

  /**
   * If a file length option is set, a file status
   * is created.
   */
  @Test
  public void testFileLength() throws Throwable {
    ObjectAssert<S3AOpenFileOperation.OpenFileInformation> asst =
        assertFI(prepareToOpenFile(
            params(FS_OPTION_OPENFILE_LENGTH, "8192")
                .withStatus(null)));
    asst.extracting(f -> f.getStatus())
        .isNotNull();
    asst.extracting(f -> f.getStatus().getPath())
        .isEqualTo(TESTPATH);
    asst.extracting(f -> f.getStatus().getLen())
        .isEqualTo(8192L);
  }


  /**
   * Create an S3A status entry with stub etag and versions, timestamp of 0.
   * @param path status path
   * @param length file length
   * @return a status instance.
   */
  private S3AFileStatus status(final Path path, final int length) {
    return new S3AFileStatus(length, 0,
        path, 0, "", "etag", "version");
  }

  /**
   * Create an instance of {@link OpenFileParameters} with
   * the key as a mandatory parameter.
   * @param key mandatory key
   * @param val value
   * @return the instance.
   */
  private OpenFileParameters params(final String key, final String val) {
    return new OpenFileParameters()
        .withMandatoryKeys(singleton(key))
        .withOptions(conf(key, val));
  }

  /**
   * Create a configuration with a single entry.
   * @param key entry key
   * @param val entry value
   * @return a configuration
   */
  private Configuration conf(String key, Object val) {
    Configuration c = new Configuration(false);
    c.set(key, val.toString());
    return c;
  }
}
