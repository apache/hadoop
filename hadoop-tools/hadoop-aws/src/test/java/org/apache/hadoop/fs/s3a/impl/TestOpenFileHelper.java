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
import static org.apache.hadoop.fs.OpenFileOptions.FS_OPTION_OPENFILE_FADVISE_ADAPTIVE;
import static org.apache.hadoop.fs.OpenFileOptions.FS_OPTION_OPENFILE_FADVISE_RANDOM;
import static org.apache.hadoop.fs.OpenFileOptions.FS_OPTION_OPENFILE_LENGTH;
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

  private ObjectAssert<S3AOpenFileOperation.OpenFileInformation> assertFI(
      final S3AOpenFileOperation.OpenFileInformation fi) {
    return Assertions.assertThat(fi)
        .describedAs("File Information %s", fi);
  }

  @Test
  public void testUnknownMandatory() throws Throwable {

    String key = "unknown";
    intercept(IllegalArgumentException.class, key, () ->
        helper.prepareToOpenFile(TESTPATH,
            params(key, "undefined"),
            0));
  }

  @Test
  public void testSeekPolicy() throws Throwable {

    // ask for random IO
    ObjectAssert<S3AOpenFileOperation.OpenFileInformation> asst =
        assertFI(helper.prepareToOpenFile(TESTPATH,
            params(INPUT_FADVISE, FS_OPTION_OPENFILE_FADVISE_RANDOM),
            0));

    // is picked up
    asst.extracting(f -> f.getInputPolicy())
        .isEqualTo(S3AInputPolicy.Random);
    // and as neither status nor length was set: no file status
    asst.extracting(f -> f.getStatus())
        .isNull();
  }

  @Test
  public void testSeekPolicyAdaptive() throws Throwable {

    // when caller asks for adaptive, they get "normal"
    ObjectAssert<S3AOpenFileOperation.OpenFileInformation> asst =
        assertFI(helper.prepareToOpenFile(TESTPATH,
            params(INPUT_FADVISE, FS_OPTION_OPENFILE_FADVISE_ADAPTIVE),
            0));

    asst.extracting(f -> f.getInputPolicy())
        .isEqualTo(S3AInputPolicy.Normal);
  }

  @Test
  public void testUnknownSeekPolicy() throws Throwable {

    // fall back to the normal seek policy.
    ObjectAssert<S3AOpenFileOperation.OpenFileInformation> asst =
        assertFI(helper.prepareToOpenFile(TESTPATH,
            params(INPUT_FADVISE, "undefined"),
            0));

    asst.extracting(f -> f.getInputPolicy())
        .isEqualTo(S3AInputPolicy.Normal);
  }

  @Test
  public void testReadahead() throws Throwable {

    // readahead range option
    ObjectAssert<S3AOpenFileOperation.OpenFileInformation> asst =
        assertFI(helper.prepareToOpenFile(TESTPATH,
            params(READAHEAD_RANGE, "4096"),
            0));

    asst.extracting(f -> f.getReadAheadRange())
        .isEqualTo(4096L);
  }

  @Test
  public void testStatusWithValidFilename() throws Throwable {
    Path p = new Path("file:///tmp/" + TESTPATH.getName());
    ObjectAssert<S3AOpenFileOperation.OpenFileInformation> asst =
        assertFI(helper.prepareToOpenFile(TESTPATH,
            params(FS_OPTION_OPENFILE_LENGTH, "32")
                .withStatus(status(p, 4096)),
            0));
    asst.extracting(f -> f.getStatus().getVersionId())
        .isEqualTo("version");
    asst.extracting(f -> f.getStatus().getETag())
        .isEqualTo("etag");
    asst.extracting(f -> f.getStatus().getLen())
        .isEqualTo(4096L);
  }

  @Test
  public void testLocatedStatus() throws Throwable {
    Path p = new Path("file:///tmp/" + TESTPATH.getName());
    ObjectAssert<S3AOpenFileOperation.OpenFileInformation> asst =
        assertFI(helper.prepareToOpenFile(TESTPATH,
            params(FS_OPTION_OPENFILE_LENGTH, "32")
                .withStatus(
                    new S3ALocatedFileStatus(
                    status(p, 4096), null)),
            0));
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
        helper.prepareToOpenFile(TESTPATH,
            params(INPUT_FADVISE, "normal")
                .withStatus(new S3AFileStatus(true, TESTPATH, USERNAME)),
            0));
  }

  /**
   * File name must match the path argument to openFile().
   */
  @Test
  public void testStatusWithInconsistentFilename() throws Throwable {
    intercept(IllegalArgumentException.class, TESTFILE, () ->
        helper.prepareToOpenFile(TESTPATH,
            params(INPUT_FADVISE, "normal")
                .withStatus(new S3AFileStatus(true,
                    new Path(TESTFILE + "-"), USERNAME)),
            0));
  }

  /**
   * If a file length option is set, create a file status
   */
  @Test
  public void testFileLength() throws Throwable {
    ObjectAssert<S3AOpenFileOperation.OpenFileInformation> asst =
        assertFI(helper.prepareToOpenFile(TESTPATH,
            params(FS_OPTION_OPENFILE_LENGTH, "8192")
                .withStatus(null),
            0));
    asst.extracting(f -> f.getStatus())
        .isNotNull();
    asst.extracting(f -> f.getStatus().getPath())
        .isEqualTo(TESTPATH);
    asst.extracting(f -> f.getStatus().getLen())
        .isEqualTo(8192L);
  }


  private S3AFileStatus status(final Path p, final int length) {
    return new S3AFileStatus(length, 0,
        p, 0, "", "etag", "version");
  }

  private OpenFileParameters params(final String key, final String val) {
    return new OpenFileParameters()
        .withMandatoryKeys(singleton(key))
        .withOptions(conf(key, val));
  }

  private Configuration conf(String key, Object val) {
    Configuration c = new Configuration(false);
    c.set(key, val.toString());
    return c;
  }
}
