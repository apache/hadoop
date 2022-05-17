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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.ObjectAssert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.impl.OpenFileParameters;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AInputPolicy;
import org.apache.hadoop.fs.s3a.S3ALocatedFileStatus;
import org.apache.hadoop.test.HadoopTestBase;

import static java.util.Collections.singleton;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_BUFFER_SIZE;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_LENGTH;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_ADAPTIVE;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_DEFAULT;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_RANDOM;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_SEQUENTIAL;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_SPLIT_END;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_SPLIT_START;
import static org.apache.hadoop.fs.s3a.Constants.DEFAULT_ASYNC_DRAIN_THRESHOLD;
import static org.apache.hadoop.fs.s3a.Constants.INPUT_FADVISE;
import static org.apache.hadoop.fs.s3a.Constants.READAHEAD_RANGE;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Unit tests for {@link OpenFileSupport} and the associated
 * seek policy lookup in {@link S3AInputPolicy}.
 */
public class TestOpenFileSupport extends HadoopTestBase {

  private static final ChangeDetectionPolicy CHANGE_POLICY =
      ChangeDetectionPolicy.createPolicy(
          ChangeDetectionPolicy.Mode.Server,
          ChangeDetectionPolicy.Source.None,
          false);

  private static final long READ_AHEAD_RANGE = 16;

  private static final String USERNAME = "hadoop";

  public static final S3AInputPolicy INPUT_POLICY = S3AInputPolicy.Sequential;

  public static final String TESTFILE = "s3a://bucket/name";

  private static final Path TESTPATH = new Path(TESTFILE);

  /**
   * Create a OpenFileSupport instance.
   */
  private static final OpenFileSupport PREPARE =
      new OpenFileSupport(
          CHANGE_POLICY,
          READ_AHEAD_RANGE,
          USERNAME,
          IO_FILE_BUFFER_SIZE_DEFAULT,
          DEFAULT_ASYNC_DRAIN_THRESHOLD,
          INPUT_POLICY);

  @Test
  public void testSimpleFile() throws Throwable {
    ObjectAssert<OpenFileSupport.OpenFileInformation>
        asst = assertFileInfo(
            PREPARE.openSimpleFile(1024));

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
  private ObjectAssert<OpenFileSupport.OpenFileInformation> assertFileInfo(
      final OpenFileSupport.OpenFileInformation fi) {
    return Assertions.assertThat(fi)
        .describedAs("File Information %s", fi);
  }

  /**
   * Create an assertion about the openFile information from a configuration
   * with the given key/value option.
   * @param key key to set.
   * @param option option value.
   * @return the constructed OpenFileInformation.
   */
  public ObjectAssert<OpenFileSupport.OpenFileInformation> assertOpenFile(
      final String key,
      final String option) throws IOException {
    return assertFileInfo(prepareToOpenFile(params(key, option)));
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
    String option = FS_OPTION_OPENFILE_READ_POLICY_RANDOM;

    // is picked up
    assertOpenFile(INPUT_FADVISE, option)
        .extracting(f -> f.getInputPolicy())
        .isEqualTo(S3AInputPolicy.Random);
    // and as neither status nor length was set: no file status
    assertOpenFile(INPUT_FADVISE, option)
        .extracting(f -> f.getStatus())
        .isNull();
  }

  /**
   * There's a standard policy name. 'adaptive',
   * meaning 'whatever this stream does to adapt to the client's use'.
   * On the S3A connector that is mapped to {@link S3AInputPolicy#Normal}.
   */
  @Test
  public void testSeekPolicyAdaptive() throws Throwable {

    // when caller asks for adaptive, they get "normal"
    assertOpenFile(FS_OPTION_OPENFILE_READ_POLICY,
        FS_OPTION_OPENFILE_READ_POLICY_ADAPTIVE)
        .extracting(f -> f.getInputPolicy())
        .isEqualTo(S3AInputPolicy.Normal);
  }

  /**
   * Verify that an unknown seek policy falls back to
   * {@link S3AInputPolicy#Normal}.
   */
  @Test
  public void testUnknownSeekPolicyS3AOption() throws Throwable {
    // fall back to the normal seek policy.
    assertOpenFile(INPUT_FADVISE, "undefined")
        .extracting(f -> f.getInputPolicy())
        .isEqualTo(INPUT_POLICY);
  }

  /**
   * The S3A option also supports a list of values.
   */
  @Test
  public void testSeekPolicyListS3AOption() throws Throwable {
    // fall back to the second seek policy if the first is unknown
    assertOpenFile(INPUT_FADVISE, "hbase, random")
        .extracting(f -> f.getInputPolicy())
        .isEqualTo(S3AInputPolicy.Random);
  }

  /**
   * Verify that if a list of policies is supplied in a configuration,
   * the first recognized policy will be adopted.
   */
  @Test
  public void testSeekPolicyExtractionFromList() throws Throwable {
    String plist = "a, b, RandOm, other ";
    Configuration conf = conf(FS_OPTION_OPENFILE_READ_POLICY, plist);
    Collection<String> options = conf.getTrimmedStringCollection(
        FS_OPTION_OPENFILE_READ_POLICY);
    Assertions.assertThat(S3AInputPolicy.getFirstSupportedPolicy(options, null))
        .describedAs("Policy from " + plist)
        .isEqualTo(S3AInputPolicy.Random);
  }

  @Test
  public void testAdaptiveSeekPolicyRecognized() throws Throwable {
    Assertions.assertThat(S3AInputPolicy.getPolicy("adaptive", null))
        .describedAs("adaptive")
        .isEqualTo(S3AInputPolicy.Normal);
  }

  @Test
  public void testUnknownSeekPolicyFallback() throws Throwable {
    Assertions.assertThat(S3AInputPolicy.getPolicy("unknown", null))
        .describedAs("unknown policy")
        .isNull();
  }

  /**
   * Test the mapping of the standard option names.
   */
  @Test
  public void testInputPolicyMapping() throws Throwable {
    Object[][] policyMapping = {
        {"normal", S3AInputPolicy.Normal},
        {FS_OPTION_OPENFILE_READ_POLICY_ADAPTIVE, S3AInputPolicy.Normal},
        {FS_OPTION_OPENFILE_READ_POLICY_DEFAULT, S3AInputPolicy.Normal},
        {FS_OPTION_OPENFILE_READ_POLICY_RANDOM, S3AInputPolicy.Random},
        {FS_OPTION_OPENFILE_READ_POLICY_SEQUENTIAL, S3AInputPolicy.Sequential},
    };
    for (Object[] mapping : policyMapping) {
      String name = (String) mapping[0];
      Assertions.assertThat(S3AInputPolicy.getPolicy(name, null))
          .describedAs("Policy %s", name)
          .isEqualTo(mapping[1]);
    }
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

  /**
   * Verify buffer size  is picked up.
   */
  @Test
  public void testBufferSize() throws Throwable {
    // readahead range option
    assertOpenFile(FS_OPTION_OPENFILE_BUFFER_SIZE, "4096")
        .extracting(f -> f.getBufferSize())
        .isEqualTo(4096);
  }

  @Test
  public void testStatusWithValidFilename() throws Throwable {
    Path p = new Path("file:///tmp/" + TESTPATH.getName());
    ObjectAssert<OpenFileSupport.OpenFileInformation> asst =
        assertFileInfo(prepareToOpenFile(
            params(FS_OPTION_OPENFILE_LENGTH, "32")
                .withStatus(status(p, 4096))));
    asst.extracting(f -> f.getStatus().getVersionId())
        .isEqualTo("version");
    asst.extracting(f -> f.getStatus().getEtag())
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
    ObjectAssert<OpenFileSupport.OpenFileInformation> asst =
        assertFileInfo(
            prepareToOpenFile(
                params(FS_OPTION_OPENFILE_LENGTH, "32")
                    .withStatus(
                        new S3ALocatedFileStatus(
                            status(p, 4096), null))));
    asst.extracting(f -> f.getStatus().getVersionId())
        .isEqualTo("version");
    asst.extracting(f -> f.getStatus().getEtag())
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

  /**
   * Prepare to open a file with the set of parameters.
   * @param parameters open a file
   * @return
   * @throws IOException
   */
  public OpenFileSupport.OpenFileInformation prepareToOpenFile(
      final OpenFileParameters parameters)
      throws IOException {
    return PREPARE.prepareToOpenFile(TESTPATH,
        parameters,
        IO_FILE_BUFFER_SIZE_DEFAULT
    );
  }

  /**
   * If a file length option is set, a file status
   * is created.
   */
  @Test
  public void testFileLength() throws Throwable {
    ObjectAssert<OpenFileSupport.OpenFileInformation> asst =
        assertFileInfo(prepareToOpenFile(
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
   * Verify that setting the split end sets the length.
   * By passing in a value greater than the size of an int,
   * the test verifies that the long is passed everywhere.
   */
  @Test
  public void testSplitEndSetsLength() throws Throwable {
    long bigFile = 2L ^ 34;
    assertOpenFile(FS_OPTION_OPENFILE_SPLIT_END, Long.toString(bigFile))
        .matches(p -> p.getSplitEnd() == bigFile, "split end")
        .matches(p -> p.getFileLength() == -1, "file length")
        .matches(p -> p.getStatus() == null, "status");
  }

  /**
   * Semantics of split and length. Split end can only be safely treated
   * as a hint unless the codec is known (how?) that it will never
   * read past it.
   */
  @Test
  public void testSplitEndAndLength() throws Throwable {
    long splitEnd = 256;
    long len = 8192;
    Configuration conf = conf(FS_OPTION_OPENFILE_LENGTH,
        Long.toString(len));
    conf.setLong(FS_OPTION_OPENFILE_SPLIT_END, splitEnd);
    conf.setLong(FS_OPTION_OPENFILE_SPLIT_START, 1024);
    Set<String> s = new HashSet<>();
    Collections.addAll(s,
        FS_OPTION_OPENFILE_SPLIT_START,
        FS_OPTION_OPENFILE_SPLIT_END,
        FS_OPTION_OPENFILE_LENGTH);
    assertFileInfo(prepareToOpenFile(
        new OpenFileParameters()
            .withMandatoryKeys(s)
            .withOptions(conf)))
        .matches(p -> p.getSplitStart() == 0, "split start")
        .matches(p -> p.getSplitEnd() == splitEnd, "split end")
        .matches(p -> p.getStatus().getLen() == len, "file length");
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
