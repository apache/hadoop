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

package org.apache.hadoop.fs.s3a.select;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import org.junit.Assume;
import org.junit.Test;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.fs.s3a.impl.ChangeDetectionPolicy;
import org.apache.hadoop.fs.s3a.impl.ChangeDetectionPolicy.Source;
import org.apache.hadoop.fs.s3a.s3guard.S3GuardTool;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.OperationDuration;
import org.apache.hadoop.util.ToolRunner;

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.disableFilesystemCaching;
import static org.apache.hadoop.fs.s3a.s3guard.S3GuardToolTestHelper.exec;
import static org.apache.hadoop.fs.s3a.select.ITestS3SelectLandsat.SELECT_NOTHING;
import static org.apache.hadoop.fs.s3a.select.ITestS3SelectLandsat.SELECT_SUNNY_ROWS_NO_LIMIT;
import static org.apache.hadoop.fs.s3a.select.SelectConstants.*;
import static org.apache.hadoop.fs.s3a.select.SelectTool.*;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_COMMAND_ARGUMENT_ERROR;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_NOT_FOUND;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_SERVICE_UNAVAILABLE;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_SUCCESS;
import static org.apache.hadoop.service.launcher.LauncherExitCodes.EXIT_USAGE;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test the S3 Select CLI through some operations against landsat
 * and files generated from it.
 */
public class ITestS3SelectCLI extends AbstractS3SelectTest {

  public static final int LINE_COUNT = 100;

  public static final String SELECT_EVERYTHING = "SELECT * FROM S3OBJECT s";

  private SelectTool selectTool;

  private Configuration selectConf;

  public static final String D = "-D";

  private File localFile;

  private String landsatSrc;

  @Override
  public void setup() throws Exception {
    super.setup();
    selectTool = new SelectTool(getConfiguration());
    selectConf = new Configuration(getConfiguration());
    localFile = getTempFilename();
    landsatSrc = getLandsatGZ().toString();
    ChangeDetectionPolicy changeDetectionPolicy =
        getLandsatFS().getChangeDetectionPolicy();
    Assume.assumeFalse("the standard landsat bucket doesn't have versioning",
        changeDetectionPolicy.getSource() == Source.VersionId
            && changeDetectionPolicy.isRequireVersion());
  }

  @Override
  public void teardown() throws Exception {
    super.teardown();
    if (localFile != null) {
      localFile.delete();
    }
  }

  /**
   * Expect a command to succeed.
   * @param message any extra text to include in the assertion error message
   * @param tool tool to run
   * @param args arguments to the command
   * @return the output of any successful run
   * @throws Exception failure
   */
  protected static String expectSuccess(
      String message,
      S3GuardTool tool,
      String... args) throws Exception {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    exec(EXIT_SUCCESS, message, tool, buf, args);
    return buf.toString();
  }

  /**
   * Run a S3GuardTool command from a varags list and the
   * configuration returned by {@code getConfiguration()}.
   * @param conf config to use
   * @param args argument list
   * @return the return code
   * @throws Exception any exception
   */
  protected int run(Configuration conf, S3GuardTool tool,
      String... args) throws Exception {
    return ToolRunner.run(conf, tool, args);
  }

  /**
   * Run a S3GuardTool command from a varags list, catch any raised
   * ExitException and verify the status code matches that expected.
   * @param status expected status code of the exception
   * @param conf config to use
   * @param args argument list
   * @throws Exception any exception
   */
  protected void runToFailure(int status, Configuration conf,
      String message,
      S3GuardTool tool, String... args)
      throws Exception {
    final ExitUtil.ExitException ex =
        intercept(ExitUtil.ExitException.class, message,
            () -> ToolRunner.run(conf, tool, args));
    if (ex.status != status) {
      throw ex;
    }

  }

  @Test
  public void testLandsatToFile() throws Throwable {
    describe("select part of the landsat to a file");
    int lineCount = LINE_COUNT;
    S3AFileSystem landsatFS =
        (S3AFileSystem) getLandsatGZ().getFileSystem(getConfiguration());
    S3ATestUtils.MetricDiff selectCount = new S3ATestUtils.MetricDiff(landsatFS,
        Statistic.OBJECT_SELECT_REQUESTS);

    run(selectConf, selectTool,
        D, v(CSV_OUTPUT_QUOTE_CHARACTER, "'"),
        D, v(CSV_OUTPUT_QUOTE_FIELDS, CSV_OUTPUT_QUOTE_FIELDS_AS_NEEEDED),
        "select",
        o(OPT_HEADER), CSV_HEADER_OPT_USE,
        o(OPT_COMPRESSION), COMPRESSION_OPT_GZIP,
        o(OPT_LIMIT), Integer.toString(lineCount),
        o(OPT_OUTPUT), localFile.toString(),
        landsatSrc,
        SELECT_SUNNY_ROWS_NO_LIMIT);
    List<String> lines = IOUtils.readLines(new FileInputStream(localFile),
        Charset.defaultCharset());
    LOG.info("Result from select:\n{}", lines.get(0));
    assertEquals(lineCount, lines.size());
    selectCount.assertDiffEquals("select count", 1);
    OperationDuration duration = selectTool.getSelectDuration();
    assertTrue("Select duration was not measured",
        duration.value() > 0);
  }

  private File getTempFilename() throws IOException {
    File dest = File.createTempFile("landat", ".csv");
    dest.delete();
    return dest;
  }

  @Test
  public void testLandsatToConsole() throws Throwable {
    describe("select part of the landsat to the console");
    // this verifies the input stream was actually closed
    S3ATestUtils.MetricDiff readOps = new S3ATestUtils.MetricDiff(
        getFileSystem(),
        Statistic.STREAM_READ_OPERATIONS_INCOMPLETE);
    run(selectConf, selectTool,
        D, v(CSV_OUTPUT_QUOTE_CHARACTER, "'"),
        D, v(CSV_OUTPUT_QUOTE_FIELDS, CSV_OUTPUT_QUOTE_FIELDS_ALWAYS),
        "select",
        o(OPT_HEADER), CSV_HEADER_OPT_USE,
        o(OPT_COMPRESSION), COMPRESSION_OPT_GZIP,
        o(OPT_LIMIT), Integer.toString(LINE_COUNT),
        landsatSrc,
        SELECT_SUNNY_ROWS_NO_LIMIT);
    assertEquals("Lines read and printed to console",
        LINE_COUNT, selectTool.getLinesRead());
    readOps.assertDiffEquals("Read operations are still considered active",
        0);  }

  @Test
  public void testSelectNothing() throws Throwable {
    describe("an empty select is not an error");
    run(selectConf, selectTool,
        "select",
        o(OPT_HEADER), CSV_HEADER_OPT_USE,
        o(OPT_COMPRESSION), COMPRESSION_OPT_GZIP,
        o(OPT_INPUTFORMAT), "csv",
        o(OPT_OUTPUTFORMAT), "csv",
        o(OPT_EXPECTED), "0",
        o(OPT_LIMIT), Integer.toString(LINE_COUNT),
        landsatSrc,
        SELECT_NOTHING);
    assertEquals("Lines read and printed to console",
        0, selectTool.getLinesRead());
  }

  @Test
  public void testLandsatToRemoteFile() throws Throwable {
    describe("select part of the landsat to a file");
    Path dest = path("testLandsatToRemoteFile.csv");
    run(selectConf, selectTool,
        D, v(CSV_OUTPUT_QUOTE_CHARACTER, "'"),
        D, v(CSV_OUTPUT_QUOTE_FIELDS, CSV_OUTPUT_QUOTE_FIELDS_ALWAYS),
        "select",
        o(OPT_HEADER), CSV_HEADER_OPT_USE,
        o(OPT_COMPRESSION), COMPRESSION_OPT_GZIP,
        o(OPT_LIMIT), Integer.toString(LINE_COUNT),
        o(OPT_OUTPUT), dest.toString(),
        landsatSrc,
        SELECT_SUNNY_ROWS_NO_LIMIT);
    FileStatus status = getFileSystem().getFileStatus(dest);
    assertEquals(
        "Mismatch between bytes selected and file len in " + status,
        selectTool.getBytesRead(), status.getLen());
    assertIsFile(dest);

    // now select on that
    Configuration conf = getConfiguration();
    SelectTool tool2 = new SelectTool(conf);
    run(conf, tool2,
        "select",
        o(OPT_HEADER), CSV_HEADER_OPT_NONE,
        dest.toString(),
        SELECT_EVERYTHING);
  }

  @Test
  public void testUsage() throws Throwable {
    runToFailure(EXIT_USAGE, getConfiguration(), TOO_FEW_ARGUMENTS,
        selectTool, "select");
  }

  @Test
  public void testRejectionOfNonS3FS() throws Throwable {
    File dest = getTempFilename();
    runToFailure(EXIT_SERVICE_UNAVAILABLE,
        getConfiguration(),
        WRONG_FILESYSTEM,
        selectTool, "select", dest.toString(),
        SELECT_EVERYTHING);
  }

  @Test
  public void testFailMissingFile() throws Throwable {
    Path dest = path("testFailMissingFile.csv");
    runToFailure(EXIT_NOT_FOUND,
        getConfiguration(),
        "",
        selectTool, "select", dest.toString(),
        SELECT_EVERYTHING);
  }

  @Test
  public void testS3SelectDisabled() throws Throwable {
    Configuration conf = getConfiguration();
    conf.setBoolean(FS_S3A_SELECT_ENABLED, false);
    disableFilesystemCaching(conf);
    runToFailure(EXIT_SERVICE_UNAVAILABLE,
        conf,
        SELECT_IS_DISABLED,
        selectTool, "select",
        o(OPT_HEADER), CSV_HEADER_OPT_USE,
        o(OPT_COMPRESSION), COMPRESSION_OPT_GZIP,
        o(OPT_LIMIT), Integer.toString(LINE_COUNT),
        landsatSrc,
        SELECT_SUNNY_ROWS_NO_LIMIT);
  }

  @Test
  public void testSelectBadLimit() throws Throwable {
    runToFailure(EXIT_USAGE,
        getConfiguration(),
        "",
        selectTool, "select",
        o(OPT_HEADER), CSV_HEADER_OPT_USE,
        o(OPT_COMPRESSION), COMPRESSION_OPT_GZIP,
        o(OPT_LIMIT), "-1",
        landsatSrc,
        SELECT_NOTHING);
  }

  @Test
  public void testSelectBadInputFormat() throws Throwable {
    runToFailure(EXIT_COMMAND_ARGUMENT_ERROR,
        getConfiguration(),
        "",
        selectTool, "select",
        o(OPT_HEADER), CSV_HEADER_OPT_USE,
        o(OPT_INPUTFORMAT), "pptx",
        o(OPT_COMPRESSION), COMPRESSION_OPT_GZIP,
        landsatSrc,
        SELECT_NOTHING);
  }

  @Test
  public void testSelectBadOutputFormat() throws Throwable {
    runToFailure(EXIT_COMMAND_ARGUMENT_ERROR,
        getConfiguration(),
        "",
        selectTool, "select",
        o(OPT_HEADER), CSV_HEADER_OPT_USE,
        o(OPT_OUTPUTFORMAT), "pptx",
        o(OPT_COMPRESSION), COMPRESSION_OPT_GZIP,
        landsatSrc,
        SELECT_NOTHING);
  }

  /**
   * Take an option and add the "-" prefix.
   * @param in input option
   * @return value for the tool args list.
   */
  private static String o(String in) {
    return "-" + in;
  }

  /**
   * Create the key=value bit of the -D key=value pair.
   * @param key key to set
   * @param value value to use
   * @return a string for the tool args list.
   */
  private static String v(String key, String value) {
    return checkNotNull(key) + "=" + checkNotNull(value);
  }

}
