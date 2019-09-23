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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.impl.AbstractFSBuilderImpl;
import org.apache.hadoop.fs.s3a.AWSBadRequestException;
import org.apache.hadoop.fs.s3a.AWSServiceIOException;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AInputStream;
import org.apache.hadoop.fs.s3a.S3AInstrumentation;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.util.DurationInfo;

import static org.apache.hadoop.fs.s3a.Constants.INPUT_FADVISE;
import static org.apache.hadoop.fs.s3a.Constants.INPUT_FADV_NORMAL;
import static org.apache.hadoop.fs.s3a.Constants.READAHEAD_RANGE;
import static org.apache.hadoop.fs.s3a.select.CsvFile.ALL_QUOTES;
import static org.apache.hadoop.fs.s3a.select.SelectBinding.expandBackslashChars;
import static org.apache.hadoop.fs.s3a.select.SelectConstants.*;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.test.LambdaTestUtils.interceptFuture;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

/**
 * Test the S3 Select feature with some basic SQL Commands.
 * Executed if the destination store declares its support for the feature.
 */
public class ITestS3Select extends AbstractS3SelectTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3Select.class);

  public static final String E_CAST_FAILED = "CastFailed";

  public static final String E_PARSE_INVALID_PATH_COMPONENT
      = "ParseInvalidPathComponent";

  public static final String E_INVALID_TABLE_ALIAS = "InvalidTableAlias";

  private Configuration selectConf;

  /** well formed CSV. */
  private Path csvPath;

  /** CSV file with fewer columns than expected, all fields parse badly. */
  private Path brokenCSV;

  @Override
  public void setup() throws Exception {
    super.setup();
    Assume.assumeTrue("S3 Select is not enabled",
        getFileSystem().hasCapability(S3_SELECT_CAPABILITY));
    csvPath = path(getMethodName() + ".csv");
    selectConf = new Configuration(false);
    selectConf.setBoolean(SELECT_ERRORS_INCLUDE_SQL, true);
    createStandardCsvFile(getFileSystem(), csvPath, ALL_QUOTES);
    // create the broken CSV file.
    brokenCSV = path("testParseBrokenCSVFile");
    createStandardCsvFile(
        getFileSystem(), brokenCSV,
        true,
        ALL_QUOTES,
        ALL_ROWS_COUNT,
        ALL_ROWS_COUNT,
        ",",
        "\n",
        "\"",
        csv -> csv
            .line("# comment")
            .row(ALL_QUOTES, "bad", "Tuesday", 0, "entry-bad", "yes", false));
  }

  @Override
  public void teardown() throws Exception {
    describe("teardown");
    try {
      if (csvPath != null) {
        getFileSystem().delete(csvPath, false);
      }
      if (brokenCSV != null) {
        getFileSystem().delete(brokenCSV, false);
      }
    } finally {
      super.teardown();
    }
  }

  @Test
  public void testCapabilityProbe() throws Throwable {

    // this should always hold true if we get past test setup
    assertTrue("Select is not available on " + getFileSystem(),
        isSelectAvailable(getFileSystem()));
  }

  @SuppressWarnings("NestedAssignment")
  @Test
  public void testReadWholeFileClassicAPI() throws Throwable {
    describe("create and read the whole file. Verifies setup working");
    int lines;
    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(
            getFileSystem().open(csvPath)))) {
      lines = 0;
      // seek to 0, which is what some input formats do
      String line;
      while ((line = reader.readLine()) != null) {
        lines++;
        LOG.info("{}", line);
      }
    }
    assertEquals("line count", ALL_ROWS_COUNT_WITH_HEADER, lines);
  }

  @Test
  public void testSelectWholeFileNoHeader() throws Throwable {
    describe("Select the entire file, expect all rows but the header");
    expectSelected(
        ALL_ROWS_COUNT,
        selectConf,
        CSV_HEADER_OPT_USE,
        "SELECT * FROM S3OBJECT");
  }

  @Test
  public void testSelectFirstColumnNoHeader() throws Throwable {
    describe("Select the entire file, expect all rows but the header");
    expectSelected(
        ALL_ROWS_COUNT_WITH_HEADER,
        selectConf,
        CSV_HEADER_OPT_NONE,
        "SELECT s._1 FROM S3OBJECT s");
  }

  @Test
  public void testSelectSelfNoHeader() throws Throwable {
    describe("Select the entire file, expect all rows but the header");
    expectSelected(
        ALL_ROWS_COUNT_WITH_HEADER,
        selectConf,
        CSV_HEADER_OPT_NONE,
        "SELECT s._1 FROM S3OBJECT s WHERE s._1 = s._1");
  }

  @Test
  public void testSelectSelfUseHeader() throws Throwable {
    describe("Select the entire file, expect all rows including the header");
    expectSelected(
        ALL_ROWS_COUNT,
        selectConf,
        CSV_HEADER_OPT_USE,
        "SELECT s.id FROM S3OBJECT s WHERE s.id = s.id");
  }

  @Test
  public void testSelectID2UseHeader() throws Throwable {
    describe("Select where ID=2; use the header");
    expectSelected(
        1,
        selectConf,
        CSV_HEADER_OPT_USE,
        "SELECT s.id FROM S3OBJECT s WHERE s.id = '2'");
  }

  @Test
  public void testSelectNoMatchingID() throws Throwable {
    describe("Select where there is no match; expect nothing back");
    expectSelected(
        0,
        selectConf,
        CSV_HEADER_OPT_USE,
        "SELECT s.id FROM S3OBJECT s WHERE s.id = '0x8000'");
  }

  @Test
  public void testSelectId1() throws Throwable {
    describe("Select the first element in the file");
    expectSelected(
        1,
        selectConf,
        CSV_HEADER_OPT_NONE,
        "SELECT * FROM S3OBJECT s WHERE s._1 = '1'",
        TRUE);
  }

  @Test
  public void testSelectEmptySQL() throws Throwable {
    describe("An empty SQL statement fails fast");
    FutureDataInputStreamBuilder builder = getFileSystem().openFile(
        csvPath)
        .must(SELECT_SQL, "");
    interceptFuture(IllegalArgumentException.class,
        SELECT_SQL,
        builder.build());
  }

  @Test
  public void testSelectEmptyFile() throws Throwable {
    describe("Select everything from an empty file");
    Path path = path("testSelectEmptyFile");
    S3AFileSystem fs = getFileSystem();
    ContractTestUtils.touch(fs, path);
    parseToLines(fs.openFile(path)
            .must(SELECT_SQL, SELECT_EVERYTHING)
            .build()
            .get(),
        0);
  }

  @Test
  public void testSelectEmptyFileWithConditions() throws Throwable {
    describe("Select everything from an empty file with a more complex SQL");
    Path path = path("testSelectEmptyFileWithConditions");
    S3AFileSystem fs = getFileSystem();
    ContractTestUtils.touch(fs, path);
    String sql = "SELECT * FROM S3OBJECT s WHERE s._1 = `TRUE`";
    CompletableFuture<FSDataInputStream> future = fs.openFile(path)
        .must(SELECT_SQL, sql).build();
    assertEquals("Not at the end of the file", -1, future.get().read());
  }

  @Test
  public void testSelectSeek() throws Throwable {
    describe("Verify forward seeks work, not others");

    // start: read in the full data through the initial select
    // this makes asserting that contents match possible
    Path path = csvPath;
    S3AFileSystem fs = getFileSystem();
    int len = (int) fs.getFileStatus(path).getLen();
    byte[] fullData = new byte[len];
    int actualLen;
    try (DurationInfo ignored =
             new DurationInfo(LOG, "Initial read of %s", path);
        FSDataInputStream sourceStream =
             select(fs, path,
                 selectConf,
                 SELECT_EVERYTHING)) {
      // read it in
      actualLen = IOUtils.read(sourceStream, fullData);
    }
    int seekRange = 20;

    try (FSDataInputStream seekStream =
             select(fs, path,
                 selectConf,
                 SELECT_EVERYTHING)) {
      SelectInputStream sis
          = (SelectInputStream) seekStream.getWrappedStream();
      S3AInstrumentation.InputStreamStatistics streamStats
          = sis.getS3AStreamStatistics();
      // lazy seek doesn't raise a problem here
      seekStream.seek(0);
      assertEquals("first byte read", fullData[0], seekStream.read());

      // and now the pos has moved, again, seek will be OK
      seekStream.seek(1);
      seekStream.seek(1);
      // but trying to seek elsewhere now fails
      PathIOException ex = intercept(PathIOException.class,
          SelectInputStream.SEEK_UNSUPPORTED,
          () -> seekStream.seek(0));
      LOG.info("Seek error is as expected", ex);
      // positioned reads from the current location work.
      byte[] buffer = new byte[1];
      long pos = seekStream.getPos();
      seekStream.readFully(pos, buffer);
      // but positioned backwards fail.
      intercept(PathIOException.class,
          SelectInputStream.SEEK_UNSUPPORTED,
          () -> seekStream.readFully(0, buffer));
      // the position has now moved on.
      assertPosition(seekStream, pos + 1);
      // so a seek to the old pos will fail
      intercept(PathIOException.class,
          SelectInputStream.SEEK_UNSUPPORTED,
          () -> seekStream.readFully(pos, buffer));

      // set the readahead to the default.
      // This verifies it reverts to the default.
      seekStream.setReadahead(null);
      assertEquals("Readahead in ",
          Constants.DEFAULT_READAHEAD_RANGE, sis.getReadahead());
      // forward seeks are implemented as 1+ skip
      long target = seekStream.getPos() + seekRange;
      seek(seekStream, target);
      assertPosition(seekStream, target);
      // now do a read and compare values
      assertEquals("byte at seek position",
          fullData[(int)seekStream.getPos()], seekStream.read());
      assertEquals("Seek bytes skipped in " + streamStats,
          seekRange, streamStats.bytesSkippedOnSeek);

      // try an invalid readahead range
      intercept(IllegalArgumentException.class,
          S3AInputStream.E_NEGATIVE_READAHEAD_VALUE,
          () -> seekStream.setReadahead(-1L));

      // do a slightly forward offset read
      int read = seekStream.read(seekStream.getPos() + 2, buffer, 0, 1);
      assertEquals(1, read);

      // final fun: seek way past the EOF
      logIntercepted(expectSeekEOF(seekStream, actualLen * 2));
      assertPosition(seekStream, actualLen);
      assertEquals(-1, seekStream.read());
      LOG.info("Seek statistics {}", streamStats);
      // this will return no, but not fail
      assertFalse("Failed to seek to new source in " + seekStream,
          seekStream.seekToNewSource(0));
      // and set the readahead to 0 to see that close path works
      seekStream.setReadahead(0L);
      // then do a manual close even though there's one in the try resource.
      // which will verify that a double close is harmless
      seekStream.close();
      LOG.info("Final stream state {}", sis);
    }
  }

  /**
   * Assert that a stream is in a specific position.
   * @param stream stream or other seekable.
   * @param pos expected position.
   * @throws IOException failure of the getPos() call.
   * @throws AssertionError mismatch between expected and actual.
   */
  private void assertPosition(Seekable stream, long pos)
      throws IOException {
    assertEquals("Wrong stream position in " + stream,
        pos, stream.getPos());
  }

  @Test
  public void testSelectOddLinesNoHeader() throws Throwable {
    describe("Select odd lines, ignoring the header");
    expectSelected(
        ODD_ROWS_COUNT,
        selectConf,
        CSV_HEADER_OPT_IGNORE,
        "SELECT * FROM S3OBJECT s WHERE s._5 = `TRUE`");
    // and do a quick check on the instrumentation
    long bytesRead = getFileSystem().getInstrumentation()
        .getCounterValue(Statistic.STREAM_SEEK_BYTES_READ);
    assertNotEquals("No bytes read count", 0, bytesRead);
  }

  @Test
  public void testSelectOddLinesHeader() throws Throwable {
    describe("Select the odd values");
    List<String> selected = expectSelected(
        ODD_ROWS_COUNT,
        selectConf,
        CSV_HEADER_OPT_USE,
        SELECT_ODD_ROWS);
    // the list includes odd values
    assertThat(selected, hasItem(ENTRY_0001));
    // but not the evens
    assertThat(selected, not(hasItem(ENTRY_0002)));
  }

  @Test
  public void testSelectOddLinesHeaderTSVOutput() throws Throwable {
    describe("Select the odd values with tab spaced output");
    selectConf.set(CSV_OUTPUT_FIELD_DELIMITER, "\t");
    selectConf.set(CSV_OUTPUT_QUOTE_CHARACTER, "'");
    selectConf.set(CSV_OUTPUT_QUOTE_FIELDS,
        CSV_OUTPUT_QUOTE_FIELDS_AS_NEEEDED);
    selectConf.set(CSV_OUTPUT_RECORD_DELIMITER, "\r");
    List<String> selected = expectSelected(
        ODD_ROWS_COUNT,
        selectConf,
        CSV_HEADER_OPT_USE,
        SELECT_ODD_ENTRIES_BOOL);
    // the list includes odd values
    String row1 = selected.get(0);

    // split that first line into columns: This is why TSV is better for code
    // to work with than CSV
    String[] columns = row1.split("\t", -1);
    assertEquals("Wrong column count from tab split line <" + row1 + ">",
        CSV_COLUMN_COUNT, columns.length);
    assertEquals("Wrong column value from tab split line <" + row1 + ">",
        "entry-0001", columns[3]);
  }

  @Test
  public void testSelectNotOperationHeader() throws Throwable {
    describe("Select the even values with a NOT call; quote the header name");
    List<String> selected = expectSelected(
        EVEN_ROWS_COUNT,
        selectConf,
        CSV_HEADER_OPT_USE,
        "SELECT s.name FROM S3OBJECT s WHERE NOT s.\"odd\" = %s",
        TRUE);
    // the list includes no odd values
    assertThat(selected, not(hasItem(ENTRY_0001)));
    // but has the evens
    assertThat(selected, hasItem(ENTRY_0002));
  }

  @Test
  public void testBackslashExpansion() throws Throwable {
    assertEquals("\t\r\n", expandBackslashChars("\t\r\n"));
    assertEquals("\t", expandBackslashChars("\\t"));
    assertEquals("\r", expandBackslashChars("\\r"));
    assertEquals("\r \n", expandBackslashChars("\\r \\n"));
    assertEquals("\\", expandBackslashChars("\\\\"));
  }

  /**
   * This is an expanded example for the documentation.
   * Also helps catch out unplanned changes to the configuration strings.
   */
  @Test
  public void testSelectFileExample() throws Throwable {
    describe("Select the entire file, expect all rows but the header");
    int len = (int) getFileSystem().getFileStatus(csvPath).getLen();
    FutureDataInputStreamBuilder builder =
        getFileSystem().openFile(csvPath)
            .must("fs.s3a.select.sql",
                SELECT_ODD_ENTRIES)
            .must("fs.s3a.select.input.format", "CSV")
            .must("fs.s3a.select.input.compression", "NONE")
            .must("fs.s3a.select.input.csv.header", "use")
            .must("fs.s3a.select.output.format", "CSV");

    CompletableFuture<FSDataInputStream> future = builder.build();
    try (FSDataInputStream select = future.get()) {
      // process the output
      byte[] bytes = new byte[len];
      int actual = select.read(bytes);
      LOG.info("file length is {}; length of selected data is {}",
          len, actual);
    }
  }

  /**
   * This is an expanded example for the documentation.
   * Also helps catch out unplanned changes to the configuration strings.
   */
  @Test
  public void testSelectUnsupportedInputFormat() throws Throwable {
    describe("Request an unsupported input format");
    FutureDataInputStreamBuilder builder = getFileSystem().openFile(csvPath)
        .must(SELECT_SQL, SELECT_ODD_ENTRIES)
        .must(SELECT_INPUT_FORMAT, "pptx");
    interceptFuture(IllegalArgumentException.class,
        "pptx",
        builder.build());
  }

  /**
   * Ask for an invalid output format.
   */
  @Test
  public void testSelectUnsupportedOutputFormat() throws Throwable {
    describe("Request a (currently) unsupported output format");
    FutureDataInputStreamBuilder builder = getFileSystem().openFile(csvPath)
        .must(SELECT_SQL, SELECT_ODD_ENTRIES)
        .must(SELECT_INPUT_FORMAT, "csv")
        .must(SELECT_OUTPUT_FORMAT, "json");
    interceptFuture(IllegalArgumentException.class,
        "json",
        builder.build());
  }

  /**
   *  Missing files fail lazy.
   */
  @Test
  public void testSelectMissingFile() throws Throwable {

    describe("Select a missing file, expect it to surface in the future");

    Path missing = path("missing");

    FutureDataInputStreamBuilder builder =
        getFileSystem().openFile(missing)
            .must(SELECT_SQL, SELECT_ODD_ENTRIES);

    interceptFuture(FileNotFoundException.class,
        "", builder.build());
  }

  @Test
  public void testSelectDirectoryFails() throws Throwable {
    describe("Verify that secondary select options are only valid on select"
        + " queries");
    S3AFileSystem fs = getFileSystem();
    Path dir = path("dir");
    // this will be an empty dir marker
    fs.mkdirs(dir);

    FutureDataInputStreamBuilder builder =
        getFileSystem().openFile(dir)
            .must(SELECT_SQL, SELECT_ODD_ENTRIES);
    interceptFuture(PathIOException.class,
        "", builder.build());

    // try the parent
    builder = getFileSystem().openFile(dir.getParent())
            .must(SELECT_SQL,
                SELECT_ODD_ENTRIES);
    interceptFuture(PathIOException.class,
        "", builder.build());
  }

  @Test
  public void testSelectRootFails() throws Throwable {
    describe("verify root dir selection is rejected");
    FutureDataInputStreamBuilder builder =
        getFileSystem().openFile(path("/"))
            .must(SELECT_SQL, SELECT_ODD_ENTRIES);
    interceptFuture(PathIOException.class,
        "", builder.build());
  }

  /**
   * Validate the abort logic.
   */
  @Test
  public void testCloseWithAbort() throws Throwable {
    describe("Close the stream with the readahead outstanding");
    S3ATestUtils.MetricDiff readOps = new S3ATestUtils.MetricDiff(
        getFileSystem(),
        Statistic.STREAM_READ_OPERATIONS_INCOMPLETE);
    selectConf.setInt(READAHEAD_RANGE, 2);

    FSDataInputStream stream = select(getFileSystem(), csvPath, selectConf,
        "SELECT * FROM S3OBJECT s");
    SelectInputStream sis = (SelectInputStream) stream.getWrappedStream();
    assertEquals("Readahead on " + sis, 2, sis.getReadahead());
    stream.setReadahead(1L);
    assertEquals("Readahead on " + sis, 1, sis.getReadahead());
    stream.read();
    S3AInstrumentation.InputStreamStatistics stats
        = sis.getS3AStreamStatistics();
    assertEquals("Read count in " + sis,
        1, stats.bytesRead);
    stream.close();
    assertEquals("Abort count in " + sis,
        1, stats.aborted);
    readOps.assertDiffEquals("Read operations are still considered active",
        0);
    intercept(PathIOException.class, FSExceptionMessages.STREAM_IS_CLOSED,
        () -> stream.read());
  }

  @Test
  public void testCloseWithNoAbort() throws Throwable {
    describe("Close the stream with the readahead outstandingV");
    FSDataInputStream stream = select(getFileSystem(), csvPath, selectConf,
        "SELECT * FROM S3OBJECT s");
    stream.setReadahead(0x1000L);
    SelectInputStream sis = (SelectInputStream) stream.getWrappedStream();
    S3AInstrumentation.InputStreamStatistics stats
        = sis.getS3AStreamStatistics();
    stream.close();
    assertEquals("Close count in " + sis, 1, stats.closed);
    assertEquals("Abort count in " + sis, 0, stats.aborted);
    assertTrue("No bytes read in close of " + sis, stats.bytesReadInClose > 0);
  }

  @Test
  public void testFileContextIntegration() throws Throwable {
    describe("Test that select works through FileContext");
    FileContext fc = S3ATestUtils.createTestFileContext(getConfiguration());
    selectConf.set(CSV_INPUT_HEADER, CSV_HEADER_OPT_USE);

    List<String> selected =
        verifySelectionCount(ODD_ROWS_COUNT, SELECT_ODD_ENTRIES_INT,
            parseToLines(
                select(fc, csvPath, selectConf, SELECT_ODD_ROWS)));
    // the list includes odd values
    assertThat(selected, hasItem(ENTRY_0001));
    // but not the evens
    assertThat(selected, not(hasItem(ENTRY_0002)));
  }

  @Test
  public void testSelectOptionsOnlyOnSelectCalls() throws Throwable {
    describe("Secondary select options are only valid on select"
        + " queries");
    String key = CSV_INPUT_HEADER;
    intercept(IllegalArgumentException.class, key,
        () -> getFileSystem().openFile(csvPath)
            .must(key, CSV_HEADER_OPT_USE).build());
  }

  @Test
  public void testSelectMustBeEnabled() throws Throwable {
    describe("Verify that the FS must have S3 select enabled.");
    Configuration conf = new Configuration(getFileSystem().getConf());
    conf.setBoolean(FS_S3A_SELECT_ENABLED, false);
    try (FileSystem fs2 = FileSystem.newInstance(csvPath.toUri(), conf)) {
      intercept(UnsupportedOperationException.class,
          SELECT_UNSUPPORTED,
          () -> {
            assertFalse("S3 Select Capability must be disabled on " + fs2,
                isSelectAvailable(fs2));
            return fs2.openFile(csvPath)
              .must(SELECT_SQL, SELECT_ODD_ROWS)
              .build();
          });
    }
  }

  @Test
  public void testSelectOptionsRejectedOnNormalOpen() throws Throwable {
    describe("Verify that a normal open fails on select must() options");
    intercept(IllegalArgumentException.class,
        AbstractFSBuilderImpl.UNKNOWN_MANDATORY_KEY,
        () -> getFileSystem().openFile(csvPath)
            .must(CSV_INPUT_HEADER, CSV_HEADER_OPT_USE)
            .build());
  }

  @Test
  public void testSelectOddRecordsWithHeader()
      throws Throwable {
    describe("work through a record reader");
    JobConf conf = createJobConf();
    inputMust(conf, CSV_INPUT_HEADER, CSV_HEADER_OPT_USE);
    expectRecordsRead(ODD_ROWS_COUNT, conf, SELECT_ODD_ENTRIES_DECIMAL);
  }

  @Test
  public void testSelectDatestampsConverted()
      throws Throwable {
    describe("timestamp conversion in record IIO");
    JobConf conf = createJobConf();
    inputMust(conf, CSV_INPUT_HEADER, CSV_HEADER_OPT_USE);
    inputMust(conf, CSV_OUTPUT_QUOTE_FIELDS,
        CSV_OUTPUT_QUOTE_FIELDS_AS_NEEEDED);
    String sql = SELECT_TO_DATE;
    List<String> records = expectRecordsRead(ALL_ROWS_COUNT, conf, sql);
    LOG.info("Result of {}\n{}", sql, prepareToPrint(records));
  }

  @Test
  public void testSelectNoMatch()
      throws Throwable {
    describe("when there's no match to a query, 0 records are returned,");
    JobConf conf = createJobConf();
    inputMust(conf, CSV_INPUT_HEADER, CSV_HEADER_OPT_USE);
    expectRecordsRead(0, conf,
        "SELECT * FROM S3OBJECT s WHERE s.odd = " + q("maybe"));
  }

  @Test
  public void testSelectOddRecordsIgnoreHeader()
      throws Throwable {
    describe("work through a record reader");
    JobConf conf = createJobConf();
    inputOpt(conf, CSV_INPUT_HEADER, CSV_HEADER_OPT_NONE);
    inputMust(conf, CSV_INPUT_HEADER, CSV_HEADER_OPT_IGNORE);
    expectRecordsRead(EVEN_ROWS_COUNT, conf,
        SELECT_EVEN_ROWS_NO_HEADER);
  }

  @Test
  public void testSelectRecordsUnknownMustOpt()
      throws Throwable {
    describe("verify reader key validation is remapped");
    JobConf conf = createJobConf();
    inputOpt(conf, CSV_INPUT_HEADER, CSV_HEADER_OPT_NONE);
    inputMust(conf, CSV_INPUT_HEADER + ".something", CSV_HEADER_OPT_IGNORE);
    intercept(IllegalArgumentException.class,
        AbstractFSBuilderImpl.UNKNOWN_MANDATORY_KEY,
        () -> readRecords(conf, SELECT_EVEN_ROWS_NO_HEADER));
  }

  @Test
  public void testSelectOddRecordsWithHeaderV1()
      throws Throwable {
    describe("work through a V1 record reader");
    JobConf conf = createJobConf();
    inputMust(conf, CSV_INPUT_HEADER, CSV_HEADER_OPT_USE);
    // using a double backslash here makes the string "\t" which will then
    // be parsed in the SelectBinding code as it if had come in on from an XML
    // entry
    inputMust(conf, CSV_OUTPUT_FIELD_DELIMITER, "\\t");
    inputMust(conf, CSV_OUTPUT_QUOTE_CHARACTER, "'");
    inputMust(conf, CSV_OUTPUT_QUOTE_FIELDS,
        CSV_OUTPUT_QUOTE_FIELDS_AS_NEEEDED);
    inputMust(conf, CSV_OUTPUT_RECORD_DELIMITER, "\n");
    verifySelectionCount(ODD_ROWS_COUNT,
        SELECT_ODD_ROWS,
        readRecordsV1(conf, SELECT_ODD_ROWS));
  }

  /**
   * Create a job conf for line reader tests.
   * This patches the job with the passthrough codec for
   * CSV files.
   * @return a job configuration
   */
  private JobConf createJobConf() {
    JobConf conf = new JobConf(getConfiguration());
    enablePassthroughCodec(conf, ".csv");
    return conf;
  }

  @Test
  public void testSelectOddRecordsIgnoreHeaderV1()
      throws Throwable {
    describe("work through a V1 record reader");
    JobConf conf = createJobConf();
    inputOpt(conf, CSV_INPUT_HEADER, CSV_HEADER_OPT_NONE);
    inputMust(conf, CSV_INPUT_HEADER, CSV_HEADER_OPT_IGNORE);
    inputMust(conf, INPUT_FADVISE, INPUT_FADV_NORMAL);
    inputMust(conf, SELECT_ERRORS_INCLUDE_SQL, "true");
    verifySelectionCount(EVEN_ROWS_COUNT,
        SELECT_EVEN_ROWS_NO_HEADER,
        readRecordsV1(conf, SELECT_EVEN_ROWS_NO_HEADER));
  }

  protected List<String> expectRecordsRead(final int expected,
      final JobConf conf,
      final String sql) throws Exception {
    return verifySelectionCount(expected, sql, readRecords(conf, sql));
  }

  /**
   * Reads lines through {@link LineRecordReader}, as if it were an MR
   * job.
   * @param conf jpb conf
   * @param sql sql to add to the configuration.
   * @return the selected lines.
   * @throws Exception failure
   */
  private List<String> readRecords(JobConf conf, String sql) throws Exception {
    return readRecords(conf,
        csvPath,
        sql,
        createLineRecordReader(),
        ALL_ROWS_COUNT_WITH_HEADER);
  }

  /**
   * Reads lines through a v1 LineRecordReader}.
   * @param conf jpb conf
   * @param sql sql to add to the configuration.
   * @return the selected lines.
   * @throws Exception failure
   */
  private List<String> readRecordsV1(JobConf conf, String sql)
      throws Exception {
    inputMust(conf, SELECT_SQL, sql);
    return super.readRecordsV1(conf,
        createLineRecordReaderV1(conf, csvPath),
        new LongWritable(),
        new Text(),
        ALL_ROWS_COUNT_WITH_HEADER);
  }

  /**
   * Issue a select call, expect the specific number of rows back.
   * Error text will include the SQL.
   * @param expected expected row count.
   * @param conf config for the select call.
   * @param header header option
   * @param sql template for a formatted SQL request.
   * @param args arguments for the formatted request.
   * @return the lines selected
   * @throws IOException failure
   */
  private List<String> expectSelected(
      final int expected,
      final Configuration conf,
      final String header,
      final String sql,
      final Object...args) throws Exception {
    conf.set(CSV_INPUT_HEADER, header);
    return verifySelectionCount(expected, sql(sql, args),
        selectCsvFile(conf, sql, args));
  }

  /**
   * Select from the CSV file.
   * @param conf config for the select call.
   * @param sql template for a formatted SQL request.
   * @param args arguments for the formatted request.
   * @return the lines selected
   * @throws IOException failure
   */
  private List<String> selectCsvFile(
      final Configuration conf,
      final String sql,
      final Object...args)
      throws Exception {

    return parseToLines(
        select(getFileSystem(), csvPath, conf, sql, args));
  }

  @Test
  public void testCommentsSkipped() throws Throwable {
    describe("Verify that comments are skipped");
    selectConf.set(CSV_INPUT_HEADER, CSV_HEADER_OPT_USE);

    List<String> lines = verifySelectionCount(
        ALL_ROWS_COUNT_WITH_HEADER,
        "select s.id",
        parseToLines(
            select(getFileSystem(), brokenCSV, selectConf,
                "SELECT * FROM S3OBJECT s")));
    LOG.info("\n{}", prepareToPrint(lines));
  }

  @Test
  public void testEmptyColumnsRegenerated() throws Throwable {
    describe("if you ask for a column but your row doesn't have it,"
        + " an empty column is inserted");
    selectConf.set(CSV_INPUT_HEADER, CSV_HEADER_OPT_USE);

    List<String> lines = verifySelectionCount(
        ALL_ROWS_COUNT_WITH_HEADER, "select s.oddrange",
        parseToLines(
            select(getFileSystem(), brokenCSV, selectConf,
                "SELECT s.oddrange FROM S3OBJECT s")));
    LOG.info("\n{}", prepareToPrint(lines));
    assertEquals("Final oddrange column is not regenerated empty",
        "\"\"", lines.get(lines.size() - 1));
  }

  @Test
  public void testIntCastFailure() throws Throwable {
    describe("Verify that int casts fail");
    expectSelectFailure(E_CAST_FAILED, SELECT_ODD_ENTRIES_INT);

  }

  @Test
  public void testSelectToDateParseFailure() throws Throwable {
    describe("Verify date parsing failure");
    expectSelectFailure(E_CAST_FAILED, SELECT_TO_DATE);
  }

  @Test
  public void testParseInvalidPathComponent() throws Throwable {
    describe("Verify bad SQL parseing");
    expectSelectFailure(E_PARSE_INVALID_PATH_COMPONENT,
        "SELECT * FROM S3OBJECT WHERE s.'oddf' = true");
  }

  @Test
  public void testSelectInvalidTableAlias() throws Throwable {
    describe("select with unknown column name");
    expectSelectFailure(E_INVALID_TABLE_ALIAS,
        "SELECT * FROM S3OBJECT WHERE s.\"oddf\" = 'true'");
  }

  @Test
  public void testSelectGeneratedAliases() throws Throwable {
    describe("select with a ._2 column when headers are enabled");
    expectSelectFailure(E_INVALID_TABLE_ALIAS,
        "SELECT * FROM S3OBJECT WHERE s._2 = 'true'");
  }

  /**
   * Expect select against the broken CSV file to fail with a specific
   * AWS exception error code.
   * If the is no failure, the results are included in the assertion raised.
   * @param expectedErrorCode error code in getErrorCode()
   * @param sql SQL to invoke
   * @return the exception, if it is as expected.
   * @throws Exception any other failure
   * @throws AssertionError when an exception is raised, but its error code
   * is different, or when no exception was raised.
   */
  protected AWSServiceIOException expectSelectFailure(
      String expectedErrorCode,
      String sql)
      throws Exception {
    selectConf.set(CSV_INPUT_HEADER, CSV_HEADER_OPT_USE);
    return verifyErrorCode(expectedErrorCode,
        intercept(AWSBadRequestException.class,
            () ->
                prepareToPrint(
                    parseToLines(
                        select(getFileSystem(), brokenCSV, selectConf, sql)
                    ))));

  }


  @Test
  public void testInputSplit()
      throws Throwable {
    describe("Verify that only a single file is used for splits");
    JobConf conf = new JobConf(getConfiguration());


    inputMust(conf, CSV_INPUT_HEADER, CSV_HEADER_OPT_USE);
    final Path input = csvPath;
    S3AFileSystem fs = getFileSystem();
    final Path output = path("testLandsatSelect")
        .makeQualified(fs.getUri(), fs.getWorkingDirectory());
    conf.set(FileInputFormat.INPUT_DIR, input.toString());
    conf.set(FileOutputFormat.OUTDIR, output.toString());

    final Job job = Job.getInstance(conf, "testInputSplit");
    JobContext jobCtx = new JobContextImpl(job.getConfiguration(),
        getTaskAttempt0().getJobID());

    TextInputFormat tif = new TextInputFormat();
    List<InputSplit> splits = tif.getSplits(jobCtx);
    assertThat("split count wrong", splits, hasSize(1));

  }

}
