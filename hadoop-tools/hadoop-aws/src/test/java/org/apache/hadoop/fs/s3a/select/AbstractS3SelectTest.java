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
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Scanner;
import java.util.function.Consumer;

import org.junit.Assume;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.s3a.AWSServiceIOException;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.commit.AbstractCommitITest;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.PassthroughCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.DurationInfo;

import static org.apache.hadoop.fs.impl.FutureIOSupport.awaitFuture;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getLandsatCSVPath;
import static org.apache.hadoop.fs.s3a.select.CsvFile.ALL_QUOTES;
import static org.apache.hadoop.fs.s3a.select.SelectConstants.*;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Superclass for S3 Select tests.
 * A lot of the work here goes into creating and querying a simple CSV test
 * format, with various datatypes which can be used in type-casting queries.
 * <pre>
 * 1  "ID": index of the row
 * 2  "date": date as ISO 8601
 * 3  "timestamp": timestamp in seconds of epoch
 * 4  "name", entry-$row
 * 5  "odd", odd/even as boolean. True means odd,
 * 6  "oddint", odd/even as int : 1 for odd, 0 for even
 * 7  "oddrange": odd/even as 1 for odd, -1 for even
 * </pre>
 */
public abstract class AbstractS3SelectTest extends AbstractS3ATestBase {

  /**
   * Number of columns in the CSV file: {@value}.
   */
  public static final int CSV_COLUMN_COUNT = 7;

  protected static final String TRUE = q("TRUE");

  protected static final String FALSE = q("FALSE");

  public static final String SELECT_EVERYTHING = "SELECT * FROM S3OBJECT s";

  public static final String SELECT_EVEN_ROWS_NO_HEADER =
      "SELECT * FROM S3OBJECT s WHERE s._5 = " + TRUE;
  public static final String SELECT_ODD_ROWS
      = "SELECT s.name FROM S3OBJECT s WHERE s.odd = " + TRUE;

  public static final String SELECT_ODD_ENTRIES
      = "SELECT * FROM S3OBJECT s WHERE s.odd = `TRUE`";

  public static final String SELECT_ODD_ENTRIES_BOOL
      = "SELECT * FROM S3OBJECT s WHERE CAST(s.odd AS BOOL) = TRUE";

  public static final String SELECT_ODD_ENTRIES_INT
      = "SELECT * FROM S3OBJECT s WHERE CAST(s.\"oddint\" AS INT) = 1";

  public static final String SELECT_ODD_ENTRIES_DECIMAL
      = "SELECT * FROM S3OBJECT s WHERE CAST(s.\"oddint\" AS DECIMAL) = 1";

  /**
   * Playing with timestamps: {@value}.
   */
  public static final String SELECT_TO_DATE
      = "SELECT\n"
      + "CAST(s.\"date\" AS TIMESTAMP)\n"
      + "FROM S3OBJECT s";


  /**
   * How many rows are being generated.
   */
  protected static final int ALL_ROWS_COUNT = 10;

  /**
   * Row count of all rows + header.
   */
  protected static final int ALL_ROWS_COUNT_WITH_HEADER = ALL_ROWS_COUNT + 1;

  /**
   * Number of odd rows expected: {@value}.
   */
  protected static final int ODD_ROWS_COUNT = ALL_ROWS_COUNT / 2;

  /**
   * Number of even rows expected: {@value}.
   * This is the same as the odd row count; it's separate just to
   * be consistent on tests which select even results.
   */
  protected static final int EVEN_ROWS_COUNT = ODD_ROWS_COUNT;

  protected static final String ENTRY_0001 = "\"entry-0001\"";

  protected static final String ENTRY_0002 = "\"entry-0002\"";

  /**
   * Path to the landsat csv.gz file.
   */
  private Path landsatGZ;

  /**
   * The filesystem with the landsat data.
   */
  private S3AFileSystem landsatFS;


  // A random task attempt id for testing.
  private String attempt0;

  private TaskAttemptID taskAttempt0;

  private String jobId;

  /**
   * Base CSV file is headers.
   * <pre>
   * 1  "ID": index of the row
   * 2  "date": date as Date.toString
   * 3  "timestamp": timestamp in seconds of epoch
   * 4  "name", entry-$row
   * 5  "odd", odd/even as boolean
   * 6  "oddint", odd/even as int : 1 for odd, 0 for even
   * 7  "oddrange": odd/even as 1 for odd, -1 for even
   * </pre>
   * @param fs filesystem
   * @param path path to write
   * @param header should the standard header be printed?
   * @param quoteHeaderPolicy what the header quote policy is.
   * @param quoteRowPolicy what the row quote policy is.
   * @param rows number of rows
   * @param separator column separator
   * @param eol end of line characters
   * @param quote quote char
   * @param footer callback to run after the main CSV file is written
   * @throws IOException IO failure.
   */
  public static void createStandardCsvFile(
      final FileSystem fs,
      final Path path,
      final boolean header,
      final long quoteHeaderPolicy,
      final long quoteRowPolicy,
      final int rows,
      final String separator,
      final String eol,
      final String quote,
      final Consumer<CsvFile> footer) throws IOException {
    try (CsvFile csv = new CsvFile(fs,
        path,
        true,
        separator,
        eol,
        quote)) {

      if (header) {
        writeStandardHeader(csv, quoteHeaderPolicy);
      }
      DateTimeFormatter formatter
          = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
      ZonedDateTime timestamp = ZonedDateTime.now();
      Duration duration = Duration.ofHours(20);
      // loop is at 1 for use in counters and flags
      for (int i = 1; i <= rows; i++) {
        // flip the odd flags
        boolean odd = (i & 1) == 1;
        // and move the timestamp back
        timestamp = timestamp.minus(duration);
        csv.row(quoteRowPolicy,
            i,
            timestamp.format(formatter),
            timestamp.toEpochSecond(),
            String.format("entry-%04d", i),
            odd ? "TRUE" : "FALSE",
            odd ? 1 : 0,
            odd ? 1 : -1
        );
      }
      // write the footer
      footer.accept(csv);
    }
  }

  /**
   * Write out the standard header to a CSV file.
   * @param csv CSV file to use.
   * @param quoteHeaderPolicy quote policy.
   * @return the input file.
   * @throws IOException failure to write.
   */
  private static CsvFile writeStandardHeader(final CsvFile csv,
      final long quoteHeaderPolicy) throws IOException {
    return csv.row(quoteHeaderPolicy,
        "id",
        "date",
        "timestamp",
        "name",
        "odd",
        "oddint",
        "oddrange");
  }

  /**
   * Verify that an exception has a specific error code.
   * if not: an assertion is raised containing the original value.
   * @param code expected code.
   * @param ex exception caught
   * @throws AssertionError on a mismatch
   */
  protected static AWSServiceIOException verifyErrorCode(final String code,
      final AWSServiceIOException ex) {
    logIntercepted(ex);
    if (!code.equals(ex.getErrorCode())) {
      throw new AssertionError("Expected Error code" + code
          + " actual " + ex.getErrorCode(),
          ex);
    }
    return ex;
  }

  /**
   * Probe for a filesystem instance supporting S3 Select.
   * @param filesystem filesystem
   * @return true iff the filesystem supports S3 Select.
   */
  boolean isSelectAvailable(final FileSystem filesystem) {
    return filesystem instanceof StreamCapabilities
        && ((StreamCapabilities) filesystem)
        .hasCapability(S3_SELECT_CAPABILITY);
  }

  /**
   * Setup: requires select to be available.
   */
  @Override
  public void setup() throws Exception {
    super.setup();
    Assume.assumeTrue("S3 Select is not enabled on "
            + getFileSystem().getUri(),
        isSelectAvailable(getFileSystem()));
    Configuration conf = getConfiguration();
    landsatGZ = getLandsatCSVPath(conf);
    landsatFS = (S3AFileSystem) landsatGZ.getFileSystem(conf);
    Assume.assumeTrue("S3 Select is not enabled on " + landsatFS.getUri(),
        isSelectAvailable(landsatFS));
    // create some job info
    jobId = AbstractCommitITest.randomJobId();
    attempt0 = "attempt_" + jobId + "_m_000000_0";
    taskAttempt0 = TaskAttemptID.forName(attempt0);
  }

  /**
   * Build the SQL statement, using String.Format rules.
   * @param template template
   * @param args arguments for the template
   * @return the template to use
   */
  protected static String sql(
      final String template,
      final Object... args) {
    return args.length > 0 ? String.format(template, args) : template;
  }

  /**
   * Quote a constant with the SQL quote logic.
   * @param c constant
   * @return quoted constant
   */
  protected static String q(String c) {
    return '\'' + c + '\'';
  }

  /**
   * Select from a source file.
   * @param fileSystem FS.
   * @param source source file.
   * @param conf config for the select call.
   * @param sql template for a formatted SQL request.
   * @param args arguments for the formatted request.
   * @return the input stream.
   * @throws IOException failure
   */
  protected FSDataInputStream select(
      final FileSystem fileSystem,
      final Path source,
      final Configuration conf,
      final String sql,
      final Object... args)
      throws IOException {
    String expression = sql(sql, args);
    describe("Execution Select call: %s", expression);
    FutureDataInputStreamBuilder builder =
        fileSystem.openFile(source)
            .must(SELECT_SQL, expression);
    // propagate all known options
    for (String key : InternalSelectConstants.SELECT_OPTIONS) {
      String value = conf.get(key);
      if (value != null) {
        builder.must(key, value);
      }
    }
    return awaitFuture(builder.build());
  }

  /**
   * Select from a source file via the file context API.
   * @param fc file context
   * @param source source file.
   * @param conf config for the select call.
   * @param sql template for a formatted SQL request.
   * @param args arguments for the formatted request.
   * @return the input stream.
   * @throws IOException failure
   */
  protected FSDataInputStream select(
      final FileContext fc,
      final Path source,
      final Configuration conf,
      final String sql,
      final Object... args)
      throws IOException {
    String expression = sql(sql, args);
    describe("Execution Select call: %s", expression);
    FutureDataInputStreamBuilder builder = fc.openFile(source)
        .must(SELECT_SQL, expression);
    // propagate all known options
    InternalSelectConstants.SELECT_OPTIONS.forEach((key) ->
        Optional.ofNullable(conf.get(key))
            .map((v) -> builder.must(key, v)));
    return awaitFuture(builder.build());
  }

  /**
   * Parse a selection to lines; log at info.
   * @param selection selection input
   * @return a list of lines.
   * @throws IOException if raised during the read.
   */
  protected List<String> parseToLines(final FSDataInputStream selection)
      throws IOException {
    return parseToLines(selection, getMaxLines());
  }

  /**
   * Enable the passthrough codec for a job, with the given extension.
   * @param conf configuration to update
   * @param extension extension to use
   */
  protected void enablePassthroughCodec(final Configuration conf,
      final String extension) {
    conf.set(CommonConfigurationKeys.IO_COMPRESSION_CODECS_KEY,
        PassthroughCodec.CLASSNAME);
    conf.set(PassthroughCodec.OPT_EXTENSION, extension);
  }

  /**
   * Override if a test suite is likely to ever return more lines.
   * @return the max number for parseToLines/1
   */
  protected int getMaxLines() {
    return 100;
  }

  /**
   * Parse a selection to lines; log at info.
   * @param selection selection input
   * @param maxLines maximum number of lines.
   * @return a list of lines.
   * @throws IOException if raised during the read.
   */
  protected List<String> parseToLines(final FSDataInputStream selection,
      int maxLines)
      throws IOException {
    List<String> result = new ArrayList<>();
    String stats;
    // the scanner assumes that any IOE => EOF; we don't want
    // that and so will check afterwards.
    try (Scanner scanner = new Scanner(
        new BufferedReader(new InputStreamReader(selection)))) {
      scanner.useDelimiter(CSV_INPUT_RECORD_DELIMITER_DEFAULT);
      while (maxLines > 0) {
        try {
          String l = scanner.nextLine();
          LOG.info("{}", l);
          result.add(l);
          maxLines--;
        } catch (NoSuchElementException e) {
          // EOL or an error
          break;
        }
      }
      stats = selection.toString();
      describe("Result line count: %s\nStatistics\n%s",
          result.size(), stats);
      // look for any raised error.
      IOException ioe = scanner.ioException();
      if (ioe != null && !(ioe instanceof EOFException)) {
        throw ioe;
      }
    }
    return result;
  }

  /**
   * Verify the selection count; return the original list.
   * If there's a mismatch, the whole list is logged at error, then
   * an assertion raised.
   * @param expected expected value.
   * @param expression expression -for error messages.
   * @param selection selected result.
   * @return the input list.
   */
  protected List<String> verifySelectionCount(
      final int expected,
      final String expression,
      final List<String> selection) {
    return verifySelectionCount(expected, expected, expression, selection);
  }

  /**
   * Verify the selection count is within a given range;
   * return the original list.
   * If there's a mismatch, the whole list is logged at error, then
   * an assertion raised.
   * @param min min value (exclusive).
   * @param max max value (exclusive). If -1: no maximum.
   * @param expression expression -for error messages.
   * @param selection selected result.
   * @return the input list.
   */
  protected List<String> verifySelectionCount(
      final int min,
      final int max,
      final String expression,
      final List<String> selection) {
    int size = selection.size();
    if (size < min || (max > -1 && size > max)) {
      // mismatch: log and then fail
      String listing = prepareToPrint(selection);
      LOG.error("\n{} => \n{}", expression, listing);
      fail("row count from select call " + expression
          + " is out of range " + min + " to " + max
          + ": " + size
          + " \n" + listing);
    }
    return selection;
  }

  /**
   * Do whatever is needed to prepare a string for logging.
   * @param selection selection
   * @return something printable.
   */
  protected String prepareToPrint(final List<String> selection) {
    return String.join("\n", selection);
  }

  /**
   * Create "the standard" CSV file with the default row count.
   * @param fs filesystem
   * @param path path to write
   * @param quoteRowPolicy what the row quote policy is.
   * @throws IOException IO failure.
   */
  protected void createStandardCsvFile(
      final FileSystem fs,
      final Path path,
      final long quoteRowPolicy)
      throws IOException {
    createStandardCsvFile(
        fs, path,
        true,
        ALL_QUOTES,
        quoteRowPolicy,
        ALL_ROWS_COUNT,
        ",",
        "\n",
        "\"",
        c -> {});
  }

  /**
   * Set an MR Job input option.
   * @param conf configuration
   * @param key key to set
   * @param val value
   */
  void inputOpt(Configuration conf, String key, String val) {
    conf.set(MRJobConfig.INPUT_FILE_OPTION_PREFIX + key, val);
  }

  /**
   * Set a mandatory MR Job input option.
   * @param conf configuration
   * @param key key to set
   * @param val value
   */
  void inputMust(Configuration conf, String key, String val) {
    conf.set(MRJobConfig.INPUT_FILE_MANDATORY_PREFIX + key,
        val);
  }

  /**
   * Reads lines through a v2 RecordReader, as if it were part of a
   * MRv2 job.
   * @param conf job conf
   * @param path path to query
   * @param sql sql to add to the configuration.
   * @param initialCapacity capacity of the read
   * @param reader reader: this is closed after the read
   * @return the selected lines.
   * @throws Exception failure
   */
  protected List<String> readRecords(JobConf conf,
      Path path,
      String sql,
      RecordReader<?, ?> reader,
      int initialCapacity) throws Exception {

    inputMust(conf, SELECT_SQL, sql);
    List<String> lines = new ArrayList<>(initialCapacity);
    try {
      reader.initialize(
          createSplit(conf, path),
          createTaskAttemptContext(conf));
      while (reader.nextKeyValue()) {
        lines.add(reader.getCurrentValue().toString());
      }
    } finally {
      reader.close();
    }
    return lines;
  }
  /**
   * Reads lines through a v1 RecordReader, as if it were part of a
   * MRv1 job.
   * @param conf job conf
   * @param reader reader: this is closed after the read
   * @param initialCapacity capacity of the read
   * @return the selected lines.
   * @throws Exception failure
   */
  protected <K, V> List<String> readRecordsV1(JobConf conf,
      org.apache.hadoop.mapred.RecordReader<K, V> reader,
      K key,
      V value,
      int initialCapacity) throws Exception {
    List<String> lines = new ArrayList<>(initialCapacity);
    try {
      while (reader.next(key, value)) {
        lines.add(value.toString());
      }
    } finally {
      reader.close();
    }
    return lines;
  }

  /**
   * Create a task attempt context for a job, creating a random JobID to
   * do this.
   * @param conf job configuration.
   * @return a new task attempt context containing the job conf
   * @throws Exception failure.
   */
  protected TaskAttemptContext createTaskAttemptContext(final JobConf conf)
      throws Exception {
    String id = AbstractCommitITest.randomJobId();
    return new TaskAttemptContextImpl(conf,
        TaskAttemptID.forName("attempt_" + id + "_m_000000_0"));
  }

  /**
   * Create an MRv2 file input split.
   * @param conf job configuration
   * @param path path to file
   * @return the split
   * @throws IOException problems reading the file.
   */
  protected FileSplit createSplit(final JobConf conf, final Path path)
      throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    FileStatus status = fs.getFileStatus(path);
    return new FileSplit(path, 0, status.getLen(),
        new String[]{"localhost"});
  }

  /**
   * Create an MRv1 file input split.
   * @param conf job configuration
   * @param path path to file
   * @return the split
   * @throws IOException problems reading the file.
   */
  protected org.apache.hadoop.mapred.FileSplit
      createSplitV1(final JobConf conf, final Path path)
      throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    FileStatus status = fs.getFileStatus(path);
    return new org.apache.hadoop.mapred.FileSplit(path, 0, status.getLen(),
        new String[]{"localhost"});
  }

  /**
   * Create a v2 line record reader expecting newlines as the EOL marker.
   * @return a reader
   */
  protected RecordReader<LongWritable, Text> createLineRecordReader() {
    return new LineRecordReader(new byte[]{'\n'});
  }

  /**
   * Create a v1 line record reader.
   * @return a reader
   */
  protected org.apache.hadoop.mapred.RecordReader<LongWritable, Text>
      createLineRecordReaderV1(
        final JobConf conf,
        final Path path) throws IOException {
    return new org.apache.hadoop.mapred.LineRecordReader(
        conf, createSplitV1(conf, path));
  }

  /**
   * Get the path to the landsat file.
   * @return the landsat CSV.GZ path.
   */
  protected Path getLandsatGZ() {
    return landsatGZ;
  }

  /**
   * Get the filesystem for the landsat file.
   * @return the landsat FS.
   */
  protected S3AFileSystem getLandsatFS() {
    return landsatFS;
  }

  /**
   * Perform a seek: log duration of the operation.
   * @param stream stream to seek.
   * @param target target position.
   * @throws IOException on an error
   */
  protected void seek(final FSDataInputStream stream, final long target)
      throws IOException {
    try(DurationInfo ignored =
            new DurationInfo(LOG, "Seek to %d", target)) {
      stream.seek(target);
    }
  }

  /**
   * Execute a seek so far past the EOF that it will be rejected.
   * If the seek did not fail, the exception raised includes the toString()
   * value of the stream.
   * @param seekStream stream to seek in.
   * @param newpos new position
   * @return the EOF Exception raised.
   * @throws Exception any other exception.
   */
  protected EOFException expectSeekEOF(final FSDataInputStream seekStream,
      final int newpos) throws Exception {
    return intercept(EOFException.class,
        () -> {
          seek(seekStream, newpos);
          // return this for the test failure reports.
          return "Stream after seek to " + newpos + ": " + seekStream;
        });
  }

  public String getAttempt0() {
    return attempt0;
  }

  public TaskAttemptID getTaskAttempt0() {
    return taskAttempt0;
  }

  public String getJobId() {
    return jobId;
  }

  /**
   * Logs intercepted exceptions.
   * This generates the stack traces for the documentation.
   * @param ex exception
   * @return the exception passed in (for chaining)
   */
  protected static <T extends Exception> T logIntercepted(T ex) {
    LOG.info("Intercepted Exception is ", ex);
    return ex;
  }
}
