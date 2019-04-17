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

import java.io.IOException;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AInstrumentation;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.DurationInfo;

import static org.apache.hadoop.fs.s3a.S3ATestUtils.assume;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestPropertyBool;
import static org.apache.hadoop.fs.s3a.scale.S3AScaleTestBase._1KB;
import static org.apache.hadoop.fs.s3a.scale.S3AScaleTestBase._1MB;
import static org.apache.hadoop.fs.s3a.select.SelectConstants.*;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;

/**
 * Test the S3 Select feature with the Landsat dataset.
 *
 * This helps explore larger datasets, compression and the like.
 *
 * This suite is only executed if the destination store declares its support for
 * the feature and the test CSV file configuration option points to the
 * standard landsat GZip file. That's because these tests require the specific
 * format of the landsat file.
 *
 * Normally working with the landsat file is a scale test.
 * Here, because of the select operations, there's a lot less data
 * to download.
 * For this to work: write aggressive select calls: filtering, using LIMIT
 * and projecting down to a few columns.
 *
 * For the structure, see
 * <a href="https://docs.opendata.aws/landsat-pds/readme.html">Landsat on AWS</a>
 *
 * <code>
 *   entityId: String         LC80101172015002LGN00
 *   acquisitionDate: String  2015-01-02 15:49:05.571384
 *   cloudCover: Float (possibly -ve) 80.81
 *   processingLevel: String  L1GT
 *   path: Int                10
 *   row:  Int                117
 *   min_lat: Float           -79.09923
 *   min_lon: Float           -139.66082
 *   max_lat: Float           -77.7544
 *   max_lon: Float           125.09297
 *   download_url: HTTPS URL https://s3-us-west-2.amazonaws.com/landsat-pds/L8/010/117/LC80101172015002LGN00/index.html
 * </code>
 * Ranges
 * <ol>
 *   <li>Latitude should range in -180 <= lat <= 180</li>
 *   <li>Longitude in 0 <= lon <= 360</li>
 *   <li>Standard Greenwich Meridian (not the french one which still surfaces)</li>
 *   <li>Cloud cover <i>Should</i> be 0-100, but there are some negative ones.</li>
 * </ol>
 *
 * Head of the file:
 * <code>
 entityId,acquisitionDate,cloudCover,processingLevel,path,row,min_lat,min_lon,max_lat,max_lon,download_url
 * LC80101172015002LGN00,2015-01-02 15:49:05.571384,80.81,L1GT,10,117,-79.09923,-139.66082,-77.7544,-125.09297,https://s3-us-west-2.amazonaws.com/landsat-pds/L8/010/117/LC80101172015002LGN00/index.html
 * LC80260392015002LGN00,2015-01-02 16:56:51.399666,90.84,L1GT,26,39,29.23106,-97.48576,31.36421,-95.16029,https://s3-us-west-2.amazonaws.com/landsat-pds/L8/026/039/LC80260392015002LGN00/index.html
 * LC82270742015002LGN00,2015-01-02 13:53:02.047000,83.44,L1GT,227,74,-21.28598,-59.27736,-19.17398,-57.07423,https://s3-us-west-2.amazonaws.com/landsat-pds/L8/227/074/LC82270742015002LGN00/index.html
 * LC82270732015002LGN00,2015-01-02 13:52:38.110317,52.29,L1T,227,73,-19.84365,-58.93258,-17.73324,-56.74692,https://s3-us-west-2.amazonaws.com/landsat-pds/L8/227/073/LC82270732015002LGN00/index.html
 * </code>
 *
 * For the Curious this is the Scala/Spark declaration of the schema.
 * <code>
 *   def addLandsatColumns(csv: DataFrame): DataFrame = {
 *     csv
 *       .withColumnRenamed("entityId", "id")
 *       .withColumn("acquisitionDate",
 *         csv.col("acquisitionDate").cast(TimestampType))
 *       .withColumn("cloudCover", csv.col("cloudCover").cast(DoubleType))
 *       .withColumn("path", csv.col("path").cast(IntegerType))
 *       .withColumn("row", csv.col("row").cast(IntegerType))
 *       .withColumn("min_lat", csv.col("min_lat").cast(DoubleType))
 *       .withColumn("min_lon", csv.col("min_lon").cast(DoubleType))
 *       .withColumn("max_lat", csv.col("max_lat").cast(DoubleType))
 *       .withColumn("max_lon", csv.col("max_lon").cast(DoubleType))
 *       .withColumn("year",
 *         year(col("acquisitionDate")))
 *       .withColumn("month",
 *         month(col("acquisitionDate")))
 *       .withColumn("day",
 *         month(col("acquisitionDate")))
 *   }
 * </code>
 */
public class ITestS3SelectLandsat extends AbstractS3SelectTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3SelectLandsat.class);

  private JobConf selectConf;

  /**
   * Normal limit for select operations.
   * Value: {@value}.
   */
  public static final int SELECT_LIMIT = 250;

  /**
   * And that select limit as a limit string.
   */
  public static final String LIMITED = " LIMIT " + SELECT_LIMIT;

  /**
   * Select days with 100% cloud cover, limited to {@link #SELECT_LIMIT}.
   * Value: {@value}.
   */
  public static final String SELECT_ENTITY_ID_ALL_CLOUDS =
      "SELECT\n"
          + "s.entityId from\n"
          + "S3OBJECT s WHERE\n"
          + "s.\"cloudCover\" = '100.0'\n"
          + LIMITED;

  /**
   * Select sunny days. There's no limit on the returned values, so
   * set one except for a scale test.
   * Value: {@value}.
   */
  public static final String SELECT_SUNNY_ROWS_NO_LIMIT
      = "SELECT * FROM S3OBJECT s WHERE s.cloudCover = '0.0'";

  /**
   * A Select call which returns nothing, always.
   * Value: {@value}.
   */
  public static final String SELECT_NOTHING
      = "SELECT * FROM S3OBJECT s WHERE s.cloudCover = 'sunny'";

  /**
   * Select the processing level; no limit.
   * Value: {@value}.
   */
  public static final String SELECT_PROCESSING_LEVEL_NO_LIMIT =
      "SELECT\n"
          + "s.processingLevel from\n"
          + "S3OBJECT s";

  @Override
  public void setup() throws Exception {
    super.setup();

    selectConf = new JobConf(false);
    // file is compressed.
    selectConf.set(SELECT_INPUT_COMPRESSION, COMPRESSION_OPT_GZIP);
    // and has a header
    selectConf.set(CSV_INPUT_HEADER, CSV_HEADER_OPT_USE);
    selectConf.setBoolean(SELECT_ERRORS_INCLUDE_SQL, true);
    inputMust(selectConf, CSV_INPUT_HEADER, CSV_HEADER_OPT_USE);
    inputMust(selectConf, SELECT_INPUT_FORMAT, SELECT_FORMAT_CSV);
    inputMust(selectConf, SELECT_OUTPUT_FORMAT, SELECT_FORMAT_CSV);
    inputMust(selectConf, SELECT_INPUT_COMPRESSION, COMPRESSION_OPT_GZIP);
    // disable the gzip codec, so that the record readers do not
    // get confused
    enablePassthroughCodec(selectConf, ".gz");
  }

  protected int getMaxLines() {
    return SELECT_LIMIT * 2;
  }

  @Test
  public void testSelectCloudcoverIgnoreHeader() throws Throwable {
    describe("select ignoring the header");
    selectConf.set(CSV_INPUT_HEADER, CSV_HEADER_OPT_IGNORE);
    String sql = "SELECT\n"
        + "* from\n"
        + "S3OBJECT s WHERE\n"
        + "s._3 = '0.0'\n"
        + LIMITED;
    List<String> list = selectLandsatFile(selectConf, sql);
    LOG.info("Line count: {}", list.size());
    verifySelectionCount(1, SELECT_LIMIT, sql, list);
  }

  @Test
  public void testSelectCloudcoverUseHeader() throws Throwable {
    describe("select 100% cover using the header, "
        + "+ verify projection and incrementing select statistics");
    S3ATestUtils.MetricDiff selectCount = new S3ATestUtils.MetricDiff(
        getLandsatFS(),
        Statistic.OBJECT_SELECT_REQUESTS);

    List<String> list = selectLandsatFile(selectConf,
        SELECT_ENTITY_ID_ALL_CLOUDS);
    LOG.info("Line count: {}", list.size());
    verifySelectionCount(1, SELECT_LIMIT, SELECT_ENTITY_ID_ALL_CLOUDS, list);
    String line1 = list.get(0);
    assertThat("no column filtering from " + SELECT_ENTITY_ID_ALL_CLOUDS,
        line1, not(containsString("100.0")));
    selectCount.assertDiffEquals("select count", 1);
  }

  @Test
  public void testFileContextIntegration() throws Throwable {
    describe("Test that select works through FileContext");
    FileContext fc = S3ATestUtils.createTestFileContext(getConfiguration());

    // there's a limit on the number of rows to read; this is larger
    // than the SELECT_LIMIT call to catch any failure where more than
    // that is returned, newline parsing fails, etc etc.
    List<String> list = parseToLines(
        select(fc, getLandsatGZ(), selectConf, SELECT_ENTITY_ID_ALL_CLOUDS),
        SELECT_LIMIT * 2);
    LOG.info("Line count: {}", list.size());
    verifySelectionCount(1, SELECT_LIMIT, SELECT_ENTITY_ID_ALL_CLOUDS, list);
  }

  @Test
  public void testReadLandsatRecords() throws Throwable {
    describe("Use a record reader to read the records");
    inputMust(selectConf, CSV_OUTPUT_FIELD_DELIMITER, "\\t");
    inputMust(selectConf, CSV_OUTPUT_QUOTE_CHARACTER, "'");
    inputMust(selectConf, CSV_OUTPUT_QUOTE_FIELDS,
        CSV_OUTPUT_QUOTE_FIELDS_AS_NEEEDED);
    inputMust(selectConf, CSV_OUTPUT_RECORD_DELIMITER, "\n");
    List<String> records = readRecords(
        selectConf,
        getLandsatGZ(),
        SELECT_ENTITY_ID_ALL_CLOUDS,
        createLineRecordReader(),
        SELECT_LIMIT);
    verifySelectionCount(1, SELECT_LIMIT, SELECT_ENTITY_ID_ALL_CLOUDS, records);
  }

  @Test
  public void testReadLandsatRecordsNoMatch() throws Throwable {
    describe("Verify the v2 record reader does not fail"
        + " when there are no results");
    verifySelectionCount(0, 0, SELECT_NOTHING,
        readRecords(
        selectConf,
            getLandsatGZ(),
        SELECT_NOTHING,
        createLineRecordReader(),
        SELECT_LIMIT));
  }

  @Test
  public void testReadLandsatRecordsGZipEnabled() throws Throwable {
    describe("Verify that by default, the gzip codec is connected to .gz"
        + " files, and so fails");
    // implicitly re-enable the gzip codec.
    selectConf.unset(CommonConfigurationKeys.IO_COMPRESSION_CODECS_KEY);
    intercept(IOException.class, "gzip",
        () -> readRecords(
            selectConf,
            getLandsatGZ(),
            SELECT_ENTITY_ID_ALL_CLOUDS,
            createLineRecordReader(),
            SELECT_LIMIT));
  }

  @Test
  public void testReadLandsatRecordsV1() throws Throwable {
    describe("Use a record reader to read the records");

    verifySelectionCount(1, SELECT_LIMIT, SELECT_ENTITY_ID_ALL_CLOUDS,
        readRecords(
          selectConf,
            getLandsatGZ(),
            SELECT_ENTITY_ID_ALL_CLOUDS,
          createLineRecordReader(),
          SELECT_LIMIT));
  }

  @Test
  public void testReadLandsatRecordsV1NoResults() throws Throwable {
    describe("verify that a select with no results is not an error");

    verifySelectionCount(0, 0, SELECT_NOTHING,
        readRecords(
          selectConf,
            getLandsatGZ(),
          SELECT_NOTHING,
          createLineRecordReader(),
          SELECT_LIMIT));
  }

  /**
   * Select from the landsat file.
   * @param conf config for the select call.
   * @param sql template for a formatted SQL request.
   * @param args arguments for the formatted request.
   * @return the lines selected
   * @throws IOException failure
   */
  private List<String> selectLandsatFile(
      final Configuration conf,
      final String sql,
      final Object... args)
      throws Exception {

    // there's a limit on the number of rows to read; this is larger
    // than the SELECT_LIMIT call to catch any failure where more than
    // that is returned, newline parsing fails, etc etc.
    return parseToLines(
        select(getLandsatFS(), getLandsatGZ(), conf, sql, args));
  }

  /**
   * This is a larger-scale version of {@link ITestS3Select#testSelectSeek()}.
   */
  @Test
  public void testSelectSeekFullLandsat() throws Throwable {
    describe("Verify forward seeks work, not others");

    boolean enabled = getTestPropertyBool(
        getConfiguration(),
        KEY_SCALE_TESTS_ENABLED,
        DEFAULT_SCALE_TESTS_ENABLED);
    assume("Scale test disabled", enabled);

    // start: read in the full data through the initial select
    // this makes asserting that contents match possible
    final Path path = getLandsatGZ();
    S3AFileSystem fs = getLandsatFS();

    int len = (int) fs.getFileStatus(path).getLen();
    byte[] dataset = new byte[4 * _1MB];
    int actualLen;
    try (DurationInfo ignored =
             new DurationInfo(LOG, "Initial read of %s", path);
         FSDataInputStream sourceStream =
             select(fs, path,
                 selectConf,
                 SELECT_EVERYTHING)) {
      // read it in
      actualLen = IOUtils.read(sourceStream, dataset);
    }
    int seekRange = 16 * _1KB;

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
      assertEquals("first byte read", dataset[0], seekStream.read());

      // and now the pos has moved, again, seek will be OK
      seekStream.seek(1);
      seekStream.seek(1);
      // but trying to seek elsewhere now fails
      intercept(PathIOException.class,
          SelectInputStream.SEEK_UNSUPPORTED,
          () -> seekStream.seek(0));
      // positioned reads from the current location work.
      byte[] buffer = new byte[1];
      seekStream.readFully(seekStream.getPos(), buffer);
      // but positioned backwards fail.
      intercept(PathIOException.class,
          SelectInputStream.SEEK_UNSUPPORTED,
          () -> seekStream.readFully(0, buffer));
      // forward seeks are implemented as 1+ skip
      long target = seekStream.getPos() + seekRange;
      seek(seekStream, target);
      assertEquals("Seek position in " + seekStream,
          target, seekStream.getPos());
      // now do a read and compare values
      assertEquals("byte at seek position",
          dataset[(int) seekStream.getPos()], seekStream.read());
      assertEquals("Seek bytes skipped in " + streamStats,
          seekRange, streamStats.bytesSkippedOnSeek);
      long offset;
      long increment = 64 * _1KB;

      // seek forward, comparing bytes
      for(offset = 32 * _1KB; offset < actualLen; offset += increment) {
        seek(seekStream, offset);
        assertEquals("Seek position in " + seekStream,
            offset, seekStream.getPos());
        // now do a read and compare values
        assertEquals("byte at seek position",
            dataset[(int) seekStream.getPos()], seekStream.read());
      }
      for(; offset < len; offset += _1MB) {
        seek(seekStream, offset);
        assertEquals("Seek position in " + seekStream,
            offset, seekStream.getPos());
      }
      // there's no knowledge of how much data is left, but with Gzip
      // involved there can be a lot. To keep the test duration down,
      // this test, unlike the simpler one, doesn't try to read past the
      // EOF. Know this: it will be slow.

      LOG.info("Seek statistics {}", streamStats);
    }
  }

}
