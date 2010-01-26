/**
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

package org.apache.hadoop.sqoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.hadoop.sqoop.lib.RecordParser;
import org.apache.hadoop.sqoop.lib.SqoopRecord;
import org.apache.hadoop.sqoop.testutil.ExportJobTestCase;
import org.apache.hadoop.sqoop.util.ClassLoaderStack;

import org.junit.Before;

/**
 * Test that we can export data from HDFS into databases.
 */
public class TestExport extends ExportJobTestCase {

  @Before
  public void setUp() {
    // start the server
    super.setUp();

    // throw away any existing data that might be in the database.
    try {
      this.getTestServer().dropExistingSchema();
    } catch (SQLException sqlE) {
      fail(sqlE.toString());
    }
  }

  private String getRecordLine(int recordNum, ColumnGenerator... extraCols) {
    String idStr = Integer.toString(recordNum);
    StringBuilder sb = new StringBuilder();

    sb.append(idStr);
    sb.append("\t");
    sb.append(getMsgPrefix());
    sb.append(idStr);
    for (ColumnGenerator gen : extraCols) {
      sb.append("\t");
      sb.append(gen.getExportText(recordNum));
    }
    sb.append("\n");

    return sb.toString();
  }

  /** When generating data for export tests, each column is generated
      according to a ColumnGenerator. Methods exist for determining
      what to put into text strings in the files to export, as well
      as what the string representation of the column as returned by
      the database should look like.
    */
  interface ColumnGenerator {
    /** for a row with id rowNum, what should we write into that
        line of the text file to export?
      */
    public String getExportText(int rowNum);

    /** for a row with id rowNum, what should the database return
        for the given column's value?
      */
    public String getVerifyText(int rowNum);

    /** Return the column type to put in the CREATE TABLE statement */
    public String getType();
  }

  /**
   * Create a data file that gets exported to the db
   * @param fileNum the number of the file (for multi-file export)
   * @param numRecords how many records to write to the file.
   * @param gzip is true if the file should be gzipped.
   */
  private void createTextFile(int fileNum, int numRecords, boolean gzip,
      ColumnGenerator... extraCols) throws IOException {
    int startId = fileNum * numRecords;

    String ext = ".txt";
    if (gzip) {
      ext = ext + ".gz";
    }
    Path tablePath = getTablePath();
    Path filePath = new Path(tablePath, "part" + fileNum + ext);

    Configuration conf = new Configuration();
    conf.set("fs.default.name", "file:///");
    FileSystem fs = FileSystem.get(conf);
    fs.mkdirs(tablePath);
    OutputStream os = fs.create(filePath);
    if (gzip) {
      CompressionCodecFactory ccf = new CompressionCodecFactory(conf);
      CompressionCodec codec = ccf.getCodec(filePath);
      os = codec.createOutputStream(os);
    }
    BufferedWriter w = new BufferedWriter(new OutputStreamWriter(os));
    for (int i = 0; i < numRecords; i++) {
      w.write(getRecordLine(startId + i, extraCols));
    }
    w.close();
    os.close();

    if (gzip) {
      verifyCompressedFile(filePath, numRecords);
    }
  }

  private void verifyCompressedFile(Path f, int expectedNumLines) throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "file:///");
    FileSystem fs = FileSystem.get(conf);
    InputStream is = fs.open(f);
    CompressionCodecFactory ccf = new CompressionCodecFactory(conf);
    CompressionCodec codec = ccf.getCodec(f);
    LOG.info("gzip check codec is " + codec);
    Decompressor decompressor = CodecPool.getDecompressor(codec);
    if (null == decompressor) {
      LOG.info("Verifying gzip sanity with null decompressor");
    } else {
      LOG.info("Verifying gzip sanity with decompressor: " + decompressor.toString());
    }
    is = codec.createInputStream(is, decompressor);
    BufferedReader r = new BufferedReader(new InputStreamReader(is));
    int numLines = 0;
    while (true) {
      String ln = r.readLine();
      if (ln == null) {
        break;
      }
      numLines++;
    }

    r.close();
    assertEquals("Did not read back correct number of lines",
        expectedNumLines, numLines);
    LOG.info("gzip sanity check returned " + numLines + " lines; ok.");
  }

  /**
   * Create a data file in SequenceFile format that gets exported to the db
   * @param fileNum the number of the file (for multi-file export).
   * @param numRecords how many records to write to the file.
   * @param className the table class name to instantiate and populate
   *          for each record.
   */
  private void createSequenceFile(int fileNum, int numRecords, String className)
      throws IOException {

    try {
      // Instantiate the value record object via reflection. 
      Class cls = Class.forName(className, true,
          Thread.currentThread().getContextClassLoader());
      SqoopRecord record = (SqoopRecord) ReflectionUtils.newInstance(cls, new Configuration());

      // Create the SequenceFile.
      Configuration conf = new Configuration();
      conf.set("fs.default.name", "file:///");
      FileSystem fs = FileSystem.get(conf);
      Path tablePath = getTablePath();
      Path filePath = new Path(tablePath, "part" + fileNum);
      fs.mkdirs(tablePath);
      SequenceFile.Writer w =
          SequenceFile.createWriter(fs, conf, filePath, LongWritable.class, cls);

      // Now write the data.
      int startId = fileNum * numRecords;
      for (int i = 0; i < numRecords; i++) {
        record.parse(getRecordLine(startId + i));
        w.append(new LongWritable(startId + i), record);
      }

      w.close();
    } catch (ClassNotFoundException cnfe) {
      throw new IOException(cnfe);
    } catch (RecordParser.ParseError pe) {
      throw new IOException(pe);
    }
  }

  /** Return the column name for a column index.
   *  Each table contains two columns named 'id' and 'msg', and then an
   *  arbitrary number of additional columns defined by ColumnGenerators.
   *  These columns are referenced by idx 0, 1, 2...
   *  @param idx the index of the ColumnGenerator in the array passed to
   *   createTable().
   *  @return the name of the column
   */
  protected String forIdx(int idx) {
    return "col" + idx;
  }

  /** Create the table definition to export to, removing any prior table.
      By specifying ColumnGenerator arguments, you can add extra columns
      to the table of arbitrary type.
   */
  public void createTable(ColumnGenerator... extraColumns) throws SQLException {
    Connection conn = getTestServer().getConnection();
    PreparedStatement statement = conn.prepareStatement(
        "DROP TABLE " + getTableName() + " IF EXISTS",
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    statement.executeUpdate();
    conn.commit();
    statement.close();

    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE ");
    sb.append(getTableName());
    sb.append(" (id INT NOT NULL PRIMARY KEY, msg VARCHAR(64)");
    int colNum = 0;
    for (ColumnGenerator gen : extraColumns) {
      sb.append(", " + forIdx(colNum++) + " " + gen.getType());
    }
    sb.append(")");

    statement = conn.prepareStatement(sb.toString(),
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    statement.executeUpdate();
    conn.commit();
    statement.close();
  }

  /** Removing an existing table directory from the filesystem */
  private void removeTablePath() throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "file:///");
    FileSystem fs = FileSystem.get(conf);
    fs.delete(getTablePath(), true);
  }

  /** Verify that on a given row, a column has a given value.
   * @param id the id column specifying the row to test.
   */
  private void assertColValForRowId(int id, String colName, String expectedVal)
      throws SQLException {
    Connection conn = getTestServer().getConnection();
    LOG.info("Verifying column " + colName + " has value " + expectedVal);

    PreparedStatement statement = conn.prepareStatement(
        "SELECT " + colName + " FROM " + getTableName() + " WHERE id = " + id,
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    ResultSet rs = statement.executeQuery();
    rs.next();

    String actualVal = rs.getString(1);
    rs.close();
    statement.close();

    assertEquals("Got unexpected column value", expectedVal, actualVal);
  }

  /** Verify that for the max and min values of the 'id' column, the values
      for a given column meet the expected values.
   */
  private void assertColMinAndMax(String colName, ColumnGenerator generator)
      throws SQLException {
    int minId = getMinRowId();
    int maxId = getMaxRowId();

    LOG.info("Checking min/max for column " + colName + " with type " + generator.getType());

    String expectedMin = generator.getVerifyText(minId);
    String expectedMax = generator.getVerifyText(maxId);

    assertColValForRowId(minId, colName, expectedMin);
    assertColValForRowId(maxId, colName, expectedMax);
  }

  /** Export 10 rows, make sure they load in correctly */
  public void testTextExport() throws IOException, SQLException {

    final int TOTAL_RECORDS = 10;

    createTextFile(0, TOTAL_RECORDS, false);
    createTable();
    runExport(getArgv(true));
    verifyExport(TOTAL_RECORDS);
  }

  /** Export 10 rows from gzipped text files. */
  public void testGzipExport() throws IOException, SQLException {

    LOG.info("Beginning gzip export test");

    final int TOTAL_RECORDS = 10;

    createTextFile(0, TOTAL_RECORDS, true);
    createTable();
    runExport(getArgv(true));
    verifyExport(TOTAL_RECORDS);
    LOG.info("Complete gzip export test");
  }

  /** Run 2 mappers, make sure all records load in correctly */
  public void testMultiMapTextExport() throws IOException, SQLException {

    final int RECORDS_PER_MAP = 10;
    final int NUM_FILES = 2;

    for (int f = 0; f < NUM_FILES; f++) {
      createTextFile(f, RECORDS_PER_MAP, false);
    }

    createTable();
    runExport(getArgv(true));
    verifyExport(RECORDS_PER_MAP * NUM_FILES);
  }


  /** Export some rows from a SequenceFile, make sure they import correctly */
  public void testSequenceFileExport() throws IOException, SQLException {

    final int TOTAL_RECORDS = 10;

    // First, generate class and jar files that represent the table we're exporting to.
    LOG.info("Creating initial schema for SeqFile test");
    createTable();
    LOG.info("Generating code..."); 
    List<String> generatedJars = runExport(getArgv(true, "--generate-only"));

    // Now, wipe the created table so we can export on top of it again.
    LOG.info("Resetting schema and data...");
    createTable();

    // Wipe the directory we use when creating files to export to ensure
    // it's ready for new SequenceFiles.
    removeTablePath();

    assertNotNull(generatedJars);
    assertEquals("Expected 1 generated jar file", 1, generatedJars.size());
    String jarFileName = generatedJars.get(0);
    // Sqoop generates jars named "foo.jar"; by default, this should contain a
    // class named 'foo'. Extract the class name.
    Path jarPath = new Path(jarFileName);
    String jarBaseName = jarPath.getName();
    assertTrue(jarBaseName.endsWith(".jar"));
    assertTrue(jarBaseName.length() > ".jar".length());
    String className = jarBaseName.substring(0, jarBaseName.length() - ".jar".length());

    LOG.info("Using jar filename: " + jarFileName);
    LOG.info("Using class name: " + className);

    ClassLoader prevClassLoader = null;

    try {
      if (null != jarFileName) {
        prevClassLoader = ClassLoaderStack.addJarFile(jarFileName, className);
      }

      // Now use this class and jar name to create a sequence file.
      LOG.info("Writing data to SequenceFiles");
      createSequenceFile(0, TOTAL_RECORDS, className);

      // Now run and verify the export.
      LOG.info("Exporting SequenceFile-based data");
      runExport(getArgv(true, "--class-name", className, "--jar-file", jarFileName));
      verifyExport(TOTAL_RECORDS);
    } finally {
      if (null != prevClassLoader) {
        ClassLoaderStack.setCurrentClassLoader(prevClassLoader);
      }
    }
  }

  public void testIntCol() throws IOException, SQLException {
    final int TOTAL_RECORDS = 10;

    // generate a column equivalent to rownum.
    ColumnGenerator gen = new ColumnGenerator() {
      public String getExportText(int rowNum) {
        return "" + rowNum;
      }
      public String getVerifyText(int rowNum) {
        return "" + rowNum;
      }
      public String getType() {
        return "INTEGER";
      }
    };

    createTextFile(0, TOTAL_RECORDS, false, gen);
    createTable(gen);
    runExport(getArgv(true));
    verifyExport(TOTAL_RECORDS);
    assertColMinAndMax(forIdx(0), gen);
  }

  public void testBigIntCol() throws IOException, SQLException {
    final int TOTAL_RECORDS = 10;

    // generate a column that won't fit in a normal int.
    ColumnGenerator gen = new ColumnGenerator() {
      public String getExportText(int rowNum) {
        long val = (long) rowNum * 1000000000;
        return "" + val;
      }
      public String getVerifyText(int rowNum) {
        long val = (long) rowNum * 1000000000;
        return "" + val;
      }
      public String getType() {
        return "BIGINT";
      }
    };

    createTextFile(0, TOTAL_RECORDS, false, gen);
    createTable(gen);
    runExport(getArgv(true));
    verifyExport(TOTAL_RECORDS);
    assertColMinAndMax(forIdx(0), gen);
  }

  private String pad(int n) {
    if (n <= 9) {
      return "0" + n;
    } else {
      return String.valueOf(n);
    }
  }

  public void testDatesAndTimes() throws IOException, SQLException {
    final int TOTAL_RECORDS = 10;

    ColumnGenerator genDate = new ColumnGenerator() {
      public String getExportText(int rowNum) {
        int day = rowNum + 1;
        return "2009-10-" + day;
      }
      public String getVerifyText(int rowNum) {
        int day = rowNum + 1;
        return "2009-10-" + pad(day);
      }
      public String getType() {
        return "DATE";
      }
    };

    ColumnGenerator genTime = new ColumnGenerator() {
      public String getExportText(int rowNum) {
        return "10:01:" + rowNum;
      }
      public String getVerifyText(int rowNum) {
        return "10:01:" + pad(rowNum);
      }
      public String getType() {
        return "TIME";
      }
    };

    createTextFile(0, TOTAL_RECORDS, false, genDate, genTime);
    createTable(genDate, genTime);
    runExport(getArgv(true));
    verifyExport(TOTAL_RECORDS);
    assertColMinAndMax(forIdx(0), genDate);
    assertColMinAndMax(forIdx(1), genTime);
  }

  public void testNumericTypes() throws IOException, SQLException {
    final int TOTAL_RECORDS = 10;

    // Check floating point values
    ColumnGenerator genFloat = new ColumnGenerator() {
      public String getExportText(int rowNum) {
        double v = 3.141 * (double) rowNum;
        return "" + v;
      }
      public String getVerifyText(int rowNum) {
        double v = 3.141 * (double) rowNum;
        return "" + v;
      }
      public String getType() {
        return "FLOAT";
      }
    };

    // Check precise decimal placement. The first of ten
    // rows will be 2.7181; the last of ten rows will be
    // 2.71810.
    ColumnGenerator genNumeric = new ColumnGenerator() {
      public String getExportText(int rowNum) {
        int digit = rowNum + 1;
        return "2.718" + digit;
      }
      public String getVerifyText(int rowNum) {
        int digit = rowNum + 1;
        return "2.718" + digit;
      }
      public String getType() {
        return "NUMERIC";
      }
    };

    createTextFile(0, TOTAL_RECORDS, false, genFloat, genNumeric);
    createTable(genFloat, genNumeric);
    runExport(getArgv(true));
    verifyExport(TOTAL_RECORDS);
    assertColMinAndMax(forIdx(0), genFloat);
    assertColMinAndMax(forIdx(1), genNumeric);
  }
}
