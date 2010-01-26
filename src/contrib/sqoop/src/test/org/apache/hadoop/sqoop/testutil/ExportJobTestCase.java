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

package org.apache.hadoop.sqoop.testutil;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.sqoop.Sqoop;

/**
 * Class that implements common methods required for tests which export data
 * from HDFS to databases, to verify correct export
 */
public class ExportJobTestCase extends BaseSqoopTestCase {

  public static final Log LOG = LogFactory.getLog(ExportJobTestCase.class.getName());

  protected String getTablePrefix() {
    return "EXPORT_TABLE_";
  }

  /**
   * Create the argv to pass to Sqoop
   * @param includeHadoopFlags if true, then include -D various.settings=values
   * @return the argv as an array of strings.
   */
  protected String [] getArgv(boolean includeHadoopFlags, String... additionalArgv) {
    ArrayList<String> args = new ArrayList<String>();

    if (includeHadoopFlags) {
      CommonArgs.addHadoopFlags(args);
    }

    args.add("--table");
    args.add(getTableName());
    args.add("--export-dir");
    args.add(getTablePath().toString());
    args.add("--connect");
    args.add(HsqldbTestServer.getUrl());
    args.add("--fields-terminated-by");
    args.add("\\t");
    args.add("--lines-terminated-by");
    args.add("\\n");


    if (null != additionalArgv) {
      for (String arg : additionalArgv) {
        args.add(arg);
      }
    }

    return args.toArray(new String[0]);
  }

  /** When exporting text columns, what should the text contain? */
  protected String getMsgPrefix() {
    return "textfield";
  }


  /** @return the minimum 'id' value in the table */
  protected int getMinRowId() throws SQLException {
    Connection conn = getTestServer().getConnection();
    PreparedStatement statement = conn.prepareStatement(
        "SELECT MIN(id) FROM " + getTableName(),
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    ResultSet rs = statement.executeQuery();
    rs.next();
    int minVal = rs.getInt(1);
    rs.close();
    statement.close();

    return minVal;
  }

  /** @return the maximum 'id' value in the table */
  protected int getMaxRowId() throws SQLException {
    Connection conn = getTestServer().getConnection();
    PreparedStatement statement = conn.prepareStatement(
        "SELECT MAX(id) FROM " + getTableName(),
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    ResultSet rs = statement.executeQuery();
    rs.next();
    int maxVal = rs.getInt(1);
    rs.close();
    statement.close();

    return maxVal;
  }

  /**
   * Check that we got back the expected row set
   * @param expectedNumRecords The number of records we expected to load
   * into the database.
   */
  protected void verifyExport(int expectedNumRecords) throws IOException, SQLException {
    Connection conn = getTestServer().getConnection();

    LOG.info("Verifying export: " + getTableName());
    // Check that we got back the correct number of records.
    PreparedStatement statement = conn.prepareStatement(
        "SELECT COUNT(*) FROM " + getTableName(),
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    ResultSet rs = statement.executeQuery();
    rs.next();
    int actualNumRecords = rs.getInt(1);
    rs.close();
    statement.close();

    assertEquals("Got back unexpected row count", expectedNumRecords,
        actualNumRecords);

    // Check that we start with row 0.
    int minVal = getMinRowId();
    assertEquals("Minimum row was not zero", 0, minVal);

    // Check that the last row we loaded is numRows - 1
    int maxVal = getMaxRowId();
    assertEquals("Maximum row had invalid id", expectedNumRecords - 1, maxVal);

    // Check that the string values associated with these points match up.
    statement = conn.prepareStatement("SELECT msg FROM " + getTableName()
        + " WHERE id = " + minVal,
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    rs = statement.executeQuery();
    rs.next();
    String minMsg = rs.getString(1);
    rs.close();
    statement.close();

    assertEquals("Invalid msg field for min value", getMsgPrefix() + minVal, minMsg);

    statement = conn.prepareStatement("SELECT msg FROM " + getTableName()
        + " WHERE id = " + maxVal,
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    rs = statement.executeQuery();
    rs.next();
    String maxMsg = rs.getString(1);
    rs.close();
    statement.close();

    assertEquals("Invalid msg field for min value", getMsgPrefix() + maxVal, maxMsg);
  }

  /**
   * Run a MapReduce-based export (using the argv provided to control execution).
   * @return the generated jar filename
   */
  protected List<String> runExport(String [] argv) throws IOException {
    // run the tool through the normal entry-point.
    int ret;
    List<String> generatedJars = null;
    try {
      Sqoop exporter = new Sqoop();
      ret = ToolRunner.run(exporter, argv);
      generatedJars = exporter.getGeneratedJarFiles();
    } catch (Exception e) {
      LOG.error("Got exception running Sqoop: " + e.toString());
      e.printStackTrace();
      ret = 1;
    }

    // expect a successful return.
    if (0 != ret) {
      throw new IOException("Failure during job; return status " + ret);
    }

    return generatedJars;
  }

}
