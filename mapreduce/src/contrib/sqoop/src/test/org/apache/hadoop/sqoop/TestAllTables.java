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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;

import org.apache.hadoop.sqoop.testutil.HsqldbTestServer;
import org.apache.hadoop.sqoop.testutil.ImportJobTestCase;

/**
 * Test the --all-tables functionality that can import multiple tables.
 */
public class TestAllTables extends ImportJobTestCase {

  /**
   * Create the argv to pass to Sqoop
   * @return the argv as an array of strings.
   */
  private String [] getArgv(boolean includeHadoopFlags) {
    ArrayList<String> args = new ArrayList<String>();

    if (includeHadoopFlags) {
      args.add("-D");
      args.add("mapreduce.jobtracker.address=local");
      args.add("-D");
      args.add("mapreduce.job.maps=1");
      args.add("-D");
      args.add("fs.default.name=file:///");
    }

    args.add("--all-tables");
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--connect");
    args.add(HsqldbTestServer.getUrl());
    args.add("--num-mappers");
    args.add("1");

    return args.toArray(new String[0]);
  }

  /** the names of the tables we're creating. */
  private List<String> tableNames;

  /** The strings to inject in the (ordered) tables */
  private List<String> expectedStrings;

  @Before
  public void setUp() {
    // start the server
    super.setUp();

    // throw away TWOINTTABLE and things we don't care about.
    try {
      this.getTestServer().dropExistingSchema();
    } catch (SQLException sqlE) {
      fail(sqlE.toString());
    }

    this.tableNames = new ArrayList<String>();
    this.expectedStrings = new ArrayList<String>();

    // create two tables.
    this.expectedStrings.add("A winner");
    this.expectedStrings.add("is you!");

    for (String expectedStr: this.expectedStrings) {
      this.createTableForColType("VARCHAR(32) PRIMARY KEY", "'" + expectedStr + "'");
      this.tableNames.add(this.getTableName());
      this.removeTableDir();
      incrementTableNum();
    }
  }

  public void testMultiTableImport() throws IOException {
    String [] argv = getArgv(true);
    runImport(argv);

    Path warehousePath = new Path(this.getWarehouseDir());
    for (String tableName : this.tableNames) {
      Path tablePath = new Path(warehousePath, tableName);
      Path filePath = new Path(tablePath, "part-m-00000");

      // dequeue the expected value for this table. This
      // list has the same order as the tableNames list.
      String expectedVal = this.expectedStrings.get(0);
      this.expectedStrings.remove(0);

      BufferedReader reader = new BufferedReader(
          new InputStreamReader(new FileInputStream(new File(filePath.toString()))));
      try {
        String line = reader.readLine();
        assertEquals("Table " + tableName + " expected a different string",
            expectedVal, line);
      } finally {
        IOUtils.closeStream(reader);
      }
    }
  }
}
