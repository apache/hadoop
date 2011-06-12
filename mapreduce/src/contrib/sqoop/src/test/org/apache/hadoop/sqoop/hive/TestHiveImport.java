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

package org.apache.hadoop.sqoop.hive;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.sqoop.ImportOptions;
import org.apache.hadoop.sqoop.testutil.HsqldbTestServer;
import org.apache.hadoop.sqoop.testutil.ImportJobTestCase;

/**
 * Test HiveImport capability after an import to HDFS.
 */
public class TestHiveImport extends ImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(TestHiveImport.class.getName());

  /**
   * Create the argv to pass to Sqoop
   * @return the argv as an array of strings.
   */
  private String [] getArgv(boolean includeHadoopFlags, String [] moreArgs) {
    ArrayList<String> args = new ArrayList<String>();

    if (includeHadoopFlags) {
      args.add("-D");
      args.add("mapreduce.jobtracker.address=local");
      args.add("-D");
      args.add("mapreduce.job.maps=1");
      args.add("-D");
      args.add("fs.default.name=file:///");
    }

    args.add("--table");
    args.add(getTableName());
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--connect");
    args.add(HsqldbTestServer.getUrl());
    args.add("--hive-import");
    args.add("--split-by");
    args.add(getColNames()[0]);
    args.add("--num-mappers");
    args.add("1");

    if (null != moreArgs) {
      for (String arg: moreArgs) {
        args.add(arg);
      }
    }

    return args.toArray(new String[0]);
  }

  private ImportOptions getImportOptions(String [] extraArgs) {
    ImportOptions opts = new ImportOptions();
    try {
      opts.parse(getArgv(false, extraArgs));
    } catch (ImportOptions.InvalidOptionsException ioe) {
      fail("Invalid options: " + ioe.toString());
    }

    return opts;
  }

  private void runImportTest(String tableName, String [] types, String [] values,
      String verificationScript, String [] extraArgs) throws IOException {

    // create a table and populate it with a row...
    setCurTableName(tableName);
    createTableWithColTypes(types, values);
    
    // set up our mock hive shell to compare our generated script
    // against the correct expected one.
    ImportOptions options = getImportOptions(extraArgs);
    String hiveHome = options.getHiveHome();
    assertNotNull("hive.home was not set", hiveHome);
    Path testDataPath = new Path(new Path(hiveHome), "scripts/" + verificationScript);
    System.setProperty("expected.script", testDataPath.toString());

    // verify that we can import it correctly into hive.
    runImport(getArgv(true, extraArgs));
  }

  /** Test that strings and ints are handled in the normal fashion */
  @Test
  public void testNormalHiveImport() throws IOException {
    String [] types = { "VARCHAR(32)", "INTEGER", "CHAR(64)" };
    String [] vals = { "'test'", "42", "'somestring'" };
    runImportTest("NORMAL_HIVE_IMPORT", types, vals, "normalImport.q", null);
  }

  /** Test that dates are coerced properly to strings */
  @Test
  public void testDate() throws IOException {
    String [] types = { "VARCHAR(32)", "DATE" };
    String [] vals = { "'test'", "'2009-05-12'" };
    runImportTest("DATE_HIVE_IMPORT", types, vals, "dateImport.q", null);
  }

  /** Test that NUMERICs are coerced to doubles */
  @Test
  public void testNumeric() throws IOException {
    String [] types = { "NUMERIC", "CHAR(64)" };
    String [] vals = { "3.14159", "'foo'" };
    runImportTest("NUMERIC_HIVE_IMPORT", types, vals, "numericImport.q", null);
  }

  /** If bin/hive returns an error exit status, we should get an IOException */
  @Test
  public void testHiveExitFails() {
    // The expected script is different than the one which would be generated
    // by this, so we expect an IOException out.
    String [] types = { "NUMERIC", "CHAR(64)" };
    String [] vals = { "3.14159", "'foo'" };
    try {
      runImportTest("FAILING_HIVE_IMPORT", types, vals, "failingImport.q", null);
      // If we get here, then the run succeeded -- which is incorrect.
      fail("FAILING_HIVE_IMPORT test should have thrown IOException");
    } catch (IOException ioe) {
      // expected; ok.
    }
  }

  /** Test that we can set delimiters how we want them */
  @Test
  public void testCustomDelimiters() throws IOException {
    String [] types = { "VARCHAR(32)", "INTEGER", "CHAR(64)" };
    String [] vals = { "'test'", "42", "'somestring'" };
    String [] extraArgs = { "--fields-terminated-by", ",", "--lines-terminated-by", "|" };
    runImportTest("CUSTOM_DELIM_IMPORT", types, vals, "customDelimImport.q", extraArgs);
  }

}

