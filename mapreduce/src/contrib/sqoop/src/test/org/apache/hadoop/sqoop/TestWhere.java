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

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.hadoop.sqoop.ImportOptions.InvalidOptionsException;
import org.apache.hadoop.sqoop.orm.CompilationManager;
import org.apache.hadoop.sqoop.testutil.HsqldbTestServer;
import org.apache.hadoop.sqoop.testutil.ImportJobTestCase;
import org.apache.hadoop.sqoop.testutil.SeqFileReader;
import org.apache.hadoop.sqoop.util.ClassLoaderStack;

/**
 * Test that --where works in Sqoop.
 * Methods essentially copied out of the other Test* classes.
 * TODO(kevin or aaron): Factor out these common test methods
 * so that every new Test* class doesn't need to copy the code.
 */
public class TestWhere extends ImportJobTestCase {

  /**
   * Create the argv to pass to Sqoop
   * @return the argv as an array of strings.
   */
  private String [] getArgv(boolean includeHadoopFlags, String [] colNames, String whereClause) {
    String columnsString = "";
    for (String col : colNames) {
      columnsString += col + ",";
    }

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
    args.add(HsqldbTestServer.getTableName());
    args.add("--columns");
    args.add(columnsString);
    args.add("--where");
    args.add(whereClause);
    args.add("--split-by");
    args.add("INTFIELD1");
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--connect");
    args.add(HsqldbTestServer.getUrl());
    args.add("--as-sequencefile");
    args.add("--num-mappers");
    args.add("1");

    return args.toArray(new String[0]);
  }

  // this test just uses the two int table.
  protected String getTableName() {
    return HsqldbTestServer.getTableName();
  }


  /**
   * Given a comma-delimited list of integers, grab and parse the first int
   * @param str a comma-delimited list of values, the first of which is an int.
   * @return the first field in the string, cast to int
   */
  private int getFirstInt(String str) {
    String [] parts = str.split(",");
    return Integer.parseInt(parts[0]);
  }

  public void runWhereTest(String whereClause, String firstValStr, int numExpectedResults,
      int expectedSum) throws IOException {

    String [] columns = HsqldbTestServer.getFieldNames();
    ClassLoader prevClassLoader = null;
    SequenceFile.Reader reader = null;

    String [] argv = getArgv(true, columns, whereClause);
    runImport(argv);
    try {
      ImportOptions opts = new ImportOptions();
      opts.parse(getArgv(false, columns, whereClause));

      CompilationManager compileMgr = new CompilationManager(opts);
      String jarFileName = compileMgr.getJarFilename();

      prevClassLoader = ClassLoaderStack.addJarFile(jarFileName, getTableName());

      reader = SeqFileReader.getSeqFileReader(getDataFilePath().toString());

      // here we can actually instantiate (k, v) pairs.
      Configuration conf = new Configuration();
      Object key = ReflectionUtils.newInstance(reader.getKeyClass(), conf);
      Object val = ReflectionUtils.newInstance(reader.getValueClass(), conf);

      if (reader.next(key) == null) {
        fail("Empty SequenceFile during import");
      }

      // make sure that the value we think should be at the top, is.
      reader.getCurrentValue(val);
      assertEquals("Invalid ordering within sorted SeqFile", firstValStr, val.toString());

      // We know that these values are two ints separated by a ',' character.
      // Since this is all dynamic, though, we don't want to actually link against
      // the class and use its methods. So we just parse this back into int fields manually.
      // Sum them up and ensure that we get the expected total for the first column, to
      // verify that we got all the results from the db into the file.
      int curSum = getFirstInt(val.toString());
      int totalResults = 1;

      // now sum up everything else in the file.
      while (reader.next(key) != null) {
        reader.getCurrentValue(val);
        curSum += getFirstInt(val.toString());
        totalResults++;
      }

      assertEquals("Total sum of first db column mismatch", expectedSum, curSum);
      assertEquals("Incorrect number of results for query", numExpectedResults, totalResults);
    } catch (InvalidOptionsException ioe) {
      fail(ioe.toString());
    } finally {
      IOUtils.closeStream(reader);

      if (null != prevClassLoader) {
        ClassLoaderStack.setCurrentClassLoader(prevClassLoader);
      }
    }
  }

  public void testSingleClauseWhere() throws IOException {
    String whereClause = "INTFIELD2 > 4";
    runWhereTest(whereClause, "1,8\n", 2, 4);
  }

  public void testMultiClauseWhere() throws IOException {
    String whereClause = "INTFIELD1 > 4 AND INTFIELD2 < 3";
    runWhereTest(whereClause, "7,2\n", 1, 7);
  }
}
