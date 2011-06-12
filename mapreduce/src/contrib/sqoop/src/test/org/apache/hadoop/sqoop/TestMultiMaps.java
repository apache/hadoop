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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
 * Test that using multiple mapper splits works.
 */
public class TestMultiMaps extends ImportJobTestCase {

  /**
   * Create the argv to pass to Sqoop
   * @return the argv as an array of strings.
   */
  private String [] getArgv(boolean includeHadoopFlags, String [] colNames, String splitByCol) {
    String columnsString = "";
    for (String col : colNames) {
      columnsString += col + ",";
    }

    ArrayList<String> args = new ArrayList<String>();

    if (includeHadoopFlags) {
      args.add("-D");
      args.add("mapred.job.tracker=local");
      args.add("-D");
      args.add("fs.default.name=file:///");
    }

    args.add("--table");
    args.add(HsqldbTestServer.getTableName());
    args.add("--columns");
    args.add(columnsString);
    args.add("--split-by");
    args.add(splitByCol);
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--connect");
    args.add(HsqldbTestServer.getUrl());
    args.add("--as-sequencefile");
    args.add("--num-mappers");
    args.add("2");

    return args.toArray(new String[0]);
  }

  // this test just uses the two int table.
  protected String getTableName() {
    return HsqldbTestServer.getTableName();
  }

  /** @return a list of Path objects for each data file */
  protected List<Path> getDataFilePaths() throws IOException {
    List<Path> paths = new ArrayList<Path>();
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "file:///");
    FileSystem fs = FileSystem.get(conf);

    FileStatus [] stats = fs.listStatus(getTablePath());
    for (FileStatus stat : stats) {
      paths.add(stat.getPath());
    }

    return paths;
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

  public void runMultiMapTest(String splitByCol, int expectedSum)
      throws IOException {

    String [] columns = HsqldbTestServer.getFieldNames();
    ClassLoader prevClassLoader = null;
    SequenceFile.Reader reader = null;

    String [] argv = getArgv(true, columns, splitByCol);
    runImport(argv);
    try {
      ImportOptions opts = new ImportOptions();
      opts.parse(getArgv(false, columns, splitByCol));

      CompilationManager compileMgr = new CompilationManager(opts);
      String jarFileName = compileMgr.getJarFilename();

      prevClassLoader = ClassLoaderStack.addJarFile(jarFileName, getTableName());

      List<Path> paths = getDataFilePaths();
      Configuration conf = new Configuration();
      int curSum = 0;

      assertTrue("Found only " + paths.size() + " path(s); expected > 1.", paths.size() > 1);

      // We expect multiple files. We need to open all the files and sum up the
      // first column across all of them.
      for (Path p : paths) {
        reader = SeqFileReader.getSeqFileReader(p.toString());

        // here we can actually instantiate (k, v) pairs.
        Object key = ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        Object val = ReflectionUtils.newInstance(reader.getValueClass(), conf);

        // We know that these values are two ints separated by a ',' character.
        // Since this is all dynamic, though, we don't want to actually link against
        // the class and use its methods. So we just parse this back into int fields manually.
        // Sum them up and ensure that we get the expected total for the first column, to
        // verify that we got all the results from the db into the file.

        // now sum up everything in the file.
        while (reader.next(key) != null) {
          reader.getCurrentValue(val);
          curSum += getFirstInt(val.toString());
        }

        IOUtils.closeStream(reader);
        reader = null;
      }

      assertEquals("Total sum of first db column mismatch", expectedSum, curSum);
    } catch (InvalidOptionsException ioe) {
      fail(ioe.toString());
    } finally {
      IOUtils.closeStream(reader);

      if (null != prevClassLoader) {
        ClassLoaderStack.setCurrentClassLoader(prevClassLoader);
      }
    }
  }

  public void testSplitByFirstCol() throws IOException {
    runMultiMapTest("INTFIELD1", HsqldbTestServer.getFirstColSum());
  }
}
