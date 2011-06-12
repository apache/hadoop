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

package org.apache.hadoop.sqoop.orm;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.hadoop.sqoop.ImportOptions;
import org.apache.hadoop.sqoop.ImportOptions.InvalidOptionsException;
import org.apache.hadoop.sqoop.mapred.RawKeyTextOutputFormat;
import org.apache.hadoop.sqoop.orm.CompilationManager;
import org.apache.hadoop.sqoop.testutil.HsqldbTestServer;
import org.apache.hadoop.sqoop.testutil.ImportJobTestCase;
import org.apache.hadoop.sqoop.testutil.ReparseMapper;
import org.apache.hadoop.sqoop.util.ClassLoaderStack;

/**
 * Test that the parse() methods generated in user SqoopRecord implementations
 * work.
 */
public class TestParseMethods extends ImportJobTestCase {

  /**
   * Create the argv to pass to Sqoop
   * @return the argv as an array of strings.
   */
  private String [] getArgv(boolean includeHadoopFlags, String fieldTerminator, 
      String lineTerminator, String encloser, String escape, boolean encloserRequired) {

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
    args.add("--as-textfile");
    args.add("--split-by");
    args.add("DATA_COL0"); // always split by first column.
    args.add("--fields-terminated-by");
    args.add(fieldTerminator);
    args.add("--lines-terminated-by");
    args.add(lineTerminator);
    args.add("--escaped-by");
    args.add(escape);
    if (encloserRequired) {
      args.add("--enclosed-by");
    } else {
      args.add("--optionally-enclosed-by");
    }
    args.add(encloser);
    args.add("--num-mappers");
    args.add("1");

    return args.toArray(new String[0]);
  }

  public void runParseTest(String fieldTerminator, String lineTerminator, String encloser,
      String escape, boolean encloseRequired) throws IOException {

    ClassLoader prevClassLoader = null;

    String [] argv = getArgv(true, fieldTerminator, lineTerminator, encloser, escape,
        encloseRequired);
    runImport(argv);
    try {
      ImportOptions opts = new ImportOptions();

      String tableClassName = getTableName();

      opts.parse(getArgv(false, fieldTerminator, lineTerminator, encloser, escape,
          encloseRequired));

      CompilationManager compileMgr = new CompilationManager(opts);
      String jarFileName = compileMgr.getJarFilename();

      // make sure the user's class is loaded into our address space.
      prevClassLoader = ClassLoaderStack.addJarFile(jarFileName, tableClassName);

      JobConf job = new JobConf();
      job.setJar(jarFileName);

      // Tell the job what class we're testing.
      job.set(ReparseMapper.USER_TYPE_NAME_KEY, tableClassName);

      // use local mode in the same JVM.
      job.set(JTConfig.JT_IPC_ADDRESS, "local");
      job.set("fs.default.name", "file:///");

      String warehouseDir = getWarehouseDir();
      Path warehousePath = new Path(warehouseDir);
      Path inputPath = new Path(warehousePath, getTableName());
      Path outputPath = new Path(warehousePath, getTableName() + "-out");

      job.setMapperClass(ReparseMapper.class);
      job.setNumReduceTasks(0);
      FileInputFormat.addInputPath(job, inputPath);
      FileOutputFormat.setOutputPath(job, outputPath);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(NullWritable.class);
      job.setOutputFormat(RawKeyTextOutputFormat.class);

      JobClient.runJob(job);
    } catch (InvalidOptionsException ioe) {
      fail(ioe.toString());
    } finally {
      if (null != prevClassLoader) {
        ClassLoaderStack.setCurrentClassLoader(prevClassLoader);
      }
    }
  }

  public void testDefaults() throws IOException {
    String [] types = { "INTEGER", "VARCHAR(32)", "INTEGER" };
    String [] vals = { "64", "'foo'", "128" };

    createTableWithColTypes(types, vals);
    runParseTest(",", "\\n", "\\\"", "\\", false);
  }

  public void testRequiredEnclose() throws IOException {
    String [] types = { "INTEGER", "VARCHAR(32)", "INTEGER" };
    String [] vals = { "64", "'foo'", "128" };

    createTableWithColTypes(types, vals);
    runParseTest(",", "\\n", "\\\"", "\\", true);
  }

  public void testStringEscapes() throws IOException {
    String [] types = { "VARCHAR(32)", "VARCHAR(32)", "VARCHAR(32)", "VARCHAR(32)", "VARCHAR(32)" };
    String [] vals = { "'foo'", "'foo,bar'", "'foo''bar'", "'foo\\bar'", "'foo,bar''baz'" };

    createTableWithColTypes(types, vals);
    runParseTest(",", "\\n", "\\\'", "\\", false);
  }

  public void testNumericTypes() throws IOException {
    String [] types = { "INTEGER", "REAL", "FLOAT", "DATE", "TIME",
        "TIMESTAMP", "NUMERIC", "BOOLEAN" };
    String [] vals = { "42", "36.0", "127.1", "'2009-07-02'", "'11:24:00'",
        "'2009-08-13 20:32:00.1234567'", "92104916282869291837672829102857271948687.287475322",
        "true" };
    
    createTableWithColTypes(types, vals);
    runParseTest(",", "\\n", "\\\'", "\\", false);
  }
}

