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

package org.apache.hadoop.streaming;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;

/**
 * Tests stream job with java tasks, commands in MapReduce local mode.
 * Validates if user-set config properties
 * {@link MRJobConfig#MAP_OUTPUT_KEY_CLASS} and
 * {@link MRJobConfig#OUTPUT_KEY_CLASS} are honored by streaming jobs.
 */
public class TestStreamingOutputKeyValueTypes extends TestStreaming {

  public TestStreamingOutputKeyValueTypes() throws IOException {
    super();
    input = "one line dummy input\n";
  }

  @Before
  @Override
  public void setUp() throws IOException {
    args.clear();
    super.setUp();
  }

  @Override
  protected String[] genArgs() {
    // set the testcase-specific config properties first and the remaining
    // arguments are set in TestStreaming.genArgs().
    args.add("-jobconf");
    args.add(MRJobConfig.MAP_OUTPUT_KEY_CLASS +
        "=org.apache.hadoop.io.LongWritable");
    args.add("-jobconf");
    args.add(MRJobConfig.OUTPUT_KEY_CLASS +
        "=org.apache.hadoop.io.LongWritable");

    // Using SequenceFileOutputFormat here because with TextOutputFormat, the
    // mapred.output.key.class set in JobConf (which we want to test here) is
    // not read/used at all.
    args.add("-outputformat");
    args.add("org.apache.hadoop.mapred.SequenceFileOutputFormat");

    return super.genArgs();
  }

  @Override
  protected void checkOutput() throws IOException {
    // No need to validate output for the test cases in this class
  }

  public static class MyReducer<K, V>
  extends MapReduceBase implements Reducer<K, V, LongWritable, Text> {

    public void reduce(K key, Iterator<V> values,
        OutputCollector<LongWritable, Text> output, Reporter reporter)
        throws IOException {
      LongWritable l = new LongWritable();
      while (values.hasNext()) {
        output.collect(l, new Text(values.next().toString()));
      }
    }
  }

  // Check with Java Mapper, Java Reducer
  @Test
  public void testJavaMapperAndJavaReducer() throws Exception {
    map = "org.apache.hadoop.mapred.lib.IdentityMapper";
    reduce = "org.apache.hadoop.mapred.lib.IdentityReducer";
    super.testCommandLine();
  }

  // Check with Java Mapper, Java Reducer and -numReduceTasks 0
  @Test
  public void testJavaMapperAndJavaReducerAndZeroReduces() throws Exception {
    map = "org.apache.hadoop.mapred.lib.IdentityMapper";
    reduce = "org.apache.hadoop.mapred.lib.IdentityReducer";
    args.add("-numReduceTasks");
    args.add("0");
    super.testCommandLine();
  }

  // Check with Java Mapper, Reducer = "NONE"
  @Test
  public void testJavaMapperWithReduceNone() throws Exception {
    map = "org.apache.hadoop.mapred.lib.IdentityMapper";
    reduce = "NONE";
    super.testCommandLine();
  }

  // Check with Java Mapper, command Reducer
  @Test
  public void testJavaMapperAndCommandReducer() throws Exception {
    map = "org.apache.hadoop.mapred.lib.IdentityMapper";
    reduce = CAT;
    super.testCommandLine();
  }

  // Check with Java Mapper, command Reducer and -numReduceTasks 0
  @Test
  public void testJavaMapperAndCommandReducerAndZeroReduces() throws Exception {
    map = "org.apache.hadoop.mapred.lib.IdentityMapper";
    reduce = CAT;
    args.add("-numReduceTasks");
    args.add("0");
    super.testCommandLine();
  }

  // Check with Command Mapper, Java Reducer
  @Test
  public void testCommandMapperAndJavaReducer() throws Exception {
    map = CAT;
    reduce = MyReducer.class.getName();
    super.testCommandLine();
  }

  // Check with Command Mapper, Java Reducer and -numReduceTasks 0
  @Test
  public void testCommandMapperAndJavaReducerAndZeroReduces() throws Exception {
    map = CAT;
    reduce = MyReducer.class.getName();
    args.add("-numReduceTasks");
    args.add("0");
    super.testCommandLine();
  }

  // Check with Command Mapper, Reducer = "NONE"
  @Test
  public void testCommandMapperWithReduceNone() throws Exception {
    map = CAT;
    reduce = "NONE";
    super.testCommandLine();
  }

  // Check with Command Mapper, Command Reducer
  @Test
  public void testCommandMapperAndCommandReducer() throws Exception {
    map = CAT;
    reduce = CAT;
    super.testCommandLine();
  }

  // Check with Command Mapper, Command Reducer and -numReduceTasks 0
  @Test
  public void testCommandMapperAndCommandReducerAndZeroReduces()
      throws Exception {
    map = CAT;
    reduce = CAT;
    args.add("-numReduceTasks");
    args.add("0");
    super.testCommandLine();
  }
  
  @Test
  public void testDefaultToIdentityReducer() throws Exception {
    args.add("-mapper");args.add(map);
    args.add("-jobconf");
    args.add("mapreduce.task.files.preserve.failedtasks=true");
    args.add("-jobconf");
    args.add("stream.tmpdir="+System.getProperty("test.build.data","/tmp"));
    args.add("-inputformat");args.add(TextInputFormat.class.getName());
    super.testCommandLine();
  }

  @Override
  @Test
  public void testCommandLine() {
    // Do nothing
  }
}
