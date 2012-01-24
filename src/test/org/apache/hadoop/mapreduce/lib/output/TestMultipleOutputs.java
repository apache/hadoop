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
package org.apache.hadoop.mapreduce.lib.output;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.HadoopTestCase;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class TestMultipleOutputs extends HadoopTestCase {

  public TestMultipleOutputs() throws IOException {
    super(HadoopTestCase.LOCAL_MR, HadoopTestCase.LOCAL_FS, 1, 1);
  }

  public void testWithoutCounters() throws Exception {
    _testMultipleOutputs(false);
  }

  public void testWithCounters() throws Exception {
    _testMultipleOutputs(true);
  }

  private static final Path ROOT_DIR = new Path("testing/mo");
  private static final Path IN_DIR = new Path(ROOT_DIR, "input");
  private static final Path OUT_DIR = new Path(ROOT_DIR, "output");

  private Path getDir(Path dir) {
    // Hack for local FS that does not have the concept of a 'mounting point'
    if (isLocalFS()) {
      String localPathRoot = System.getProperty("test.build.data", "/tmp")
        .replace(' ', '+');
      dir = new Path(localPathRoot, dir);
    }
    return dir;
  }

  public void setUp() throws Exception {
    super.setUp();
    Path rootDir = getDir(ROOT_DIR);
    Path inDir = getDir(IN_DIR);

    JobConf conf = createJobConf();
    FileSystem fs = FileSystem.get(conf);
    fs.delete(rootDir, true);
    if (!fs.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }
  }

  public void tearDown() throws Exception {
    Path rootDir = getDir(ROOT_DIR);

    JobConf conf = createJobConf();
    FileSystem fs = FileSystem.get(conf);
    fs.delete(rootDir, true);
    super.tearDown();
  }

  protected void _testMultipleOutputs(boolean withCounters) throws Exception {
    Path inDir = getDir(IN_DIR);
    Path outDir = getDir(OUT_DIR);

    JobConf conf = createJobConf();
    FileSystem fs = FileSystem.get(conf);

    DataOutputStream file = fs.create(new Path(inDir, "part-0"));
    file.writeBytes("a\nb\n\nc\nd\ne");
    file.close();

    file = fs.create(new Path(inDir, "part-1"));
    file.writeBytes("a\nb\n\nc\nd\ne");
    file.close();

    Job job = new Job(conf);
    job.setJobName("mo");
    job.setInputFormatClass(TextInputFormat.class);

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);

    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);

    MultipleOutputs.addNamedOutput(job, "text", TextOutputFormat.class,
      LongWritable.class, Text.class);

    MultipleOutputs.setCountersEnabled(job, withCounters);

    job.setMapperClass(MOMap.class);
    job.setReducerClass(MOReduce.class);

    FileInputFormat.setInputPaths(job, inDir);
    FileOutputFormat.setOutputPath(job, outDir);

    job.waitForCompletion(false);

    // assert number of named output part files
    int namedOutputCount = 0;
    FileStatus[] statuses = fs.listStatus(outDir);
    
    for (FileStatus status : statuses) {
      if (status.getPath().getName().equals("text-m-00000") ||
        status.getPath().getName().equals("text-m-00001") ||
        status.getPath().getName().equals("text-r-00000")) {
        namedOutputCount++;
      }
    }
    assertEquals(3, namedOutputCount);

    // assert TextOutputFormat files correctness
    JobContext jobContext = new JobContext(job.getConfiguration(), new JobID());
    BufferedReader reader = new BufferedReader(
      new InputStreamReader(fs.open(
        new Path(FileOutputFormat.getOutputPath(jobContext), "text-r-00000"))));
    int count = 0;
    String line = reader.readLine();
    while (line != null) {
      assertTrue(line.endsWith("text"));
      line = reader.readLine();
      count++;
    }
    reader.close();
    assertFalse(count == 0);

    CounterGroup counters =
      job.getCounters().getGroup(MultipleOutputs.class.getName());
    if (!withCounters) {
      assertEquals(0, counters.size());
    }
    else {
      assertEquals(1, counters.size());
      assertEquals(4, counters.findCounter("text").getValue());
    }

  }

  @SuppressWarnings({"unchecked"})
  public static class MOMap extends Mapper<LongWritable, Text, LongWritable,
    Text> {

    private MultipleOutputs mos;

    @Override
    protected void setup(Context context) {
      mos = new MultipleOutputs(context);
    }
    
    @Override
    public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
      if (!value.toString().equals("a")) {
        context.write(key, value);
      } else {
        mos.write("text", key, new Text("text"));
      }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      mos.close();
    }
  }

  @SuppressWarnings({"unchecked"})
  public static class MOReduce extends Reducer<LongWritable, Text,
    LongWritable, Text> {

    private MultipleOutputs mos;

    @Override
    protected void setup(Context context) {
      mos = new MultipleOutputs(context);
    }

    @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
      for (Text value : values) {
        if (!value.toString().equals("b")) {
          context.write(key, value);
        } else {
          mos.write("text", key, new Text("text"));
        }
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      mos.close();
    }
  }

}
