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

package org.apache.hadoop.mapreduce;

import org.junit.jupiter.api.AfterEach;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.test.GenericTestUtils;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestNewCombinerGrouping {
  private static File testRootDir = GenericTestUtils.getRandomizedTestDir();

  public static class Map extends
      Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable key, Text value,
        Context context)
        throws IOException, InterruptedException {
      String v = value.toString();
      String k = v.substring(0, v.indexOf(","));
      v = v.substring(v.indexOf(",") + 1);
      context.write(new Text(k), new LongWritable(Long.parseLong(v)));
    }
  }

  public static class Reduce extends
      Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values,
        Context context)
        throws IOException, InterruptedException {
      LongWritable maxValue = null;
      for (LongWritable value : values) {
        if (maxValue == null) {
          maxValue = value;
        } else if (value.compareTo(maxValue) > 0) {
          maxValue = value;
        }
      }
      context.write(key, maxValue);
    }
  }

  public static class Combiner extends Reduce {
  }

  public static class GroupComparator implements RawComparator<Text> {
    @Override
    public int compare(byte[] bytes, int i, int i2, byte[] bytes2, int i3,
        int i4) {
      byte[] b1 = new byte[i2];
      System.arraycopy(bytes, i, b1, 0, i2);

      byte[] b2 = new byte[i4];
      System.arraycopy(bytes2, i3, b2, 0, i4);

      return compare(new Text(new String(b1)), new Text(new String(b2)));
    }

    @Override
    public int compare(Text o1, Text o2) {
      String s1 = o1.toString();
      String s2 = o2.toString();
      s1 = s1.substring(0, s1.indexOf("|"));
      s2 = s2.substring(0, s2.indexOf("|"));
      return s1.compareTo(s2);
    }

  }

  @AfterEach
  public void cleanup() {
    FileUtil.fullyDelete(testRootDir);
  }

  @Test
  public void testCombiner() throws Exception {
    if (!testRootDir.mkdirs()) {
      throw new RuntimeException("Could not create test dir: " + testRootDir);
    }
    File in = new File(testRootDir, "input");
    if (!in.mkdirs()) {
      throw new RuntimeException("Could not create test dir: " + in);
    }
    File out = new File(testRootDir, "output");
    PrintWriter pw = new PrintWriter(new FileWriter(new File(in, "data.txt")));
    pw.println("A|a,1");
    pw.println("A|b,2");
    pw.println("B|a,3");
    pw.println("B|b,4");
    pw.println("B|c,5");
    pw.close();
    JobConf conf = new JobConf();
    conf.set("mapreduce.framework.name", "local");
    Job job = new Job(conf);
    TextInputFormat.setInputPaths(job, new Path(in.getPath()));
    TextOutputFormat.setOutputPath(job, new Path(out.getPath()));

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setGroupingComparatorClass(GroupComparator.class);

    job.setCombinerKeyGroupingComparatorClass(GroupComparator.class);
    job.setCombinerClass(Combiner.class);
    job.getConfiguration().setInt("min.num.spills.for.combine", 0);

    job.submit();
    job.waitForCompletion(false);
    if (job.isSuccessful()) {
      Counters counters = job.getCounters();

      long combinerInputRecords = counters.findCounter(
          "org.apache.hadoop.mapreduce.TaskCounter",
          "COMBINE_INPUT_RECORDS").getValue();
      long combinerOutputRecords = counters.findCounter(
          "org.apache.hadoop.mapreduce.TaskCounter",
          "COMBINE_OUTPUT_RECORDS").getValue();
      assertTrue(combinerInputRecords > 0);
      assertTrue(combinerInputRecords > combinerOutputRecords);

      BufferedReader br = new BufferedReader(new FileReader(
          new File(out, "part-r-00000")));
      Set<String> output = new HashSet<String>();
      String line = br.readLine();
      assertNotNull(line);
      output.add(line.substring(0, 1) + line.substring(4, 5));
      line = br.readLine();
      assertNotNull(line);
      output.add(line.substring(0, 1) + line.substring(4, 5));
      line = br.readLine();
      assertNull(line);
      br.close();

      Set<String> expected = new HashSet<String>();
      expected.add("A2");
      expected.add("B5");

      assertEquals(expected, output);

    } else {
      fail("Job failed");
    }
  }

}
