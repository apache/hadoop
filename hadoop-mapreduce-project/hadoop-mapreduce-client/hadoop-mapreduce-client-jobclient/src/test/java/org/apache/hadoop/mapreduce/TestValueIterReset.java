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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Utils;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;

/**
 * A JUnit test to test the Map-Reduce framework's support for the
 * "mark-reset" functionality in Reduce Values Iterator
 */
public class TestValueIterReset {
  private static final int NUM_MAPS = 1;
  private static final int NUM_TESTS = 4;
  private static final int NUM_VALUES = 40;

  private static Path TEST_ROOT_DIR =
    new Path(System.getProperty("test.build.data","/tmp"));
  private static Configuration conf = new Configuration();
  private static FileSystem localFs;
  static {
    try {
      localFs = FileSystem.getLocal(conf);
    } catch (IOException io) {
      throw new RuntimeException("problem getting local fs", io);
    }
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(TestValueIterReset.class);

  public static class TestMapper 
  extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

      IntWritable outKey = new IntWritable();
      IntWritable outValue = new IntWritable();

      for (int j = 0; j < NUM_TESTS; j++) {
        for (int i = 0; i < NUM_VALUES; i++) {
          outKey.set(j);
          outValue.set(i);
          context.write(outKey, outValue);
        }
      }
    }
  }

  public static class TestReducer 
  extends Reducer< IntWritable,IntWritable,IntWritable,IntWritable> {

    public void reduce(IntWritable key, Iterable<IntWritable> values, 
        Context context) throws IOException, InterruptedException {

      int errors = 0;

      MarkableIterator<IntWritable> mitr = 
        new MarkableIterator<IntWritable>(values.iterator());

      switch (key.get()) {
      case 0:
        errors += test0(key, mitr);
        break;
      case 1:
        errors += test1(key, mitr);
        break;
      case 2:
        errors += test2(key, mitr);
        break;
      case 3:
        errors += test3(key, mitr);
        break;
      default:
        break;
      }
      context.write(key, new IntWritable(errors));
    }
  }

  /**
   * Test the most common use case. Mark before start of the iteration and
   * reset at the end to go over the entire list
   * @param key
   * @param values
   * @return
   * @throws IOException
   */

  private static int test0(IntWritable key,
                           MarkableIterator<IntWritable> values)
  throws IOException {

    int errors = 0;
    IntWritable i;
    ArrayList<IntWritable> expectedValues = new ArrayList<IntWritable>();
    

    LOG.info("Executing TEST:0 for Key:"+ key.toString());

    values.mark();
    LOG.info("TEST:0. Marking");

    while (values.hasNext()) {
      i = values.next();
      expectedValues.add(i);
      LOG.info(key + ":" + i);
    }

    values.reset();
    LOG.info("TEST:0. Reset");

    int count = 0;

    while (values.hasNext()) {
      i = values.next();
      LOG.info(key + ":" + i);
      if (i != expectedValues.get(count)) {
        LOG.info("TEST:0. Check:1 Expected: " + expectedValues.get(count) +
            ", Got: " + i);
        errors ++;
        return errors;
      }
      count ++;
    }

    LOG.info("TEST:0 Done");
    return errors;
  }

  /**
   * Test the case where we do a mark outside of a reset. Test for both file
   * and memory caches
   * @param key
   * @param values
   * @return
   * @throws IOException
   */
  private static int test1(IntWritable key, 
                           MarkableIterator<IntWritable> values)
  throws IOException {

    IntWritable i;
    int errors = 0;
    int count = 0;
    
    ArrayList<IntWritable> expectedValues = new ArrayList<IntWritable>();
    ArrayList<IntWritable> expectedValues1 = new ArrayList<IntWritable>();

    LOG.info("Executing TEST:1 for Key:" + key);

    values.mark();
    LOG.info("TEST:1. Marking");

    while (values.hasNext()) {
      i = values.next();
      LOG.info(key + ":" + i);
      expectedValues.add(i);
      if (count == 2) {
        break;
      }
      count ++;
    }

    values.reset();
    LOG.info("TEST:1. Reset");
    count = 0;

    while (values.hasNext()) {
      i = values.next();
      LOG.info(key + ":" + i);

      if (count < expectedValues.size()) {
        if (i != expectedValues.get(count)) {
          errors ++;
          LOG.info("TEST:1. Check:1 Expected: " + expectedValues.get(count) +
              ", Got: " + i);
          return errors;
        }
      }
      
      // We have moved passed the first mark, but still in the memory cache
      if (count == 3) {
        values.mark();
        LOG.info("TEST:1. Marking -- " + key + ": " + i);
      }

      if (count >= 3) {
        expectedValues1.add(i);
      }
      
      if (count == 5) {
        break;
      }
      count ++;
    }

    if (count < expectedValues.size()) {
      LOG.info(("TEST:1 Check:2. Iterator returned lesser values"));
      errors ++;
      return errors;
    }
    
    values.reset();
    count = 0;
    LOG.info("TEST:1. Reset");
    expectedValues.clear();

    while (values.hasNext()) {
      i = values.next();
      LOG.info(key + ":" + i);

      if (count < expectedValues1.size()) {
        if (i != expectedValues1.get(count)) {
          errors ++;
          LOG.info("TEST:1. Check:3 Expected: " + expectedValues1.get(count)
              + ", Got: " + i);
          return errors;
        }
      }
      
      // We have moved passed the previous mark, but now we are in the file
      // cache
      if (count == 25) {
        values.mark();
        LOG.info("TEST:1. Marking -- " + key + ":" + i);
      }
      
      if (count >= 25) {
        expectedValues.add(i);
      }
      count ++;
    }

    if (count < expectedValues1.size()) {
      LOG.info(("TEST:1 Check:4. Iterator returned fewer values"));
      errors ++;
      return errors;
    }

    values.reset();
    LOG.info("TEST:1. Reset");
    count = 0;

    while (values.hasNext()) {
      i = values.next();
      LOG.info(key + ":" + i);

      if (i != expectedValues.get(count)) {
        errors ++;
        LOG.info("TEST:1. Check:5 Expected: " + expectedValues.get(count)
            + ", Got: " + i);
        return errors;
      }
    }

    LOG.info("TEST:1 Done");
    return errors;
  }

  /**
   * Test the case where we do a mark inside a reset. Test for both file
   * and memory
   * @param key
   * @param values
   * @return
   * @throws IOException
   */
  private static int test2(IntWritable key,
                           MarkableIterator<IntWritable> values)
  throws IOException {

    IntWritable i;
    int errors = 0;
    int count = 0;
    
    ArrayList<IntWritable> expectedValues = new ArrayList<IntWritable>();
    ArrayList<IntWritable> expectedValues1 = new ArrayList<IntWritable>();

    LOG.info("Executing TEST:2 for Key:" + key);

    values.mark();
    LOG.info("TEST:2 Marking");

    while (values.hasNext()) {
      i = values.next();
      LOG.info(key + ":" + i);
      expectedValues.add(i);
      if (count == 8) {
        break;
      }
      count ++;
    }

    values.reset();
    count = 0;
    LOG.info("TEST:2 reset");

    while (values.hasNext()) {
      i = values.next();
      LOG.info(key + ":" + i);
      
      if (count < expectedValues.size()) {
        if (i != expectedValues.get(count)) {
          errors ++;
          LOG.info("TEST:2. Check:1 Expected: " + expectedValues.get(count)
              + ", Got: " + i);
          return errors;
        }
      }

      // We have moved passed the first mark, but still reading from the
      // memory cache
      if (count == 3) {
        values.mark();
        LOG.info("TEST:2. Marking -- " + key + ":" + i);
      }
      
      if (count >= 3) {
        expectedValues1.add(i);
      }
      count ++;
    }

    values.reset();
    LOG.info("TEST:2. Reset");
    expectedValues.clear();
    count = 0;

    while (values.hasNext()) {
      i = values.next();
      LOG.info(key + ":" + i);

      if (count < expectedValues1.size()) {
        if (i != expectedValues1.get(count)) {
          errors ++;
          LOG.info("TEST:2. Check:2 Expected: " + expectedValues1.get(count)
              + ", Got: " + i);
          return errors;
        }
      }
      
      // We have moved passed the previous mark, but now we are in the file
      // cache
      if (count == 20) {
        values.mark();
        LOG.info("TEST:2. Marking -- " + key + ":" + i);
      }
      
      if (count >= 20) {
        expectedValues.add(i);
      }
      count ++;
    }

    values.reset();
    count = 0;
    LOG.info("TEST:2. Reset");

    while (values.hasNext()) {
      i = values.next();
      LOG.info(key + ":" + i);

      if (i != expectedValues.get(count)) {
        errors ++;
        LOG.info("TEST:2. Check:1 Expected: " + expectedValues.get(count)
            + ", Got: " + i);
        return errors;
      }
    }

    LOG.info("TEST:2 Done");
    return errors;
  }

  /**
   * Test "clearMark"
   * @param key
   * @param values
   * @return
   * @throws IOException
   */
  private static int test3(IntWritable key,
                              MarkableIterator<IntWritable> values)
  throws IOException {

    int errors = 0;
    IntWritable i;

    ArrayList<IntWritable> expectedValues = new ArrayList<IntWritable>();

    LOG.info("Executing TEST:3 for Key:" + key);

    values.mark();
    LOG.info("TEST:3. Marking");
    int count = 0;

    while (values.hasNext()) {
      i = values.next();;
      LOG.info(key + ":" + i);
      
      if (count == 5) {
        LOG.info("TEST:3. Clearing Mark");
        values.clearMark();
      }

      if (count == 8) {
        LOG.info("TEST:3. Marking -- " + key + ":" + i);
        values.mark();
      }
      
      if (count >= 8) {
        expectedValues.add(i);
      }
      count ++;
    }

    values.reset();
    LOG.info("TEST:3. After reset");

    if (!values.hasNext()) {
      errors ++;
      LOG.info("TEST:3, Check:1. HasNext returned false");
      return errors;
    }

    count = 0;
    
    while (values.hasNext()) {
      i = values.next();
      LOG.info(key + ":" + i);
      
      if (count < expectedValues.size()) {
        if (i != expectedValues.get(count)) {
          errors ++;
          LOG.info("TEST:2. Check:1 Expected: " + expectedValues.get(count)
              + ", Got: " + i);
          return errors;
        }
      }

      if (count == 10) {
        values.clearMark();
        LOG.info("TEST:3. After clear mark");
      }
      count ++;
    }

    boolean successfulClearMark = false;
    try {
      LOG.info("TEST:3. Before Reset");
      values.reset();
    } catch (IOException e) {
      successfulClearMark = true;
    }
    
    if (!successfulClearMark) {
      LOG.info("TEST:3 Check:4 reset was successfule even after clearMark");
      errors ++;
      return errors;
    }
    
    LOG.info("TEST:3 Done.");
    return errors;
  }


  public void createInput() throws Exception {
    // Just create one line files. We use this only to
    // control the number of map tasks
    for (int i = 0; i < NUM_MAPS; i++) {
      Path file = new Path(TEST_ROOT_DIR+"/in", "test" + i + ".txt");
      localFs.delete(file, false);
      OutputStream os = localFs.create(file);
      Writer wr = new OutputStreamWriter(os);
      wr.write("dummy");
      wr.close();
    }
  }

  @Test
  public void testValueIterReset() {
    try {
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "TestValueIterReset") ;
      job.setJarByClass(TestValueIterReset.class);
      job.setMapperClass(TestMapper.class);
      job.setReducerClass(TestReducer.class);
      job.setNumReduceTasks(NUM_TESTS);
      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(IntWritable.class);
      job.setOutputKeyClass(IntWritable.class);
      job.setOutputValueClass(IntWritable.class);
      job.getConfiguration().
        setInt(MRJobConfig.REDUCE_MARKRESET_BUFFER_SIZE,128);  
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      FileInputFormat.addInputPath(job,
          new Path(TEST_ROOT_DIR + "/in"));
      Path output = new Path(TEST_ROOT_DIR + "/out");
      localFs.delete(output, true);
      FileOutputFormat.setOutputPath(job, output);
      createInput();
      assertTrue(job.waitForCompletion(true));
      validateOutput();
    } catch (Exception e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }

  private void validateOutput() throws IOException {
    Path[] outputFiles = FileUtil.stat2Paths(
        localFs.listStatus(new Path(TEST_ROOT_DIR + "/out"),
            new Utils.OutputFileUtils.OutputFilesFilter()));
    if (outputFiles.length > 0) {
      InputStream is = localFs.open(outputFiles[0]);
      BufferedReader reader = new BufferedReader(new InputStreamReader(is));
      String line = reader.readLine();
      while (line != null) {
        StringTokenizer tokeniz = new StringTokenizer(line, "\t");
        String key = tokeniz.nextToken();
        String value = tokeniz.nextToken();
        LOG.info("Output: key: "+ key + " value: "+ value);
        int errors = Integer.parseInt(value);
        assertTrue(errors == 0);
        line = reader.readLine();
      }   
      reader.close();
    }
  }
}
