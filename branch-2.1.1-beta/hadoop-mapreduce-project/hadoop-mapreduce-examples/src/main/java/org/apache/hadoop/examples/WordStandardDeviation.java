package org.apache.hadoop.examples;

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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Charsets;

public class WordStandardDeviation extends Configured implements Tool {

  private double stddev = 0;

  private final static Text LENGTH = new Text("length");
  private final static Text SQUARE = new Text("square");
  private final static Text COUNT = new Text("count");
  private final static LongWritable ONE = new LongWritable(1);

  /**
   * Maps words from line of text into 3 key-value pairs; one key-value pair for
   * counting the word, one for counting its length, and one for counting the
   * square of its length.
   */
  public static class WordStandardDeviationMapper extends
      Mapper<Object, Text, Text, LongWritable> {

    private LongWritable wordLen = new LongWritable();
    private LongWritable wordLenSq = new LongWritable();

    /**
     * Emits 3 key-value pairs for counting the word, its length, and the
     * squares of its length. Outputs are (Text, LongWritable).
     * 
     * @param value
     *          This will be a line of text coming in from our input file.
     */
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        String string = itr.nextToken();

        this.wordLen.set(string.length());

        // the square of an integer is an integer...
        this.wordLenSq.set((long) Math.pow(string.length(), 2.0));

        context.write(LENGTH, this.wordLen);
        context.write(SQUARE, this.wordLenSq);
        context.write(COUNT, ONE);
      }
    }
  }

  /**
   * Performs integer summation of all the values for each key.
   */
  public static class WordStandardDeviationReducer extends
      Reducer<Text, LongWritable, Text, LongWritable> {

    private LongWritable val = new LongWritable();

    /**
     * Sums all the individual values within the iterator and writes them to the
     * same key.
     * 
     * @param key
     *          This will be one of 2 constants: LENGTH_STR, COUNT_STR, or
     *          SQUARE_STR.
     * @param values
     *          This will be an iterator of all the values associated with that
     *          key.
     */
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {

      int sum = 0;
      for (LongWritable value : values) {
        sum += value.get();
      }
      val.set(sum);
      context.write(key, val);
    }
  }

  /**
   * Reads the output file and parses the summation of lengths, the word count,
   * and the lengths squared, to perform a quick calculation of the standard
   * deviation.
   * 
   * @param path
   *          The path to find the output file in. Set in main to the output
   *          directory.
   * @throws IOException
   *           If it cannot access the output directory, we throw an exception.
   */
  private double readAndCalcStdDev(Path path, Configuration conf)
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Path file = new Path(path, "part-r-00000");

    if (!fs.exists(file))
      throw new IOException("Output not found!");

    double stddev = 0;
    BufferedReader br = null;
    try {
      br = new BufferedReader(new InputStreamReader(fs.open(file), Charsets.UTF_8));
      long count = 0;
      long length = 0;
      long square = 0;
      String line;
      while ((line = br.readLine()) != null) {
        StringTokenizer st = new StringTokenizer(line);

        // grab type
        String type = st.nextToken();

        // differentiate
        if (type.equals(COUNT.toString())) {
          String countLit = st.nextToken();
          count = Long.parseLong(countLit);
        } else if (type.equals(LENGTH.toString())) {
          String lengthLit = st.nextToken();
          length = Long.parseLong(lengthLit);
        } else if (type.equals(SQUARE.toString())) {
          String squareLit = st.nextToken();
          square = Long.parseLong(squareLit);
        }
      }
      // average = total sum / number of elements;
      double mean = (((double) length) / ((double) count));
      // standard deviation = sqrt((sum(lengths ^ 2)/count) - (mean ^ 2))
      mean = Math.pow(mean, 2.0);
      double term = (((double) square / ((double) count)));
      stddev = Math.sqrt((term - mean));
      System.out.println("The standard deviation is: " + stddev);
    } finally {
      if (br != null) {
        br.close();
      }
    }
    return stddev;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new WordStandardDeviation(),
        args);
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: wordstddev <in> <out>");
      return 0;
    }

    Configuration conf = getConf();

    @SuppressWarnings("deprecation")
    Job job = new Job(conf, "word stddev");
    job.setJarByClass(WordStandardDeviation.class);
    job.setMapperClass(WordStandardDeviationMapper.class);
    job.setCombinerClass(WordStandardDeviationReducer.class);
    job.setReducerClass(WordStandardDeviationReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    Path outputpath = new Path(args[1]);
    FileOutputFormat.setOutputPath(job, outputpath);
    boolean result = job.waitForCompletion(true);

    // read output and calculate standard deviation
    stddev = readAndCalcStdDev(outputpath, conf);

    return (result ? 0 : 1);
  }

  public double getStandardDeviation() {
    return stddev;
  }
}