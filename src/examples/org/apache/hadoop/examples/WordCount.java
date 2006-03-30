/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.examples;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.MapReduceBase;

/**
 * This is an example Hadoop Map/Reduce application.
 * It reads the text input files, breaks each line into words
 * and counts them. The output is a locally sorted list of words and the 
 * count of how often they occurred.
 *
 * To run: bin/hadoop jar build/hadoop-examples.jar wordcount
 *            [-m <i>maps</i>] [-r <i>reduces</i>] <i>in-dir</i> <i>out-dir</i> 
 *
 * @author Owen O'Malley
 */
public class WordCount {
  
  /**
   * Counts the words in each line.
   * For each line of input, break the line into words and emit them as
   * (<b>word</b>, <b>1</b>).
   */
  public static class MapClass extends MapReduceBase implements Mapper {
    
    private final static IntWritable one = new IntWritable(1);
    private UTF8 word = new UTF8();
    
    public void map(WritableComparable key, Writable value, 
        OutputCollector output, 
        Reporter reporter) throws IOException {
      String line = ((UTF8)value).toString();
      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        output.collect(word, one);
      }
    }
  }
  
  /**
   * A reducer class that just emits the sum of the input values.
   */
  public static class Reduce extends MapReduceBase implements Reducer {
    
    public void reduce(WritableComparable key, Iterator values,
        OutputCollector output, 
        Reporter reporter) throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += ((IntWritable) values.next()).get();
      }
      output.collect(key, new IntWritable(sum));
    }
  }
  
  static void printUsage() {
    System.out.println("wordcount [-m <maps>] [-r <reduces>] <input> <output>");
    System.exit(1);
  }
  
  /**
   * The main driver for word count map/reduce program.
   * Invoke this method to submit the map/reduce job.
   * @throws IOException When there is communication problems with the 
   *                     job tracker.
   */
  public static void main(String[] args) throws IOException {
    Configuration defaults = new Configuration();
    
    JobConf conf = new JobConf(defaults, WordCount.class);
    conf.setJobName("wordcount");
 
    // the keys are words (strings)
    conf.setOutputKeyClass(UTF8.class);
    // the values are counts (ints)
    conf.setOutputValueClass(IntWritable.class);
    
    conf.setMapperClass(MapClass.class);        
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);
    
    List other_args = new ArrayList();
    for(int i=0; i < args.length; ++i) {
      try {
        if ("-m".equals(args[i])) {
          conf.setNumMapTasks(Integer.parseInt(args[++i]));
        } else if ("-r".equals(args[i])) {
          conf.setNumReduceTasks(Integer.parseInt(args[++i]));
        } else {
          other_args.add(args[i]);
        }
      } catch (NumberFormatException except) {
        System.out.println("ERROR: Integer expected instead of " + args[i]);
        printUsage();
      } catch (ArrayIndexOutOfBoundsException except) {
        System.out.println("ERROR: Required parameter missing from " +
                           args[i-1]);
        printUsage(); // exits
      }
    }
    // Make sure there are exactly 2 parameters left.
    if (other_args.size() != 2) {
      System.out.println("ERROR: Wrong number of parameters: " +
          other_args.size() + " instead of 2.");
      printUsage();
    }
    conf.setInputDir(new File((String) other_args.get(0)));
    conf.setOutputDir(new File((String) other_args.get(1)));
    
    // Uncomment to run locally in a single process
    // conf.set("mapred.job.tracker", "local");
    
    JobClient.runJob(conf);
  }
  
}
