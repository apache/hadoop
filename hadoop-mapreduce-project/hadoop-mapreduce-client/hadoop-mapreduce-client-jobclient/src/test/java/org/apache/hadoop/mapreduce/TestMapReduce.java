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
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.File;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**********************************************************
 * MapredLoadTest generates a bunch of work that exercises
 * a Hadoop Map-Reduce system (and DFS, too).  It goes through
 * the following steps:
 *
 * 1) Take inputs 'range' and 'counts'.
 * 2) Generate 'counts' random integers between 0 and range-1.
 * 3) Create a file that lists each integer between 0 and range-1,
 *    and lists the number of times that integer was generated.
 * 4) Emit a (very large) file that contains all the integers
 *    in the order generated.
 * 5) After the file has been generated, read it back and count
 *    how many times each int was generated.
 * 6) Compare this big count-map against the original one.  If
 *    they match, then SUCCESS!  Otherwise, FAILURE!
 *
 * OK, that's how we can think about it.  What are the map-reduce
 * steps that get the job done?
 *
 * 1) In a non-mapred thread, take the inputs 'range' and 'counts'.
 * 2) In a non-mapread thread, generate the answer-key and write to disk.
 * 3) In a mapred job, divide the answer key into K jobs.
 * 4) A mapred 'generator' task consists of K map jobs.  Each reads
 *    an individual "sub-key", and generates integers according to
 *    to it (though with a random ordering).
 * 5) The generator's reduce task agglomerates all of those files
 *    into a single one.
 * 6) A mapred 'reader' task consists of M map jobs.  The output
 *    file is cut into M pieces. Each of the M jobs counts the 
 *    individual ints in its chunk and creates a map of all seen ints.
 * 7) A mapred job integrates all the count files into a single one.
 *
 **********************************************************/
public class TestMapReduce {
  private static final File TEST_DIR = new File(
      System.getProperty("test.build.data",
          System.getProperty("java.io.tmpdir")), "TestMapReduce-mapreduce");
  private static FileSystem fs;
  
  static {
    try {
       fs = FileSystem.getLocal(new Configuration());
    } catch (IOException ioe) {
      fs = null;
    }
  }
  
  /**
   * Modified to make it a junit test.
   * The RandomGen Job does the actual work of creating
   * a huge file of assorted numbers.  It receives instructions
   * as to how many times each number should be counted.  Then
   * it emits those numbers in a crazy order.
   *
   * The map() function takes a key/val pair that describes
   * a value-to-be-emitted (the key) and how many times it 
   * should be emitted (the value), aka "numtimes".  map() then
   * emits a series of intermediate key/val pairs.  It emits
   * 'numtimes' of these.  The key is a random number and the
   * value is the 'value-to-be-emitted'.
   *
   * The system collates and merges these pairs according to
   * the random number.  reduce() function takes in a key/value
   * pair that consists of a crazy random number and a series
   * of values that should be emitted.  The random number key
   * is now dropped, and reduce() emits a pair for every intermediate value.
   * The emitted key is an intermediate value.  The emitted value
   * is just a blank string.  Thus, we've created a huge file
   * of numbers in random order, but where each number appears
   * as many times as we were instructed.
   */
  static class RandomGenMapper
      extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable> {
    
    public void map(IntWritable key, IntWritable val,
        Context context) throws IOException, InterruptedException {
      int randomVal = key.get();
      int randomCount = val.get();

      for (int i = 0; i < randomCount; i++) {
        context.write(new IntWritable(Math.abs(r.nextInt())),
          new IntWritable(randomVal));
      }
    }
  }
  /**
   */
  static class RandomGenReducer
      extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    
    public void reduce(IntWritable key, Iterable<IntWritable> it,
        Context context) throws IOException, InterruptedException {
      for (IntWritable iw : it) {
        context.write(iw, null);
      }
    }
  }

  /**
   * The RandomCheck Job does a lot of our work.  It takes
   * in a num/string keyspace, and transforms it into a
   * key/count(int) keyspace.
   *
   * The map() function just emits a num/1 pair for every
   * num/string input pair.
   *
   * The reduce() function sums up all the 1s that were
   * emitted for a single key.  It then emits the key/total
   * pair.
   *
   * This is used to regenerate the random number "answer key".
   * Each key here is a random number, and the count is the
   * number of times the number was emitted.
   */
  static class RandomCheckMapper
      extends Mapper<WritableComparable<?>, Text, IntWritable, IntWritable> {
    
    public void map(WritableComparable<?> key, Text val,
        Context context) throws IOException, InterruptedException {
      context.write(new IntWritable(
        Integer.parseInt(val.toString().trim())), new IntWritable(1));
    }
  }
  
  /**
   */
  static class RandomCheckReducer
      extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    public void reduce(IntWritable key, Iterable<IntWritable> it,
        Context context) throws IOException, InterruptedException {
      int keyint = key.get();
      int count = 0;
      for (IntWritable iw : it) {
        count++;
      }
      context.write(new IntWritable(keyint), new IntWritable(count));
    }
  }

  /**
   * The Merge Job is a really simple one.  It takes in
   * an int/int key-value set, and emits the same set.
   * But it merges identical keys by adding their values.
   *
   * Thus, the map() function is just the identity function
   * and reduce() just sums.  Nothing to see here!
   */
  static class MergeMapper
      extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable> {
    
    public void map(IntWritable key, IntWritable val,
        Context context) throws IOException, InterruptedException {
      int keyint = key.get();
      int valint = val.get();
      context.write(new IntWritable(keyint), new IntWritable(valint));
    }
  }
  
  static class MergeReducer
      extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    public void reduce(IntWritable key, Iterator<IntWritable> it,
        Context context) throws IOException, InterruptedException {
      int keyint = key.get();
      int total = 0;
      while (it.hasNext()) {
        total += it.next().get();
      }
      context.write(new IntWritable(keyint), new IntWritable(total));
    }
  }

  private static int range = 10;
  private static int counts = 100;
  private static Random r = new Random();

  @After
  public void cleanup() {
    FileUtil.fullyDelete(TEST_DIR);
  }

  @Test
  public void testMapred() throws Exception {
    launch();
  }

  private static void launch() throws Exception {
    //
    // Generate distribution of ints.  This is the answer key.
    //
    Configuration conf = new Configuration();
    int countsToGo = counts;
    int dist[] = new int[range];
    for (int i = 0; i < range; i++) {
      double avgInts = (1.0 * countsToGo) / (range - i);
      dist[i] = (int) Math.max(0, Math.round(avgInts + 
        (Math.sqrt(avgInts) * r.nextGaussian())));
      countsToGo -= dist[i];
    }
    if (countsToGo > 0) {
      dist[dist.length-1] += countsToGo;
    }

    //
    // Write the answer key to a file.  
    //
    Path testdir = new Path(TEST_DIR.getAbsolutePath());
    if (!fs.mkdirs(testdir)) {
      throw new IOException("Mkdirs failed to create " + testdir.toString());
    }

    Path randomIns = new Path(testdir, "genins");
    if (!fs.mkdirs(randomIns)) {
      throw new IOException("Mkdirs failed to create " + randomIns.toString());
    }

    Path answerkey = new Path(randomIns, "answer.key");
    SequenceFile.Writer out = 
      SequenceFile.createWriter(fs, conf, answerkey, IntWritable.class,
                                IntWritable.class, 
                                SequenceFile.CompressionType.NONE);
    try {
      for (int i = 0; i < range; i++) {
        out.append(new IntWritable(i), new IntWritable(dist[i]));
      }
    } finally {
      out.close();
    }
    
    printFiles(randomIns, conf);

    //
    // Now we need to generate the random numbers according to
    // the above distribution.
    //
    // We create a lot of map tasks, each of which takes at least
    // one "line" of the distribution.  (That is, a certain number
    // X is to be generated Y number of times.)
    //
    // A map task emits Y key/val pairs.  The val is X.  The key
    // is a randomly-generated number.
    //
    // The reduce task gets its input sorted by key.  That is, sorted
    // in random order.  It then emits a single line of text that
    // for the given values.  It does not emit the key.
    //
    // Because there's just one reduce task, we emit a single big
    // file of random numbers.
    //
    Path randomOuts = new Path(testdir, "genouts");
    fs.delete(randomOuts, true);


    Job genJob = Job.getInstance(conf);
    FileInputFormat.setInputPaths(genJob, randomIns);
    genJob.setInputFormatClass(SequenceFileInputFormat.class);
    genJob.setMapperClass(RandomGenMapper.class);

    FileOutputFormat.setOutputPath(genJob, randomOuts);
    genJob.setOutputKeyClass(IntWritable.class);
    genJob.setOutputValueClass(IntWritable.class);
    genJob.setReducerClass(RandomGenReducer.class);
    genJob.setNumReduceTasks(1);

    genJob.waitForCompletion(true);
    printFiles(randomOuts, conf);

    //
    // Next, we read the big file in and regenerate the 
    // original map.  It's split into a number of parts.
    // (That number is 'intermediateReduces'.)
    //
    // We have many map tasks, each of which read at least one
    // of the output numbers.  For each number read in, the
    // map task emits a key/value pair where the key is the
    // number and the value is "1".
    //
    // We have a single reduce task, which receives its input
    // sorted by the key emitted above.  For each key, there will
    // be a certain number of "1" values.  The reduce task sums
    // these values to compute how many times the given key was
    // emitted.
    //
    // The reduce task then emits a key/val pair where the key
    // is the number in question, and the value is the number of
    // times the key was emitted.  This is the same format as the
    // original answer key (except that numbers emitted zero times
    // will not appear in the regenerated key.)  The answer set
    // is split into a number of pieces.  A final MapReduce job
    // will merge them.
    //
    // There's not really a need to go to 10 reduces here 
    // instead of 1.  But we want to test what happens when
    // you have multiple reduces at once.
    //
    int intermediateReduces = 10;
    Path intermediateOuts = new Path(testdir, "intermediateouts");
    fs.delete(intermediateOuts, true);
    Job checkJob = Job.getInstance(conf);
    FileInputFormat.setInputPaths(checkJob, randomOuts);
    checkJob.setMapperClass(RandomCheckMapper.class);

    FileOutputFormat.setOutputPath(checkJob, intermediateOuts);
    checkJob.setOutputKeyClass(IntWritable.class);
    checkJob.setOutputValueClass(IntWritable.class);
    checkJob.setOutputFormatClass(MapFileOutputFormat.class);
    checkJob.setReducerClass(RandomCheckReducer.class);
    checkJob.setNumReduceTasks(intermediateReduces);
    checkJob.waitForCompletion(true);
    printFiles(intermediateOuts, conf); 

    //
    // OK, now we take the output from the last job and
    // merge it down to a single file.  The map() and reduce()
    // functions don't really do anything except reemit tuples.
    // But by having a single reduce task here, we end up merging
    // all the files.
    //
    Path finalOuts = new Path(testdir, "finalouts");
    fs.delete(finalOuts, true);
    Job mergeJob = Job.getInstance(conf);
    FileInputFormat.setInputPaths(mergeJob, intermediateOuts);
    mergeJob.setInputFormatClass(SequenceFileInputFormat.class);
    mergeJob.setMapperClass(MergeMapper.class);
        
    FileOutputFormat.setOutputPath(mergeJob, finalOuts);
    mergeJob.setOutputKeyClass(IntWritable.class);
    mergeJob.setOutputValueClass(IntWritable.class);
    mergeJob.setOutputFormatClass(SequenceFileOutputFormat.class);
    mergeJob.setReducerClass(MergeReducer.class);
    mergeJob.setNumReduceTasks(1);
        
    mergeJob.waitForCompletion(true);
    printFiles(finalOuts, conf); 
 
    //
    // Finally, we compare the reconstructed answer key with the
    // original one.  Remember, we need to ignore zero-count items
    // in the original key.
    //
    boolean success = true;
    Path recomputedkey = new Path(finalOuts, "part-r-00000");
    SequenceFile.Reader in = new SequenceFile.Reader(fs, recomputedkey, conf);
    int totalseen = 0;
    try {
      IntWritable key = new IntWritable();
      IntWritable val = new IntWritable();            
      for (int i = 0; i < range; i++) {
        if (dist[i] == 0) {
          continue;
        }
        if (!in.next(key, val)) {
          System.err.println("Cannot read entry " + i);
          success = false;
          break;
        } else {
          if (!((key.get() == i) && (val.get() == dist[i]))) {
            System.err.println("Mismatch!  Pos=" + key.get() + ", i=" + i + 
                               ", val=" + val.get() + ", dist[i]=" + dist[i]);
            success = false;
          }
          totalseen += val.get();
        }
      }
      if (success) {
        if (in.next(key, val)) {
          System.err.println("Unnecessary lines in recomputed key!");
          success = false;
        }
      }
    } finally {
      in.close();
    }
    int originalTotal = 0;
    for (int i = 0; i < dist.length; i++) {
      originalTotal += dist[i];
    }
    System.out.println("Original sum: " + originalTotal);
    System.out.println("Recomputed sum: " + totalseen);

    //
    // Write to "results" whether the test succeeded or not.
    //
    Path resultFile = new Path(testdir, "results");
    BufferedWriter bw = new BufferedWriter(
      new OutputStreamWriter(fs.create(resultFile)));
    try {
      bw.write("Success=" + success + "\n");
      System.out.println("Success=" + success);
    } finally {
      bw.close();
    }
    assertTrue("testMapRed failed", success);
    fs.delete(testdir, true);
  }

  private static void printTextFile(FileSystem fs, Path p) throws IOException {
    BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(p)));
    String line;
    while ((line = in.readLine()) != null) {
      System.out.println("  Row: " + line);
    }
    in.close();
  }

  private static void printSequenceFile(FileSystem fs, Path p, 
      Configuration conf) throws IOException {
    SequenceFile.Reader r = new SequenceFile.Reader(fs, p, conf);
    Object key = null;
    Object value = null;
    while ((key = r.next(key)) != null) {
      value = r.getCurrentValue(value);
      System.out.println("  Row: " + key + ", " + value);
    }
    r.close();    
  }

  private static boolean isSequenceFile(FileSystem fs,
                                        Path f) throws IOException {
    DataInputStream in = fs.open(f);
    try {
      byte[] seq = "SEQ".getBytes();
      for (int i = 0; i < seq.length; ++i) {
        if (seq[i] != in.read()) {
          return false;
        }
      }
    } finally {
      in.close();
    }
    return true;
  }

  private static void printFiles(Path dir, 
                                 Configuration conf) throws IOException {
    FileSystem fs = dir.getFileSystem(conf);
    for(FileStatus f: fs.listStatus(dir)) {
      System.out.println("Reading " + f.getPath() + ": ");
      if (f.isDirectory()) {
        System.out.println("  it is a map file.");
        printSequenceFile(fs, new Path(f.getPath(), "data"), conf);
      } else if (isSequenceFile(fs, f.getPath())) {
        System.out.println("  it is a sequence file.");
        printSequenceFile(fs, f.getPath(), conf);
      } else {
        System.out.println("  it is a text file.");
        printTextFile(fs, f.getPath());
      }
    }
  }

  /**
   * Launches all the tasks in order.
   */
  public static void main(String[] argv) throws Exception {
    if (argv.length < 2) {
      System.err.println("Usage: TestMapReduce <range> <counts>");
      System.err.println();
      System.err.println("Note: a good test will have a <counts> value" +
          " that is substantially larger than the <range>");
      return;
    }

    int i = 0;
    range = Integer.parseInt(argv[i++]);
    counts = Integer.parseInt(argv[i++]);
    try {
      launch();
    } finally {
      FileUtil.fullyDelete(TEST_DIR);
    }
  }
}
