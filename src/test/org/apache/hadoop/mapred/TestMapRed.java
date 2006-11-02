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
package org.apache.hadoop.mapred;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.lib.*;
import junit.framework.TestCase;
import java.io.*;
import java.util.*;


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
public class TestMapRed extends TestCase {
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
    static class RandomGenMapper implements Mapper {
        public void configure(JobConf job) {
        }

        public void map(WritableComparable key, Writable val, OutputCollector out, Reporter reporter) throws IOException {
            int randomVal = ((IntWritable) key).get();
            int randomCount = ((IntWritable) val).get();

            for (int i = 0; i < randomCount; i++) {
                out.collect(new IntWritable(Math.abs(r.nextInt())), new IntWritable(randomVal));
            }
        }
        public void close() {
        }
    }
    /**
     */
    static class RandomGenReducer implements Reducer {
        public void configure(JobConf job) {
        }

        public void reduce(WritableComparable key, Iterator it, OutputCollector out, Reporter reporter) throws IOException {
            while (it.hasNext()) {
                int val = ((IntWritable) it.next()).get();
                out.collect(new Text("" + val), new Text(""));
            }
        }
        public void close() {
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
    static class RandomCheckMapper implements Mapper {
        public void configure(JobConf job) {
        }

        public void map(WritableComparable key, Writable val, OutputCollector out, Reporter reporter) throws IOException {
            Text str = (Text) val;

            out.collect(new IntWritable(Integer.parseInt(str.toString().trim())), new IntWritable(1));
        }
        public void close() {
        }
    }
    /**
     */
    static class RandomCheckReducer implements Reducer {
        public void configure(JobConf job) {
        }
        
        public void reduce(WritableComparable key, Iterator it, OutputCollector out, Reporter reporter) throws IOException {
            int keyint = ((IntWritable) key).get();
            int count = 0;
            while (it.hasNext()) {
                it.next();
                count++;
            }
            out.collect(new IntWritable(keyint), new IntWritable(count));
        }
        public void close() {
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
    static class MergeMapper implements Mapper {
        public void configure(JobConf job) {
        }

        public void map(WritableComparable key, Writable val, OutputCollector out, Reporter reporter) throws IOException {
            int keyint = ((IntWritable) key).get();
            int valint = ((IntWritable) val).get();

            out.collect(new IntWritable(keyint), new IntWritable(valint));
        }
        public void close() {
        }
    }
    static class MergeReducer implements Reducer {
        public void configure(JobConf job) {
        }
        
        public void reduce(WritableComparable key, Iterator it, OutputCollector out, Reporter reporter) throws IOException {
            int keyint = ((IntWritable) key).get();
            int total = 0;
            while (it.hasNext()) {
                total += ((IntWritable) it.next()).get();
            }
            out.collect(new IntWritable(keyint), new IntWritable(total));
        }
        public void close() {
        }
    }

    private static int range = 10;
    private static int counts = 100;
    private static Random r = new Random();

    /**
       public TestMapRed(int range, int counts, Configuration conf) throws IOException {
       this.range = range;
       this.counts = counts;
       this.conf = conf;
       }
    **/

    public void testMapred() throws Exception {
      launch();
    }

    private static class MyMap implements Mapper {
      private JobConf conf;
      private boolean compress;
      private String taskId;
      
      public void configure(JobConf conf) {
        this.conf = conf;
        compress = conf.getBoolean("mapred.compress.map.output", false);
        taskId = conf.get("mapred.task.id");
      }
      
      public void map(WritableComparable key, Writable value,
                      OutputCollector output, Reporter reporter
                      ) throws IOException {
        String str = ((Text) value).toString().toLowerCase();
        output.collect(new Text(str), value);
      }

      public void close() throws IOException {
        MapOutputFile namer = new MapOutputFile();
        namer.setConf(conf);
        FileSystem fs = FileSystem.get(conf);
        Path output = namer.getOutputFile(taskId, 0);
        assertTrue("map output exists " + output, fs.exists(output));
        SequenceFile.Reader rdr = 
          new SequenceFile.Reader(fs, output, conf);
        assertEquals("is map output compressed " + output, compress, 
                     rdr.isCompressed());
        rdr.close();
      }
    }
    
    private static class MyReduce extends IdentityReducer {
      private JobConf conf;
      private boolean compressInput;
      private String taskId;
      private boolean first = true;
      
      public void configure(JobConf conf) {
        this.conf = conf;
        compressInput = conf.getBoolean("mapred.compress.map.output", 
                                        false);
        taskId = conf.get("mapred.task.id");
      }
      
      public void reduce(WritableComparable key, Iterator values,
                         OutputCollector output, Reporter reporter
                        ) throws IOException {
        if (first) {
          first = false;
          Path input = conf.getLocalPath(taskId+"/all.2");
          FileSystem fs = FileSystem.get(conf);
          assertTrue("reduce input exists " + input, fs.exists(input));
          SequenceFile.Reader rdr = 
            new SequenceFile.Reader(fs, input, conf);
          assertEquals("is reduce input compressed " + input, 
                       compressInput, 
                       rdr.isCompressed());
          rdr.close();          
        }
      }
      
    }
    
    private void checkCompression(boolean compressMapOutput,
                                  boolean compressReduceOutput,
                                  boolean includeCombine
                                  ) throws Exception {
      JobConf conf = new JobConf();
      Path testdir = new Path("build/test/test.mapred.compress");
      Path inDir = new Path(testdir, "in");
      Path outDir = new Path(testdir, "out");
      FileSystem fs = FileSystem.get(conf);
      fs.delete(testdir);
      conf.setInputPath(inDir);
      conf.setOutputPath(outDir);
      conf.setMapperClass(MyMap.class);
      conf.setReducerClass(MyReduce.class);
      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(Text.class);
      conf.setOutputFormat(SequenceFileOutputFormat.class);
      if (includeCombine) {
        conf.setCombinerClass(IdentityReducer.class);
      }
      if (compressMapOutput) {
        conf.setCompressMapOutput(true);
      }
      if (compressReduceOutput) {
        SequenceFileOutputFormat.setCompressOutput(conf, true);
      }
      try {
        if (!fs.mkdirs(testdir)) {
          throw new IOException("Mkdirs failed to create " + testdir.toString());
        }
        if (!fs.mkdirs(inDir)) {
          throw new IOException("Mkdirs failed to create " + inDir.toString());
        }
        Path inFile = new Path(inDir, "part0");
        DataOutputStream f = fs.create(inFile);
        f.writeBytes("Owen was here\n");
        f.writeBytes("Hadoop is fun\n");
        f.writeBytes("Is this done, yet?\n");
        f.close();
        JobClient.runJob(conf);
        Path output = new Path(outDir,
                               ReduceTask.getOutputName(0));
        assertTrue("reduce output exists " + output, fs.exists(output));
        SequenceFile.Reader rdr = 
            new SequenceFile.Reader(fs, output, conf);
        assertEquals("is reduce output compressed " + output, 
                     compressReduceOutput, 
                     rdr.isCompressed());
        rdr.close();
      } finally {
        fs.delete(testdir);
      }
    }
    
    public void testCompression() throws Exception {
      for(int compressMap=0; compressMap < 2; ++compressMap) {
        for(int compressOut=0; compressOut < 2; ++compressOut) {
          for(int combine=0; combine < 2; ++combine) {
            checkCompression(compressMap == 1, compressOut == 1,
                             combine == 1);
          }
        }
      }
    }
    
    
    /**
     * 
     */
    public static void launch() throws Exception {
        //
        // Generate distribution of ints.  This is the answer key.
        //
        JobConf conf = new JobConf();
        int countsToGo = counts;
        int dist[] = new int[range];
        for (int i = 0; i < range; i++) {
            double avgInts = (1.0 * countsToGo) / (range - i);
            dist[i] = (int) Math.max(0, Math.round(avgInts + (Math.sqrt(avgInts) * r.nextGaussian())));
            countsToGo -= dist[i];
        }
        if (countsToGo > 0) {
            dist[dist.length-1] += countsToGo;
        }

        //
        // Write the answer key to a file.  
        //
        FileSystem fs = FileSystem.get(conf);
        Path testdir = new Path("mapred.loadtest");
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
        fs.delete(randomOuts);


        JobConf genJob = new JobConf(conf);
        genJob.setInputPath(randomIns);
        genJob.setInputFormat(SequenceFileInputFormat.class);
        genJob.setMapperClass(RandomGenMapper.class);

        genJob.setOutputPath(randomOuts);
        genJob.setOutputKeyClass(IntWritable.class);
        genJob.setOutputValueClass(IntWritable.class);
        genJob.setOutputFormat(TextOutputFormat.class);
        genJob.setReducerClass(RandomGenReducer.class);
        genJob.setNumReduceTasks(1);

        JobClient.runJob(genJob);

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
        fs.delete(intermediateOuts);
        JobConf checkJob = new JobConf(conf);
        checkJob.setInputPath(randomOuts);
        checkJob.setInputFormat(TextInputFormat.class);
        checkJob.setMapperClass(RandomCheckMapper.class);

        checkJob.setOutputPath(intermediateOuts);
        checkJob.setOutputKeyClass(IntWritable.class);
        checkJob.setOutputValueClass(IntWritable.class);
        checkJob.setOutputFormat(SequenceFileOutputFormat.class);
        checkJob.setReducerClass(RandomCheckReducer.class);
        checkJob.setNumReduceTasks(intermediateReduces);

        JobClient.runJob(checkJob);

        //
        // OK, now we take the output from the last job and
        // merge it down to a single file.  The map() and reduce()
        // functions don't really do anything except reemit tuples.
        // But by having a single reduce task here, we end up merging
        // all the files.
        //
        Path finalOuts = new Path(testdir, "finalouts");        
        fs.delete(finalOuts);
        JobConf mergeJob = new JobConf(conf);
        mergeJob.setInputPath(intermediateOuts);
        mergeJob.setInputFormat(SequenceFileInputFormat.class);
        mergeJob.setMapperClass(MergeMapper.class);
        
        mergeJob.setOutputPath(finalOuts);
        mergeJob.setOutputKeyClass(IntWritable.class);
        mergeJob.setOutputValueClass(IntWritable.class);
        mergeJob.setOutputFormat(SequenceFileOutputFormat.class);
        mergeJob.setReducerClass(MergeReducer.class);
        mergeJob.setNumReduceTasks(1);
        
        JobClient.runJob(mergeJob);
        
 
        //
        // Finally, we compare the reconstructed answer key with the
        // original one.  Remember, we need to ignore zero-count items
        // in the original key.
        //
        boolean success = true;
        Path recomputedkey = new Path(finalOuts, "part-00000");
        SequenceFile.Reader in = new SequenceFile.Reader(fs, recomputedkey, conf);
        int totalseen = 0;
        try {
            IntWritable key = new IntWritable();
            IntWritable val = new IntWritable();            
            for (int i = 0; i < range; i++) {
                if (dist[i] == 0) {
                    continue;
                }
                if (! in.next(key, val)) {
                    System.err.println("Cannot read entry " + i);
                    success = false;
                    break;
                } else {
                    if ( !((key.get() == i ) && (val.get() == dist[i]))) {
                        System.err.println("Mismatch!  Pos=" + key.get() + ", i=" + i + ", val=" + val.get() + ", dist[i]=" + dist[i]);
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
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(resultFile)));
        try {
            bw.write("Success=" + success + "\n");
            System.out.println("Success=" + success);            
        } finally {
            bw.close();
        }
	fs.delete(testdir);
    }

    /**
     * Launches all the tasks in order.
     */
    public static void main(String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println("Usage: TestMapRed <range> <counts>");
            System.err.println();
            System.err.println("Note: a good test will have a <counts> value that is substantially larger than the <range>");
            return;
        }

        int i = 0;
        range = Integer.parseInt(argv[i++]);
        counts = Integer.parseInt(argv[i++]);
	      launch();
    }
}
