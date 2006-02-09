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
package org.apache.hadoop.mapred;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;

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
public class MapredLoadTest {
    static class RandomGenMapper implements Mapper {
        Random r = new Random();
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
    static class RandomGenReducer implements Reducer {
        public void configure(JobConf job) {
        }

        public void reduce(WritableComparable key, Iterator it, OutputCollector out, Reporter reporter) throws IOException {
            int keyint = ((IntWritable) key).get();
            while (it.hasNext()) {
                int val = ((IntWritable) it.next()).get();
                out.collect(new UTF8("" + val), new UTF8(""));
            }
        }
				public void close() {
				}
    }
    static class RandomCheckMapper implements Mapper {
        public void configure(JobConf job) {
        }

        public void map(WritableComparable key, Writable val, OutputCollector out, Reporter reporter) throws IOException {
            long pos = ((LongWritable) key).get();
            UTF8 str = (UTF8) val;

            out.collect(new IntWritable(Integer.parseInt(str.toString().trim())), new IntWritable(1));
        }
				public void close() {
				}
    }
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

    int range;
    int counts;
    Random r = new Random();
    Configuration conf;

    /**
     * MapredLoadTest
     */
    public MapredLoadTest(int range, int counts, Configuration conf) throws IOException {
        this.range = range;
        this.counts = counts;
        this.conf = conf;
    }

    /**
     * 
     */
    public void launch() throws IOException {
        //
        // Generate distribution of ints.  This is the answer key.
        //
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
        File testdir = new File("mapred.loadtest");
        fs.mkdirs(testdir);

        File randomIns = new File(testdir, "genins");
        fs.mkdirs(randomIns);

        File answerkey = new File(randomIns, "answer.key");
        SequenceFile.Writer out = new SequenceFile.Writer(fs, answerkey.getPath(), IntWritable.class, IntWritable.class);
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
        File randomOuts = new File(testdir, "genouts");
        fs.mkdirs(randomOuts);


        JobConf genJob = new JobConf(conf);
        genJob.setInputDir(randomIns);
        genJob.setInputKeyClass(IntWritable.class);
        genJob.setInputValueClass(IntWritable.class);
        genJob.setInputFormat(SequenceFileInputFormat.class);
        genJob.setMapperClass(RandomGenMapper.class);

        genJob.setOutputDir(randomOuts);
        genJob.setOutputKeyClass(IntWritable.class);
        genJob.setOutputValueClass(IntWritable.class);
        genJob.setOutputFormat(TextOutputFormat.class);
        genJob.setReducerClass(RandomGenReducer.class);
        genJob.setNumReduceTasks(1);

        JobClient.runJob(genJob);

        //
        // Next, we read the big file in and regenerate the 
        // original map.
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
        // will not appear in the regenerated key.)
        //
        File finalOuts = new File(testdir, "finalouts");
        fs.mkdirs(finalOuts);
        JobConf checkJob = new JobConf(conf);
        checkJob.setInputDir(randomOuts);
        checkJob.setInputKeyClass(LongWritable.class);
        checkJob.setInputValueClass(UTF8.class);
        checkJob.setInputFormat(TextInputFormat.class);
        checkJob.setMapperClass(RandomCheckMapper.class);

        checkJob.setOutputDir(finalOuts);
        checkJob.setOutputKeyClass(IntWritable.class);
        checkJob.setOutputValueClass(IntWritable.class);
        checkJob.setOutputFormat(SequenceFileOutputFormat.class);
        checkJob.setReducerClass(RandomCheckReducer.class);
        checkJob.setNumReduceTasks(1);

        JobClient.runJob(checkJob);

        //
        // Finally, we compare the reconstructed answer key with the
        // original one.  Remember, we need to ignore zero-count items
        // in the original key.
        //
        boolean success = true;
        File recomputedkey = new File(finalOuts, "part-00000");
        SequenceFile.Reader in = new SequenceFile.Reader(fs, recomputedkey.getPath(), conf);
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
        File resultFile = new File(testdir, "results");
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(resultFile)));
        try {
            bw.write("Success=" + success + "\n");
            System.out.println("Success=" + success);            
        } finally {
            bw.close();
        }
    }

    /**
     * Launches all the tasks in order.
     */
    public static void main(String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println("Usage: MapredLoadTest <range> <counts>");
            System.err.println();
            System.err.println("Note: a good test will have a <counts> value that is substantially larger than the <range>");
            return;
        }

        int i = 0;
        int range = Integer.parseInt(argv[i++]);
        int counts = Integer.parseInt(argv[i++]);

        MapredLoadTest mlt = new MapredLoadTest(range, counts, new Configuration());
        mlt.launch();
    }
}
