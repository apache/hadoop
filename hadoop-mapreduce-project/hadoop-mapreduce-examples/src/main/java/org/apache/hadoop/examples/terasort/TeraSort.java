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

package org.apache.hadoop.examples.terasort;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Generates the sampled split points, launches the job, and waits for it to
 * finish. 
 * <p>
 * To run the program: 
 * <b>bin/hadoop jar hadoop-*-examples.jar terasort in-dir out-dir</b>
 */
public class TeraSort extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(TeraSort.class);

  /**
   * A partitioner that splits text keys into roughly equal partitions
   * in a global sorted order.
   */
  static class TotalOrderPartitioner extends Partitioner<Text,Text>
      implements Configurable {
    private TrieNode trie;
    private Text[] splitPoints;
    private Configuration conf;

    /**
     * A generic trie node
     */
    static abstract class TrieNode {
      private int level;
      TrieNode(int level) {
        this.level = level;
      }
      abstract int findPartition(Text key);
      abstract void print(PrintStream strm) throws IOException;
      int getLevel() {
        return level;
      }
    }

    /**
     * An inner trie node that contains 256 children based on the next
     * character.
     */
    static class InnerTrieNode extends TrieNode {
      private TrieNode[] child = new TrieNode[256];
      
      InnerTrieNode(int level) {
        super(level);
      }
      int findPartition(Text key) {
        int level = getLevel();
        if (key.getLength() <= level) {
          return child[0].findPartition(key);
        }
        return child[key.getBytes()[level] & 0xff].findPartition(key);
      }
      void setChild(int idx, TrieNode child) {
        this.child[idx] = child;
      }
      void print(PrintStream strm) throws IOException {
        for(int ch=0; ch < 256; ++ch) {
          for(int i = 0; i < 2*getLevel(); ++i) {
            strm.print(' ');
          }
          strm.print(ch);
          strm.println(" ->");
          if (child[ch] != null) {
            child[ch].print(strm);
          }
        }
      }
    }

    /**
     * A leaf trie node that does string compares to figure out where the given
     * key belongs between lower..upper.
     */
    static class LeafTrieNode extends TrieNode {
      int lower;
      int upper;
      Text[] splitPoints;
      LeafTrieNode(int level, Text[] splitPoints, int lower, int upper) {
        super(level);
        this.splitPoints = splitPoints;
        this.lower = lower;
        this.upper = upper;
      }
      int findPartition(Text key) {
        for(int i=lower; i<upper; ++i) {
          if (splitPoints[i].compareTo(key) > 0) {
            return i;
          }
        }
        return upper;
      }
      void print(PrintStream strm) throws IOException {
        for(int i = 0; i < 2*getLevel(); ++i) {
          strm.print(' ');
        }
        strm.print(lower);
        strm.print(", ");
        strm.println(upper);
      }
    }


    /**
     * Read the cut points from the given sequence file.
     * @param fs the file system
     * @param p the path to read
     * @param conf the job config
     * @return the strings to split the partitions on
     * @throws IOException
     */
    private static Text[] readPartitions(FileSystem fs, Path p,
        Configuration conf) throws IOException {
      int reduces = conf.getInt(MRJobConfig.NUM_REDUCES, 1);
      Text[] result = new Text[reduces - 1];
      DataInputStream reader = fs.open(p);
      for(int i=0; i < reduces - 1; ++i) {
        result[i] = new Text();
        result[i].readFields(reader);
      }
      reader.close();
      return result;
    }

    /**
     * Given a sorted set of cut points, build a trie that will find the correct
     * partition quickly.
     * @param splits the list of cut points
     * @param lower the lower bound of partitions 0..numPartitions-1
     * @param upper the upper bound of partitions 0..numPartitions-1
     * @param prefix the prefix that we have already checked against
     * @param maxDepth the maximum depth we will build a trie for
     * @return the trie node that will divide the splits correctly
     */
    private static TrieNode buildTrie(Text[] splits, int lower, int upper, 
                                      Text prefix, int maxDepth) {
      int depth = prefix.getLength();
      if (depth >= maxDepth || lower == upper) {
        return new LeafTrieNode(depth, splits, lower, upper);
      }
      InnerTrieNode result = new InnerTrieNode(depth);
      Text trial = new Text(prefix);
      // append an extra byte on to the prefix
      trial.append(new byte[1], 0, 1);
      int currentBound = lower;
      for(int ch = 0; ch < 255; ++ch) {
        trial.getBytes()[depth] = (byte) (ch + 1);
        lower = currentBound;
        while (currentBound < upper) {
          if (splits[currentBound].compareTo(trial) >= 0) {
            break;
          }
          currentBound += 1;
        }
        trial.getBytes()[depth] = (byte) ch;
        result.child[ch] = buildTrie(splits, lower, currentBound, trial, 
                                     maxDepth);
      }
      // pick up the rest
      trial.getBytes()[depth] = (byte) 255;
      result.child[255] = buildTrie(splits, currentBound, upper, trial,
                                    maxDepth);
      return result;
    }

    public void setConf(Configuration conf) {
      try {
        FileSystem fs = FileSystem.getLocal(conf);
        this.conf = conf;
        Path partFile = new Path(TeraInputFormat.PARTITION_FILENAME);
        splitPoints = readPartitions(fs, partFile, conf);
        trie = buildTrie(splitPoints, 0, splitPoints.length, new Text(), 2);
      } catch (IOException ie) {
        throw new IllegalArgumentException("can't read partitions file", ie);
      }
    }

    public Configuration getConf() {
      return conf;
    }
    
    public TotalOrderPartitioner() {
    }

    public int getPartition(Text key, Text value, int numPartitions) {
      return trie.findPartition(key);
    }
    
  }
  
  /**
   * A total order partitioner that assigns keys based on their first 
   * PREFIX_LENGTH bytes, assuming a flat distribution.
   */
  public static class SimplePartitioner extends Partitioner<Text, Text>
      implements Configurable {
    int prefixesPerReduce;
    private static final int PREFIX_LENGTH = 3;
    private Configuration conf = null;
    public void setConf(Configuration conf) {
      this.conf = conf;
      prefixesPerReduce = (int) Math.ceil((1 << (8 * PREFIX_LENGTH)) / 
        (float) conf.getInt(MRJobConfig.NUM_REDUCES, 1));
    }
    
    public Configuration getConf() {
      return conf;
    }
    
    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
      byte[] bytes = key.getBytes();
      int len = Math.min(PREFIX_LENGTH, key.getLength());
      int prefix = 0;
      for(int i=0; i < len; ++i) {
        prefix = (prefix << 8) | (0xff & bytes[i]);
      }
      return prefix / prefixesPerReduce;
    }
  }

  public static boolean getUseSimplePartitioner(JobContext job) {
    return job.getConfiguration().getBoolean(
        TeraSortConfigKeys.USE_SIMPLE_PARTITIONER.key(),
        TeraSortConfigKeys.DEFAULT_USE_SIMPLE_PARTITIONER);
  }

  public static void setUseSimplePartitioner(Job job, boolean value) {
    job.getConfiguration().setBoolean(
        TeraSortConfigKeys.USE_SIMPLE_PARTITIONER.key(), value);
  }

  public static int getOutputReplication(JobContext job) {
    return job.getConfiguration().getInt(
        TeraSortConfigKeys.OUTPUT_REPLICATION.key(),
        TeraSortConfigKeys.DEFAULT_OUTPUT_REPLICATION);
  }

  public static void setOutputReplication(Job job, int value) {
    job.getConfiguration().setInt(TeraSortConfigKeys.OUTPUT_REPLICATION.key(),
        value);
  }

  private static void usage() throws IOException {
    System.err.println("Usage: terasort [-Dproperty=value] <in> <out>");
    System.err.println("TeraSort configurations are:");
    for (TeraSortConfigKeys teraSortConfigKeys : TeraSortConfigKeys.values()) {
      System.err.println(teraSortConfigKeys.toString());
    }
  }

  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      usage();
      return 2;
    }
    LOG.info("starting");
    Job job = Job.getInstance(getConf());
    Path inputDir = new Path(args[0]);
    Path outputDir = new Path(args[1]);
    boolean useSimplePartitioner = getUseSimplePartitioner(job);
    TeraInputFormat.setInputPaths(job, inputDir);
    FileOutputFormat.setOutputPath(job, outputDir);
    job.setJobName("TeraSort");
    job.setJarByClass(TeraSort.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(TeraInputFormat.class);
    job.setOutputFormatClass(TeraOutputFormat.class);
    if (useSimplePartitioner) {
      job.setPartitionerClass(SimplePartitioner.class);
    } else {
      long start = System.currentTimeMillis();
      Path partitionFile = new Path(outputDir, 
                                    TeraInputFormat.PARTITION_FILENAME);
      URI partitionUri = new URI(partitionFile.toString() +
                                 "#" + TeraInputFormat.PARTITION_FILENAME);
      try {
        TeraInputFormat.writePartitionFile(job, partitionFile);
      } catch (Throwable e) {
        LOG.error(e.getMessage());
        return -1;
      }
      job.addCacheFile(partitionUri);  
      long end = System.currentTimeMillis();
      System.out.println("Spent " + (end - start) + "ms computing partitions.");
      job.setPartitionerClass(TotalOrderPartitioner.class);
    }
    
    job.getConfiguration().setInt("dfs.replication", getOutputReplication(job));
    int ret = job.waitForCompletion(true) ? 0 : 1;
    LOG.info("done");
    return ret;
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new TeraSort(), args);
    System.exit(res);
  }

}
