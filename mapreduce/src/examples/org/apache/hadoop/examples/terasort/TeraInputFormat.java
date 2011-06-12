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

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;
import org.apache.hadoop.util.StringUtils;

/**
 * An input format that reads the first 10 characters of each line as the key
 * and the rest of the line as the value. Both key and value are represented
 * as Text.
 */
public class TeraInputFormat extends FileInputFormat<Text,Text> {

  static final String PARTITION_FILENAME = "_partition.lst";
  private static final String NUM_PARTITIONS = "terasort.num.partitions";
  private static final String SAMPLE_SIZE = "terasort.partitions.sample";
  static final int KEY_LENGTH = 10;
  static final int VALUE_LENGTH = 90;
  static final int RECORD_LENGTH = KEY_LENGTH + VALUE_LENGTH;
  private static JobConf lastConf = null;
  private static InputSplit[] lastResult = null;

  static class TeraFileSplit extends FileSplit {
    private String[] locations;
    public TeraFileSplit() {}
    public TeraFileSplit(Path file, long start, long length, String[] hosts) {
      super(file, start, length, hosts);
      locations = hosts;
    }
    protected void setLocations(String[] hosts) {
      locations = hosts;
    }
    @Override
    public String[] getLocations() {
      return locations;
    }
    public String toString() {
      StringBuffer result = new StringBuffer();
      result.append(getPath());
      result.append(" from ");
      result.append(getStart());
      result.append(" length ");
      result.append(getLength());
      for(String host: getLocations()) {
        result.append(" ");
        result.append(host);
      }
      return result.toString();
    }
  }

  static class TextSampler implements IndexedSortable {
    private ArrayList<Text> records = new ArrayList<Text>();

    public int compare(int i, int j) {
      Text left = records.get(i);
      Text right = records.get(j);
      return left.compareTo(right);
    }

    public void swap(int i, int j) {
      Text left = records.get(i);
      Text right = records.get(j);
      records.set(j, left);
      records.set(i, right);
    }

    public void addKey(Text key) {
      synchronized (this) {
        records.add(new Text(key));
      }
    }

    /**
     * Find the split points for a given sample. The sample keys are sorted
     * and down sampled to find even split points for the partitions. The
     * returned keys should be the start of their respective partitions.
     * @param numPartitions the desired number of partitions
     * @return an array of size numPartitions - 1 that holds the split points
     */
    Text[] createPartitions(int numPartitions) {
      int numRecords = records.size();
      System.out.println("Making " + numPartitions + " from " + numRecords + 
                         " sampled records");
      if (numPartitions > numRecords) {
        throw new IllegalArgumentException
          ("Requested more partitions than input keys (" + numPartitions +
           " > " + numRecords + ")");
      }
      new QuickSort().sort(this, 0, records.size());
      float stepSize = numRecords / (float) numPartitions;
      Text[] result = new Text[numPartitions-1];
      for(int i=1; i < numPartitions; ++i) {
        result[i-1] = records.get(Math.round(stepSize * i));
      }
      return result;
    }
  }
  
  /**
   * Use the input splits to take samples of the input and generate sample
   * keys. By default reads 100,000 keys from 10 locations in the input, sorts
   * them and picks N-1 keys to generate N equally sized partitions.
   * @param conf the job to sample
   * @param partFile where to write the output file to
   * @throws IOException if something goes wrong
   */
  public static void writePartitionFile(final JobConf conf, 
                                        Path partFile) throws IOException {
    long t1 = System.currentTimeMillis();
    final TeraInputFormat inFormat = new TeraInputFormat();
    final TextSampler sampler = new TextSampler();
    int partitions = conf.getNumReduceTasks();
    long sampleSize = conf.getLong(SAMPLE_SIZE, 100000);
    final InputSplit[] splits = inFormat.getSplits(conf, conf.getNumMapTasks());
    long t2 = System.currentTimeMillis();
    System.out.println("Computing input splits took " + (t2 - t1) + "ms");
    int samples = Math.min(conf.getInt(NUM_PARTITIONS, 10), splits.length);
    System.out.println("Sampling " + samples + " splits of " + splits.length);
    final long recordsPerSample = sampleSize / samples;
    final int sampleStep = splits.length / samples;
    Thread[] samplerReader = new Thread[samples];
    // take N samples from different parts of the input
    for(int i=0; i < samples; ++i) {
      final int idx = i;
      samplerReader[i] = 
        new Thread ("Sampler Reader " + idx) {
        {
          setDaemon(true);
        }
        public void run() {
          Text key = new Text();
          Text value = new Text();
          long records = 0;
          try {
            RecordReader<Text,Text> reader = 
              inFormat.getRecordReader(splits[sampleStep * idx], conf, null);
            while (reader.next(key, value)) {
              sampler.addKey(key);
              records += 1;
              if (recordsPerSample <= records) {
                break;
              }
            }
          } catch (IOException ie){
            System.err.println("Got an exception while reading splits " +
                StringUtils.stringifyException(ie));
            System.exit(-1);
          }
        }
      };
      samplerReader[i].start();
    }
    FileSystem outFs = partFile.getFileSystem(conf);
    DataOutputStream writer = outFs.create(partFile, true, 64*1024, (short) 10, 
                                           outFs.getDefaultBlockSize());
    for (int i = 0; i < samples; i++) {
      try {
        samplerReader[i].join();
      } catch (InterruptedException e) {
      }
    }
    for(Text split : sampler.createPartitions(partitions)) {
      split.write(writer);
    }
    writer.close();
    long t3 = System.currentTimeMillis();
    System.out.println("Computing parititions took " + (t3 - t2) + "ms");
  }

  static class TeraRecordReader implements RecordReader<Text,Text> {
    private FSDataInputStream in;
    private long offset;
    private long length;
    private static final int RECORD_LENGTH = KEY_LENGTH + VALUE_LENGTH;
    private byte[] buffer = new byte[RECORD_LENGTH];

    public TeraRecordReader(Configuration job, 
                            FileSplit split) throws IOException {
      Path p = split.getPath();
      FileSystem fs = p.getFileSystem(job);
      in = fs.open(p);
      long start = split.getStart();
      // find the offset to start at a record boundary
      offset = (RECORD_LENGTH - (start % RECORD_LENGTH)) % RECORD_LENGTH;
      in.seek(start + offset);
      length = split.getLength();
    }

    public void close() throws IOException {
      in.close();
    }

    public Text createKey() {
      return new Text();
    }

    public Text createValue() {
      return new Text();
    }

    public long getPos() throws IOException {
      return in.getPos();
    }

    public float getProgress() throws IOException {
      return (float) offset / length;
    }

    public boolean next(Text key, Text value) throws IOException {
      if (offset >= length) {
        return false;
      }
      int read = 0;
      while (read < RECORD_LENGTH) {
        long newRead = in.read(buffer, read, RECORD_LENGTH - read);
        if (newRead == -1) {
          if (read == 0) {
            return false;
          } else {
            throw new EOFException("read past eof");
          }
        }
        read += newRead;
      }
      key.set(buffer, 0, KEY_LENGTH);
      value.set(buffer, KEY_LENGTH, VALUE_LENGTH);
      offset += RECORD_LENGTH;
      return true;
    }
  }

  @Override
  public RecordReader<Text, Text> 
      getRecordReader(InputSplit split,
                      JobConf job, 
                      Reporter reporter) throws IOException {
    return new TeraRecordReader(job, (FileSplit) split);
  }

  @Override
  protected FileSplit makeSplit(Path file, long start, long length, 
                                String[] hosts) {
    return new TeraFileSplit(file, start, length, hosts);
  }

  @Override
  public InputSplit[] getSplits(JobConf conf, int splits) throws IOException {
    if (conf == lastConf) {
      return lastResult;
    }
    long t1, t2, t3;
    t1 = System.currentTimeMillis();
    lastConf = conf;
    lastResult = super.getSplits(conf, splits);
    t2 = System.currentTimeMillis();
    System.out.println("Spent " + (t2 - t1) + "ms computing base-splits.");
    if (conf.getBoolean("terasort.use.terascheduler", true)) {
      TeraScheduler scheduler = new TeraScheduler((FileSplit[]) lastResult, conf);
      lastResult = scheduler.getNewFileSplits();
      t3 = System.currentTimeMillis(); 
      System.out.println("Spent " + (t3 - t2) + "ms computing TeraScheduler splits.");
    }
    return lastResult;
  }
}
