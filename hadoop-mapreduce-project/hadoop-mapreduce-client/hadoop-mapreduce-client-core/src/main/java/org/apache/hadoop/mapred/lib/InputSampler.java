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

package org.apache.hadoop.mapred.lib;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class InputSampler<K,V> extends 
  org.apache.hadoop.mapreduce.lib.partition.InputSampler<K, V> {

  private static final Log LOG = LogFactory.getLog(InputSampler.class);

  public InputSampler(JobConf conf) {
    super(conf);
  }

  public static <K,V> void writePartitionFile(JobConf job, Sampler<K,V> sampler)
      throws IOException, ClassNotFoundException, InterruptedException {
    writePartitionFile(Job.getInstance(job), sampler);
  }
  /**
   * Interface to sample using an {@link org.apache.hadoop.mapred.InputFormat}.
   */
  public interface Sampler<K,V> extends
    org.apache.hadoop.mapreduce.lib.partition.InputSampler.Sampler<K, V> {
    /**
     * For a given job, collect and return a subset of the keys from the
     * input data.
     */
    K[] getSample(InputFormat<K,V> inf, JobConf job) throws IOException;
  }

  /**
   * Samples the first n records from s splits.
   * Inexpensive way to sample random data.
   */
  public static class SplitSampler<K,V> extends
      org.apache.hadoop.mapreduce.lib.partition.InputSampler.SplitSampler<K, V>
          implements Sampler<K,V> {

    /**
     * Create a SplitSampler sampling <em>all</em> splits.
     * Takes the first numSamples / numSplits records from each split.
     * @param numSamples Total number of samples to obtain from all selected
     *                   splits.
     */
    public SplitSampler(int numSamples) {
      this(numSamples, Integer.MAX_VALUE);
    }

    /**
     * Create a new SplitSampler.
     * @param numSamples Total number of samples to obtain from all selected
     *                   splits.
     * @param maxSplitsSampled The maximum number of splits to examine.
     */
    public SplitSampler(int numSamples, int maxSplitsSampled) {
      super(numSamples, maxSplitsSampled);
    }

    /**
     * From each split sampled, take the first numSamples / numSplits records.
     */
    @SuppressWarnings("unchecked") // ArrayList::toArray doesn't preserve type
    public K[] getSample(InputFormat<K,V> inf, JobConf job) throws IOException {
      InputSplit[] splits = inf.getSplits(job, job.getNumMapTasks());
      ArrayList<K> samples = new ArrayList<K>(numSamples);
      int splitsToSample = Math.min(maxSplitsSampled, splits.length);
      int splitStep = splits.length / splitsToSample;
      int samplesPerSplit = numSamples / splitsToSample;
      long records = 0;
      for (int i = 0; i < splitsToSample; ++i) {
        RecordReader<K,V> reader = inf.getRecordReader(splits[i * splitStep],
            job, Reporter.NULL);
        K key = reader.createKey();
        V value = reader.createValue();
        while (reader.next(key, value)) {
          samples.add(key);
          key = reader.createKey();
          ++records;
          if ((i+1) * samplesPerSplit <= records) {
            break;
          }
        }
        reader.close();
      }
      return (K[])samples.toArray();
    }
  }

  /**
   * Sample from random points in the input.
   * General-purpose sampler. Takes numSamples / maxSplitsSampled inputs from
   * each split.
   */
  public static class RandomSampler<K,V> extends
      org.apache.hadoop.mapreduce.lib.partition.InputSampler.RandomSampler<K, V>
          implements Sampler<K,V> {

    /**
     * Create a new RandomSampler sampling <em>all</em> splits.
     * This will read every split at the client, which is very expensive.
     * @param freq Probability with which a key will be chosen.
     * @param numSamples Total number of samples to obtain from all selected
     *                   splits.
     */
    public RandomSampler(double freq, int numSamples) {
      this(freq, numSamples, Integer.MAX_VALUE);
    }

    /**
     * Create a new RandomSampler.
     * @param freq Probability with which a key will be chosen.
     * @param numSamples Total number of samples to obtain from all selected
     *                   splits.
     * @param maxSplitsSampled The maximum number of splits to examine.
     */
    public RandomSampler(double freq, int numSamples, int maxSplitsSampled) {
      super(freq, numSamples, maxSplitsSampled);
    }

    /**
     * Randomize the split order, then take the specified number of keys from
     * each split sampled, where each key is selected with the specified
     * probability and possibly replaced by a subsequently selected key when
     * the quota of keys from that split is satisfied.
     */
    @SuppressWarnings("unchecked") // ArrayList::toArray doesn't preserve type
    public K[] getSample(InputFormat<K,V> inf, JobConf job) throws IOException {
      InputSplit[] splits = inf.getSplits(job, job.getNumMapTasks());
      ArrayList<K> samples = new ArrayList<K>(numSamples);
      int splitsToSample = Math.min(maxSplitsSampled, splits.length);

      Random r = new Random();
      long seed = r.nextLong();
      r.setSeed(seed);
      LOG.debug("seed: " + seed);
      // shuffle splits
      for (int i = 0; i < splits.length; ++i) {
        InputSplit tmp = splits[i];
        int j = r.nextInt(splits.length);
        splits[i] = splits[j];
        splits[j] = tmp;
      }
      // our target rate is in terms of the maximum number of sample splits,
      // but we accept the possibility of sampling additional splits to hit
      // the target sample keyset
      for (int i = 0; i < splitsToSample ||
                     (i < splits.length && samples.size() < numSamples); ++i) {
        RecordReader<K,V> reader = inf.getRecordReader(splits[i], job,
            Reporter.NULL);
        K key = reader.createKey();
        V value = reader.createValue();
        while (reader.next(key, value)) {
          if (r.nextDouble() <= freq) {
            if (samples.size() < numSamples) {
              samples.add(key);
            } else {
              // When exceeding the maximum number of samples, replace a
              // random element with this one, then adjust the frequency
              // to reflect the possibility of existing elements being
              // pushed out
              int ind = r.nextInt(numSamples);
              if (ind != numSamples) {
                samples.set(ind, key);
              }
              freq *= (numSamples - 1) / (double) numSamples;
            }
            key = reader.createKey();
          }
        }
        reader.close();
      }
      return (K[])samples.toArray();
    }
  }

  /**
   * Sample from s splits at regular intervals.
   * Useful for sorted data.
   */
  public static class IntervalSampler<K,V> extends
      org.apache.hadoop.mapreduce.lib.partition.InputSampler.IntervalSampler<K, V>
          implements Sampler<K,V> {

    /**
     * Create a new IntervalSampler sampling <em>all</em> splits.
     * @param freq The frequency with which records will be emitted.
     */
    public IntervalSampler(double freq) {
      this(freq, Integer.MAX_VALUE);
    }

    /**
     * Create a new IntervalSampler.
     * @param freq The frequency with which records will be emitted.
     * @param maxSplitsSampled The maximum number of splits to examine.
     * @see #getSample
     */
    public IntervalSampler(double freq, int maxSplitsSampled) {
      super(freq, maxSplitsSampled);
    }

    /**
     * For each split sampled, emit when the ratio of the number of records
     * retained to the total record count is less than the specified
     * frequency.
     */
    @SuppressWarnings("unchecked") // ArrayList::toArray doesn't preserve type
    public K[] getSample(InputFormat<K,V> inf, JobConf job) throws IOException {
      InputSplit[] splits = inf.getSplits(job, job.getNumMapTasks());
      ArrayList<K> samples = new ArrayList<K>();
      int splitsToSample = Math.min(maxSplitsSampled, splits.length);
      int splitStep = splits.length / splitsToSample;
      long records = 0;
      long kept = 0;
      for (int i = 0; i < splitsToSample; ++i) {
        RecordReader<K,V> reader = inf.getRecordReader(splits[i * splitStep],
            job, Reporter.NULL);
        K key = reader.createKey();
        V value = reader.createValue();
        while (reader.next(key, value)) {
          ++records;
          if ((double) kept / records < freq) {
            ++kept;
            samples.add(key);
            key = reader.createKey();
          }
        }
        reader.close();
      }
      return (K[])samples.toArray();
    }
  }

}
