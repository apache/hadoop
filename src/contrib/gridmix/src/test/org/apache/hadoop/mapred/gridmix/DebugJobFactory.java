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
package org.apache.hadoop.mapred.gridmix;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.tools.rumen.JobStory;
import org.apache.hadoop.tools.rumen.JobStoryProducer;
import org.apache.hadoop.tools.rumen.TaskAttemptInfo;
import org.apache.hadoop.tools.rumen.TaskInfo;
import org.apache.hadoop.tools.rumen.Pre21JobHistoryConstants.Values;;


/**
 * Component generating random job traces for testing on a single node.
 */
class DebugJobFactory extends JobFactory {

  public DebugJobFactory(JobSubmitter submitter, Path scratch, int numJobs,
      Configuration conf, CountDownLatch startFlag) throws IOException {
    super(submitter, new DebugJobProducer(numJobs, conf), scratch, conf,
        startFlag);
  }

  ArrayList<JobStory> getSubmitted() {
    return ((DebugJobProducer)jobProducer).submitted;
  }

  private static class DebugJobProducer implements JobStoryProducer {
    final ArrayList<JobStory> submitted;
    private final Configuration conf;
    private final AtomicInteger numJobs;

    public DebugJobProducer(int numJobs, Configuration conf) {
      super();
      this.conf = conf;
      this.numJobs = new AtomicInteger(numJobs);
      this.submitted = new ArrayList<JobStory>();
    }

    @Override
    public JobStory getNextJob() throws IOException {
      if (numJobs.getAndDecrement() > 0) {
        final MockJob ret = new MockJob(conf);
        submitted.add(ret);
        return ret;
      }
      return null;
    }

    @Override
    public void close() { }
  }

  /**
   * Generate random task data for a synthetic job.
   */
  static class MockJob implements JobStory {

    public static final String MIN_BYTES_IN =   "gridmix.test.min.bytes.in";
    public static final String VAR_BYTES_IN =   "gridmix.test.var.bytes.in";
    public static final String MIN_BYTES_OUT =  "gridmix.test.min.bytes.out";
    public static final String VAR_BYTES_OUT =  "gridmix.test.var.bytes.out";

    public static final String MIN_REC_SIZE =   "gridmix.test.min.rec.bytes";
    public static final String VAR_REC_SIZE =   "gridmix.test.var.rec.bytes";

    public static final String MAX_MAPS =       "gridmix.test.max.maps";
    public static final String MAX_REDS =       "gridmix.test.max.reduces";

    private static final AtomicInteger seq = new AtomicInteger(0);
    // set timestamps in the past
    private static final AtomicLong timestamp =
      new AtomicLong(System.currentTimeMillis() -
        TimeUnit.MILLISECONDS.convert(60, TimeUnit.DAYS));

    private final String name;
    private final int[] m_recsIn, m_recsOut, r_recsIn, r_recsOut;
    private final long[] m_bytesIn, m_bytesOut, r_bytesIn, r_bytesOut;
    private final long submitTime;

    public MockJob() {
      this(new Configuration(false));
    }

    public MockJob(Configuration conf) {
      this(conf.getInt(MIN_BYTES_IN, 1 << 20),
           conf.getInt(VAR_BYTES_IN, 5 << 20),
           conf.getInt(MIN_BYTES_OUT, 1 << 20),
           conf.getInt(VAR_BYTES_OUT, 5 << 20),
           conf.getInt(MIN_REC_SIZE , 100),
           conf.getInt(VAR_REC_SIZE , 1 << 15),
           conf.getInt(MAX_MAPS, 5),
           conf.getInt(MAX_REDS, 3));
    }

    public MockJob(int min_bytes_in, int var_bytes_in,
                   int min_bytes_out, int var_bytes_out,
                   int min_rec_size, int var_rec_size,
                   int max_maps, int max_reds) {
      final Random r = new Random();
      name = String.format("MOCKJOB%05d", seq.getAndIncrement());
      submitTime = timestamp.addAndGet(TimeUnit.MILLISECONDS.convert(
            r.nextInt(10), TimeUnit.SECONDS));
      int iMapBTotal = 0, oMapBTotal = 0, iRedBTotal = 0, oRedBTotal = 0;
      int iMapRTotal = 0, oMapRTotal = 0, iRedRTotal = 0, oRedRTotal = 0;

      final int iAvgMapRec = r.nextInt(var_rec_size) + min_rec_size;
      final int oAvgMapRec = r.nextInt(var_rec_size) + min_rec_size;

      // MAP
      m_bytesIn = new long[r.nextInt(max_maps) + 1];
      m_bytesOut = new long[m_bytesIn.length];
      m_recsIn = new int[m_bytesIn.length];
      m_recsOut = new int[m_bytesIn.length];
      for (int i = 0; i < m_bytesIn.length; ++i) {
        m_bytesIn[i] = r.nextInt(var_bytes_in) + min_bytes_in;
        iMapBTotal += m_bytesIn[i];
        m_recsIn[i] = (int)(m_bytesIn[i] / iAvgMapRec);
        iMapRTotal += m_recsIn[i];

        m_bytesOut[i] = r.nextInt(var_bytes_out) + min_bytes_out;
        oMapBTotal += m_bytesOut[i];
        m_recsOut[i] = (int)(m_bytesOut[i] / oAvgMapRec);
        oMapRTotal += m_recsOut[i];
      }

      // REDUCE
      r_bytesIn = new long[r.nextInt(max_reds) + 1];
      r_bytesOut = new long[r_bytesIn.length];
      r_recsIn = new int[r_bytesIn.length];
      r_recsOut = new int[r_bytesIn.length];
      iRedBTotal = oMapBTotal;
      iRedRTotal = oMapRTotal;
      for (int j = 0; iRedBTotal > 0; ++j) {
        int i = j % r_bytesIn.length;
        final int inc = r.nextInt(var_bytes_out) + min_bytes_out;
        r_bytesIn[i] += inc;
        iRedBTotal -= inc;
        if (iRedBTotal < 0) {
          r_bytesIn[i] += iRedBTotal;
          iRedBTotal = 0;
        }
        iRedRTotal += r_recsIn[i];
        r_recsIn[i] = (int)(r_bytesIn[i] / oAvgMapRec);
        iRedRTotal -= r_recsIn[i];
        if (iRedRTotal < 0) {
          r_recsIn[i] += iRedRTotal;
          iRedRTotal = 0;
        }

        r_bytesOut[i] = r.nextInt(var_bytes_in) + min_bytes_in;
        oRedBTotal += r_bytesOut[i];
        r_recsOut[i] = (int)(r_bytesOut[i] / iAvgMapRec);
        oRedRTotal += r_recsOut[i];
      }
      r_recsIn[0] += iRedRTotal;

      if (LOG.isDebugEnabled()) {
        iRedRTotal = 0;
        iRedBTotal = 0;
        for (int i = 0; i < r_bytesIn.length; ++i) {
          iRedRTotal += r_recsIn[i];
          iRedBTotal += r_bytesIn[i];
        }
        LOG.debug(String.format("%s: M (%03d) %6d/%10d -> %6d/%10d" +
                             " R (%03d) %6d/%10d -> %6d/%10d @%d", name,
            m_bytesIn.length, iMapRTotal, iMapBTotal, oMapRTotal, oMapBTotal,
            r_bytesIn.length, iRedRTotal, iRedBTotal, oRedRTotal, oRedBTotal,
            submitTime));
      }
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public String getUser() {
      return "FOOBAR";
    }

    @Override
    public JobID getJobID() {
      return null;
    }

    @Override
    public Values getOutcome() {
      return Values.SUCCESS;
    }

    @Override
    public long getSubmissionTime() {
      return submitTime;
    }

    @Override
    public int getNumberMaps() {
      return m_bytesIn.length;
    }

    @Override
    public int getNumberReduces() {
      return r_bytesIn.length;
    }

    @Override
    public TaskInfo getTaskInfo(TaskType taskType, int taskNumber) {
      switch (taskType) {
        case MAP:
          return new TaskInfo(m_bytesIn[taskNumber], m_recsIn[taskNumber],
              m_bytesOut[taskNumber], m_recsOut[taskNumber], -1);
        case REDUCE:
          return new TaskInfo(r_bytesIn[taskNumber], r_recsIn[taskNumber],
              r_bytesOut[taskNumber], r_recsOut[taskNumber], -1);
        default:
          throw new IllegalArgumentException("Not interested");
      }
    }

    @Override
    public InputSplit[] getInputSplits() {
      throw new UnsupportedOperationException();
    }

    @Override
    public TaskAttemptInfo getTaskAttemptInfo(TaskType taskType,
        int taskNumber, int taskAttemptNumber) {
      throw new UnsupportedOperationException();
    }

    @Override
    public TaskAttemptInfo getMapTaskAttemptInfoAdjusted(int taskNumber,
        int taskAttemptNumber, int locality) {
      throw new UnsupportedOperationException();
    }

    @Override
    public org.apache.hadoop.mapred.JobConf getJobConf() {
      throw new UnsupportedOperationException();
    }
  }
}
