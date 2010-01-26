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
import java.util.Formatter;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.tools.rumen.JobStory;
import org.apache.hadoop.tools.rumen.TaskInfo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Synthetic job generated from a trace description.
 */
class GridmixJob implements Callable<Job>, Delayed {

  public static final String JOBNAME = "GRIDMIX";
  public static final String ORIGNAME = "gridmix.job.name.original";
  public static final Log LOG = LogFactory.getLog(GridmixJob.class);

  private static final ThreadLocal<Formatter> nameFormat =
    new ThreadLocal<Formatter>() {
      @Override
      protected Formatter initialValue() {
        final StringBuilder sb = new StringBuilder(JOBNAME.length() + 5);
        sb.append(JOBNAME);
        return new Formatter(sb);
      }
    };

  private final int seq;
  private final Path outdir;
  protected final Job job;
  private final JobStory jobdesc;
  private final long submissionTimeNanos;

  public GridmixJob(Configuration conf, long submissionMillis,
      JobStory jobdesc, Path outRoot, int seq) throws IOException {
    ((StringBuilder)nameFormat.get().out()).setLength(JOBNAME.length());
    job = new Job(conf, nameFormat.get().format("%05d", seq).toString());
    submissionTimeNanos = TimeUnit.NANOSECONDS.convert(
        submissionMillis, TimeUnit.MILLISECONDS);
    this.jobdesc = jobdesc;
    this.seq = seq;
    outdir = new Path(outRoot, "" + seq);
  }

  protected GridmixJob(Configuration conf, long submissionMillis, String name)
      throws IOException {
    job = new Job(conf, name);
    submissionTimeNanos = TimeUnit.NANOSECONDS.convert(
        submissionMillis, TimeUnit.MILLISECONDS);
    jobdesc = null;
    outdir = null;
    seq = -1;
  }

  public String toString() {
    return job.getJobName();
  }

  public long getDelay(TimeUnit unit) {
    return unit.convert(submissionTimeNanos - System.nanoTime(),
        TimeUnit.NANOSECONDS);
  }

  @Override
  public int compareTo(Delayed other) {
    if (this == other) {
      return 0;
    }
    if (other instanceof GridmixJob) {
      final long otherNanos = ((GridmixJob)other).submissionTimeNanos;
      if (otherNanos < submissionTimeNanos) {
        return 1;
      }
      if (otherNanos > submissionTimeNanos) {
        return -1;
      }
      return id() - ((GridmixJob)other).id();
    }
    final long diff =
      getDelay(TimeUnit.NANOSECONDS) - other.getDelay(TimeUnit.NANOSECONDS);
    return 0 == diff ? 0 : (diff > 0 ? 1 : -1);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    // not possible unless job is cloned; all jobs should be unique
    return other instanceof GridmixJob && id() == ((GridmixJob)other).id();
  }

  @Override
  public int hashCode() {
    return id();
  }

  int id() {
    return seq;
  }

  Job getJob() {
    return job;
  }

  JobStory getJobDesc() {
    return jobdesc;
  }

  public Job call() throws IOException, InterruptedException,
                           ClassNotFoundException {
    job.setMapperClass(GridmixMapper.class);
    job.setReducerClass(GridmixReducer.class);
    job.setNumReduceTasks(jobdesc.getNumberReduces());
    job.setMapOutputKeyClass(GridmixKey.class);
    job.setMapOutputValueClass(GridmixRecord.class);
    job.setSortComparatorClass(GridmixKey.Comparator.class);
    job.setGroupingComparatorClass(SpecGroupingComparator.class);
    job.setInputFormatClass(GridmixInputFormat.class);
    job.setOutputFormatClass(RawBytesOutputFormat.class);
    job.setPartitionerClass(DraftPartitioner.class);
    job.setJarByClass(GridmixJob.class);
    job.getConfiguration().setInt("gridmix.job.seq", seq);
    job.getConfiguration().set(ORIGNAME, null == jobdesc.getJobID()
        ? "<unknown>" : jobdesc.getJobID().toString());
    job.getConfiguration().setBoolean(Job.USED_GENERIC_PARSER, true);
    FileInputFormat.addInputPath(job, new Path("ignored"));
    FileOutputFormat.setOutputPath(job, outdir);
    job.submit();
    return job;
  }

  public static class DraftPartitioner<V> extends Partitioner<GridmixKey,V> {
    public int getPartition(GridmixKey key, V value, int numReduceTasks) {
      return key.getPartition();
    }
  }

  public static class SpecGroupingComparator
      implements RawComparator<GridmixKey> {
    private final DataInputBuffer di = new DataInputBuffer();
    private final byte[] reset = di.getData();
    @Override
    public int compare(GridmixKey g1, GridmixKey g2) {
      final byte t1 = g1.getType();
      final byte t2 = g2.getType();
      if (t1 == GridmixKey.REDUCE_SPEC ||
          t2 == GridmixKey.REDUCE_SPEC) {
        return t1 - t2;
      }
      assert t1 == GridmixKey.DATA;
      assert t2 == GridmixKey.DATA;
      return g1.compareTo(g2);
    }
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      try {
        final int ret;
        di.reset(b1, s1, l1);
        final int x1 = WritableUtils.readVInt(di);
        di.reset(b2, s2, l2);
        final int x2 = WritableUtils.readVInt(di);
        final int t1 = b1[s1 + x1];
        final int t2 = b2[s2 + x2];
        if (t1 == GridmixKey.REDUCE_SPEC ||
            t2 == GridmixKey.REDUCE_SPEC) {
          ret = t1 - t2;
        } else {
          assert t1 == GridmixKey.DATA;
          assert t2 == GridmixKey.DATA;
          ret =
            WritableComparator.compareBytes(b1, s1, x1, b2, s2, x2);
        }
        di.reset(reset, 0, 0);
        return ret;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class GridmixMapper
      extends Mapper<NullWritable,GridmixRecord,GridmixKey,GridmixRecord> {

    private double acc;
    private double ratio;
    private final ArrayList<RecordFactory> reduces =
      new ArrayList<RecordFactory>();
    private final Random r = new Random();

    private final GridmixKey key = new GridmixKey();
    private final GridmixRecord val = new GridmixRecord();

    @Override
    protected void setup(Context ctxt)
        throws IOException, InterruptedException {
      final Configuration conf = ctxt.getConfiguration();
      final GridmixSplit split = (GridmixSplit) ctxt.getInputSplit();
      final int maps = split.getMapCount();
      final long[] reduceBytes = split.getOutputBytes();
      final long[] reduceRecords = split.getOutputRecords();

      long totalRecords = 0L;
      final int nReduces = ctxt.getNumReduceTasks();
      if (nReduces > 0) {
        int idx = 0;
        int id = split.getId();
        for (int i = 0; i < nReduces; ++i) {
          final GridmixKey.Spec spec = new GridmixKey.Spec();
          if (i == id) {
            spec.bytes_out = split.getReduceBytes(idx);
            spec.rec_out = split.getReduceRecords(idx);
            ++idx;
            id += maps;
          }
          reduces.add(new IntermediateRecordFactory(
              new AvgRecordFactory(reduceBytes[i], reduceRecords[i], conf),
              i, reduceRecords[i], spec, conf));
          totalRecords += reduceRecords[i];
        }
      } else {
        reduces.add(new AvgRecordFactory(reduceBytes[0], reduceRecords[0],
              conf));
        totalRecords = reduceRecords[0];
      }
      final long splitRecords = split.getInputRecords();
      final long inputRecords = splitRecords <= 0 && split.getLength() >= 0
        ? Math.max(1,
          split.getLength() / conf.getInt("gridmix.missing.rec.size", 64*1024))
        : splitRecords;
      ratio = totalRecords / (1.0 * inputRecords);
      acc = 0.0;
    }

    @Override
    public void map(NullWritable ignored, GridmixRecord rec,
        Context context) throws IOException, InterruptedException {
      acc += ratio;
      while (acc >= 1.0 && !reduces.isEmpty()) {
        key.setSeed(r.nextLong());
        val.setSeed(r.nextLong());
        final int idx = r.nextInt(reduces.size());
        final RecordFactory f = reduces.get(idx);
        if (!f.next(key, val)) {
          reduces.remove(idx);
          continue;
        }
        context.write(key, val);
        acc -= 1.0;
      }
    }

    @Override
    public void cleanup(Context context)
        throws IOException, InterruptedException {
      for (RecordFactory factory : reduces) {
        key.setSeed(r.nextLong());
        while (factory.next(key, val)) {
          context.write(key, val);
          key.setSeed(r.nextLong());
        }
      }
    }
  }

  public static class GridmixReducer
      extends Reducer<GridmixKey,GridmixRecord,NullWritable,GridmixRecord> {

    private final Random r = new Random();
    private final GridmixRecord val = new GridmixRecord();

    private double acc;
    private double ratio;
    private RecordFactory factory;

    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {
      if (!context.nextKey() ||
           context.getCurrentKey().getType() != GridmixKey.REDUCE_SPEC) {
        throw new IOException("Missing reduce spec");
      }
      long outBytes = 0L;
      long outRecords = 0L;
      long inRecords = 0L;
      for (GridmixRecord ignored : context.getValues()) {
        final GridmixKey spec = context.getCurrentKey();
        inRecords += spec.getReduceInputRecords();
        outBytes += spec.getReduceOutputBytes();
        outRecords += spec.getReduceOutputRecords();
      }
      if (0 == outRecords && inRecords > 0) {
        LOG.info("Spec output bytes w/o records. Using input record count");
        outRecords = inRecords;
      }
      factory =
        new AvgRecordFactory(outBytes, outRecords, context.getConfiguration());
      ratio = outRecords / (1.0 * inRecords);
      acc = 0.0;
    }
    @Override
    protected void reduce(GridmixKey key, Iterable<GridmixRecord> values,
        Context context) throws IOException, InterruptedException {
      for (GridmixRecord ignored : values) {
        acc += ratio;
        while (acc >= 1.0 && factory.next(null, val)) {
          context.write(NullWritable.get(), val);
          acc -= 1.0;
        }
      }
    }
    @Override
    protected void cleanup(Context context)
        throws IOException, InterruptedException {
      val.setSeed(r.nextLong());
      while (factory.next(null, val)) {
        context.write(NullWritable.get(), val);
        val.setSeed(r.nextLong());
      }
    }
  }

  static class GridmixRecordReader
      extends RecordReader<NullWritable,GridmixRecord> {

    private RecordFactory factory;
    private final Random r = new Random();
    private final GridmixRecord val = new GridmixRecord();

    public GridmixRecordReader() { }

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext ctxt)
            throws IOException, InterruptedException {
      final GridmixSplit split = (GridmixSplit)genericSplit;
      final Configuration conf = ctxt.getConfiguration();
      factory = new ReadRecordFactory(split.getLength(),
          split.getInputRecords(), new FileQueue(split, conf), conf);
    }

    @Override
    public boolean nextKeyValue() throws IOException {
      val.setSeed(r.nextLong());
      return factory.next(null, val);
    }
    @Override
    public float getProgress() throws IOException {
      return factory.getProgress();
    }
    @Override
    public NullWritable getCurrentKey() {
      return NullWritable.get();
    }
    @Override
    public GridmixRecord getCurrentValue() {
      return val;
    }
    @Override
    public void close() throws IOException {
      factory.close();
    }
  }

  static class GridmixInputFormat
      extends InputFormat<NullWritable,GridmixRecord> {

    @Override
    public List<InputSplit> getSplits(JobContext jobCtxt) throws IOException {
      return pullDescription(jobCtxt.getConfiguration().getInt(
            "gridmix.job.seq", -1));
    }
    @Override
    public RecordReader<NullWritable,GridmixRecord> createRecordReader(
        InputSplit split, final TaskAttemptContext taskContext)
        throws IOException {
      return new GridmixRecordReader();
    }
  }

  static class RawBytesOutputFormat<K>
      extends FileOutputFormat<K,GridmixRecord> {

    @Override
    public RecordWriter<K,GridmixRecord> getRecordWriter(
        TaskAttemptContext job) throws IOException {

      Path file = getDefaultWorkFile(job, "");
      FileSystem fs = file.getFileSystem(job.getConfiguration());
      final FSDataOutputStream fileOut = fs.create(file, false);
      return new RecordWriter<K,GridmixRecord>() {
        @Override
        public void write(K ignored, GridmixRecord value)
            throws IOException {
          value.writeRandom(fileOut, value.getSize());
        }
        @Override
        public void close(TaskAttemptContext ctxt) throws IOException {
          fileOut.close();
        }
      };
    }
  }

  // TODO replace with ThreadLocal submitter?
  private static final ConcurrentHashMap<Integer,List<InputSplit>> descCache =
    new ConcurrentHashMap<Integer,List<InputSplit>>();

  static void pushDescription(int seq, List<InputSplit> splits) {
    if (null != descCache.putIfAbsent(seq, splits)) {
      throw new IllegalArgumentException("Description exists for id " + seq);
    }
  }

  static List<InputSplit> pullDescription(int seq) {
    return descCache.remove(seq);
  }

  // not nesc when TL
  static void clearAll() {
    descCache.clear();
  }

  void buildSplits(FilePool inputDir) throws IOException {
    long mapInputBytesTotal = 0L;
    long mapOutputBytesTotal = 0L;
    long mapOutputRecordsTotal = 0L;
    final JobStory jobdesc = getJobDesc();
    if (null == jobdesc) {
      return;
    }
    final int maps = jobdesc.getNumberMaps();
    final int reds = jobdesc.getNumberReduces();
    for (int i = 0; i < maps; ++i) {
      final TaskInfo info = jobdesc.getTaskInfo(TaskType.MAP, i);
      mapInputBytesTotal += info.getInputBytes();
      mapOutputBytesTotal += info.getOutputBytes();
      mapOutputRecordsTotal += info.getOutputRecords();
    }
    final double[] reduceRecordRatio = new double[reds];
    final double[] reduceByteRatio = new double[reds];
    for (int i = 0; i < reds; ++i) {
      final TaskInfo info = jobdesc.getTaskInfo(TaskType.REDUCE, i);
      reduceByteRatio[i] = info.getInputBytes() / (1.0 * mapOutputBytesTotal);
      reduceRecordRatio[i] =
        info.getInputRecords() / (1.0 * mapOutputRecordsTotal);
    }
    final InputStriper striper = new InputStriper(inputDir, mapInputBytesTotal);
    final List<InputSplit> splits = new ArrayList<InputSplit>();
    for (int i = 0; i < maps; ++i) {
      final int nSpec = reds / maps + ((reds % maps) > i ? 1 : 0);
      final long[] specBytes = new long[nSpec];
      final long[] specRecords = new long[nSpec];
      for (int j = 0; j < nSpec; ++j) {
        final TaskInfo info =
          jobdesc.getTaskInfo(TaskType.REDUCE, i + j * maps);
        specBytes[j] = info.getOutputBytes();
        specRecords[j] = info.getOutputRecords();
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("SPEC(%d) %d -> %d %d %d", id(), i,
              i + j * maps, info.getOutputRecords(), info.getOutputBytes()));
        }
      }
      final TaskInfo info = jobdesc.getTaskInfo(TaskType.MAP, i);
      splits.add(new GridmixSplit(striper.splitFor(inputDir,
              info.getInputBytes(), 3), maps, i,
            info.getInputBytes(), info.getInputRecords(),
            info.getOutputBytes(), info.getOutputRecords(),
            reduceByteRatio, reduceRecordRatio, specBytes, specRecords));
    }
    pushDescription(id(), splits);
  }

}
