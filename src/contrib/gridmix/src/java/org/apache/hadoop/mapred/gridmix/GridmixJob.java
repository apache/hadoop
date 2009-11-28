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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
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
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
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
    job.setMapOutputValueClass(BytesWritable.class);
    job.setSortComparatorClass(BytesWritable.Comparator.class);
    job.setGroupingComparatorClass(SpecGroupingComparator.class);
    job.setInputFormatClass(GridmixInputFormat.class);
    job.setOutputFormatClass(RawBytesOutputFormat.class);
    job.setPartitionerClass(DraftPartitioner.class);
    job.setJarByClass(GridmixJob.class);
    job.getConfiguration().setInt("gridmix.job.seq", seq);
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

  /**
   * Group REDUCE_SPEC records together
   */
  public static class SpecGroupingComparator
      implements RawComparator<GridmixKey>, Serializable {
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
      return WritableComparator.compareBytes(
          g1.getBytes(), 0, g1.getLength(),
          g2.getBytes(), 0, g2.getLength());
    }
    @Override
    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      final byte t1 = b1[s1 + 4];
      final byte t2 = b2[s2 + 4];
      if (t1 == GridmixKey.REDUCE_SPEC ||
          t2 == GridmixKey.REDUCE_SPEC) {
        return t1 - t2;
      }
      assert t1 == GridmixKey.DATA;
      assert t2 == GridmixKey.DATA;
      return WritableComparator.compareBytes(
          b1, s1 + 4, l1 - 4,
          b2, s2 + 4, l2 - 4);
    }
  }

  /**
   * Keytype for synthetic jobs, some embedding instructions for the reduce.
   */
  public static class GridmixKey extends BytesWritable {
    // long fields specifying reduce contract
    private enum RSpec { REC_IN, REC_OUT, BYTES_OUT };
    private static final int SPEC_START = 5; // type + partition len
    private static final int NUMFIELDS = RSpec.values().length;
    private static final int SPEC_SIZE = NUMFIELDS * 8;

    // Key types
    static final byte REDUCE_SPEC = 0;
    static final byte DATA = 1;

    private IntBuffer partition;
    private LongBuffer spec;

    public GridmixKey() {
      super(new byte[SPEC_START]);
    }

    public GridmixKey(byte type, byte[] b) {
      super(b);
      setType(type);
    }

    public byte getType() {
      return getBytes()[0];
    }
    public void setPartition(int partition) {
      this.partition.put(0, partition);
    }
    public int getPartition() {
      return partition.get(0);
    }
    public long getReduceInputRecords() {
      checkState(REDUCE_SPEC);
      return spec.get(RSpec.REC_IN.ordinal());
    }
    public long getReduceOutputBytes() {
      checkState(REDUCE_SPEC);
      return spec.get(RSpec.BYTES_OUT.ordinal());
    }
    public long getReduceOutputRecords() {
      checkState(REDUCE_SPEC);
      return spec.get(RSpec.REC_OUT.ordinal());
    }
    public void setType(byte b) {
      switch (b) {
        case REDUCE_SPEC:
          if (getCapacity() < SPEC_START + SPEC_SIZE) {
            setSize(SPEC_START + SPEC_SIZE);
          }
          spec =
            ByteBuffer.wrap(getBytes(), SPEC_START, SPEC_SIZE).asLongBuffer();
          break;
        case DATA:
          if (getCapacity() < SPEC_START) {
            setSize(SPEC_START);
          }
          spec = null;
          break;
        default:
          throw new IllegalArgumentException("Illegal type " + b);
      }
      getBytes()[0] = b;
      partition =
        ByteBuffer.wrap(getBytes(), 1, SPEC_START - 1).asIntBuffer();
    }
    public void setReduceInputRecords(long records) {
      checkState(REDUCE_SPEC);
      spec.put(RSpec.REC_IN.ordinal(), records);
    }
    public void setReduceOutputBytes(long bytes) {
      checkState(REDUCE_SPEC);
      spec.put(RSpec.BYTES_OUT.ordinal(), bytes);
    }
    public void setReduceOutputRecords(long records) {
      checkState(REDUCE_SPEC);
      spec.put(RSpec.REC_OUT.ordinal(), records);
    }
    private void checkState(byte b) {
      if (getLength() < SPEC_START || getType() != b) {
        throw new IllegalStateException("Expected " + b + ", was " + getType());
      }
    }
    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      if (getLength() < SPEC_START) {
        throw new IOException("Invalid GridmixKey, len " + getLength());
      }
      partition =
        ByteBuffer.wrap(getBytes(), 1, SPEC_START - 1).asIntBuffer();
      spec = getType() == REDUCE_SPEC
        ? ByteBuffer.wrap(getBytes(), SPEC_START, SPEC_SIZE).asLongBuffer()
        : null;
    }
    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      if (getType() == REDUCE_SPEC) {
        LOG.debug("SPEC(" + getPartition() + ") " + getReduceInputRecords() +
            " -> " + getReduceOutputRecords() + "/" + getReduceOutputBytes());
      }
    }
    @Override
    public boolean equals(Object other) {
      if (other instanceof GridmixKey) {
        return super.equals(other);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }
  }

  public static class GridmixMapper
      extends Mapper<IntWritable,BytesWritable,GridmixKey,BytesWritable> {

    private final Random r = new Random();
    private GridmixKey key;
    private final BytesWritable val = new BytesWritable();

    private int keyLen;
    private double acc;
    private double ratio;
    private int[] reduceRecordSize;
    private long[] reduceRecordCount;
    private long[] reduceRecordRemaining;

    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {
      // TODO clearly job-specific, but no data at present
      keyLen = context.getConfiguration().getInt(Gridmix.GRIDMIX_KEY_LEN, 20);
      key = new GridmixKey(GridmixKey.DATA, new byte[keyLen]);
      final GridmixSplit split = (GridmixSplit) context.getInputSplit();
      LOG.info("ID: " + split.getId());
      reduceRecordCount = split.getOutputRecords();
      reduceRecordRemaining =
        Arrays.copyOf(reduceRecordCount, reduceRecordCount.length);
      reduceRecordSize = new int[reduceRecordCount.length];
      int valsize = -1;
      final long[] reduceBytes = split.getOutputBytes();
      long totalRecords = 0L;
      for (int i = 0; i < reduceBytes.length; ++i) {
        reduceRecordSize[i] = Math.max(0,
          Math.round(reduceBytes[i] / (1.0f * reduceRecordCount[i])) - keyLen);
        valsize = Math.max(reduceRecordSize[i], valsize);
        totalRecords += reduceRecordCount[i];
      }
      valsize = Math.max(0, valsize - 4); // BW len encoding
      val.setCapacity(valsize);
      val.setSize(valsize);
      ratio = totalRecords / (1.0 * split.getInputRecords());
      acc = 0.0;
    }

    protected void fillBytes(BytesWritable val, int len) {
      r.nextBytes(val.getBytes());
      val.setSize(len);
    }

    /** Find next non-empty partition after start. */
    private int getNextPart(final int start) {
      int p = start;
      do {
        p = (p + 1) % reduceRecordSize.length;
      } while (0 == reduceRecordRemaining[p] && p != start);
      return 0 == reduceRecordRemaining[p] ? -1 : p;
    }

    @Override
    public void map(IntWritable ignored, BytesWritable bytes,
        Context context) throws IOException, InterruptedException {
      int p = getNextPart(r.nextInt(reduceRecordSize.length));
      if (-1 == p) {
        return;
      }
      acc += ratio;
      while (acc >= 1.0) {
        fillBytes(key, key.getLength());
        key.setType(GridmixKey.DATA);
        key.setPartition(p);
        --reduceRecordRemaining[p];
        fillBytes(val, reduceRecordSize[p]);
        context.write(key, val);
        acc -= 1.0;
        if (0 == reduceRecordRemaining[p] && -1 == (p = getNextPart(p))) {
          return;
        }
      }
    }

    @Override
    public void cleanup(Context context)
        throws IOException, InterruptedException {
      // output any remaining records
      // TODO include reduce spec in remaining records if avail
      //      (i.e. move this to map)
      for (int i = 0; i < reduceRecordSize.length; ++i) {
        for (long j = reduceRecordRemaining[i]; j > 0; --j) {
          fillBytes(key, key.getLength());
          key.setType(GridmixKey.DATA);
          key.setPartition(i);
          fillBytes(val, reduceRecordSize[i]);
          context.write(key, val);
        }
      }
      val.setSize(0);
      key.setType(GridmixKey.REDUCE_SPEC);
      final int reduces = context.getNumReduceTasks();
      final GridmixSplit split = (GridmixSplit) context.getInputSplit();
      final int maps = split.getMapCount();
      int idx = 0;
      int id = split.getId();
      for (int i = 0; i < reduces; ++i) {
        key.setPartition(i);
        key.setReduceInputRecords(reduceRecordCount[i]);
        // Write spec for all red st r_id % id == 0
        if (i == id) {
          key.setReduceOutputBytes(split.getReduceBytes(idx));
          key.setReduceOutputRecords(split.getReduceRecords(idx));
          LOG.debug(String.format("SPEC'D %d / %d to %d",
                split.getReduceRecords(idx), split.getReduceBytes(idx), i));
          ++idx;
          id += maps;
        } else {
          key.setReduceOutputBytes(0);
          key.setReduceOutputRecords(0);
        }
        context.write(key, val);
      }
    }
  }

  public static class GridmixReducer
      extends Reducer<GridmixKey,BytesWritable,NullWritable,BytesWritable> {

    private final Random r = new Random();
    private final BytesWritable val = new BytesWritable();

    private double acc;
    private double ratio;
    private long written;
    private long inRecords = 0L;
    private long outBytes = 0L;
    private long outRecords = 0L;

    protected void fillBytes(BytesWritable val, int len) {
      r.nextBytes(val.getBytes());
      val.setSize(len);
    }

    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {
      if (!context.nextKey() ||
           context.getCurrentKey().getType() != GridmixKey.REDUCE_SPEC) {
        throw new IOException("Missing reduce spec");
      }
      for (BytesWritable ignored : context.getValues()) {
        final GridmixKey spec = context.getCurrentKey();
        inRecords += spec.getReduceInputRecords();
        LOG.debug("GOT COUNT " + spec.getReduceInputRecords());
        outBytes += spec.getReduceOutputBytes();
        outRecords += spec.getReduceOutputRecords();
      }
      LOG.debug("GOT SPEC " + outRecords + "/" + outBytes);
      val.setCapacity(Math.round(outBytes / (1.0f * outRecords)));
      ratio = outRecords / (1.0 * inRecords);
      acc = 0.0;
      LOG.debug(String.format("RECV %d -> %10d/%10d %d %f", inRecords,
            outRecords, outBytes, val.getCapacity(), ratio));
    }
    @Override
    protected void reduce(GridmixKey key, Iterable<BytesWritable> values,
        Context context) throws IOException, InterruptedException {
      for (BytesWritable ignored : values) {
        acc += ratio;
        while (acc >= 1.0 && written < outBytes) {
          final int len = (int) Math.min(outBytes - written, val.getCapacity());
          fillBytes(val, len);
          context.write(NullWritable.get(), val);
          acc -= 1.0;
          written += len;
          LOG.debug(String.format("%f %d/%d", acc, written, outBytes));
        }
      }
    }
    @Override
    protected void cleanup(Context context)
        throws IOException, InterruptedException {
      while (written < outBytes) {
        final int len = (int) Math.min(outBytes - written, val.getCapacity());
        fillBytes(val, len);
        context.write(NullWritable.get(), val);
        written += len;
      }
    }
  }

  static class GridmixRecordReader
      extends RecordReader<IntWritable,BytesWritable> {

    private long bytesRead = 0;
    private long bytesTotal;
    private Configuration conf;
    private final IntWritable key = new IntWritable();
    private final BytesWritable inBytes = new BytesWritable();

    private FSDataInputStream input;
    private int idx = -1;
    private int capacity;
    private Path[] paths;
    private long[] startoffset;
    private long[] lengths;

    public GridmixRecordReader() { }

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext ctxt)
            throws IOException, InterruptedException {
      final GridmixSplit split = (GridmixSplit)genericSplit;
      this.conf = ctxt.getConfiguration();
      paths = split.getPaths();
      startoffset = split.getStartOffsets();
      lengths = split.getLengths();
      bytesTotal = split.getLength();
      capacity = (int) Math.round(bytesTotal / (1.0 * split.getInputRecords()));
      inBytes.setCapacity(capacity);
      nextSource();
    }
    private void nextSource() throws IOException {
      idx = (idx + 1) % paths.length;
      final Path file = paths[idx];
      final FileSystem fs = file.getFileSystem(conf);
      input = fs.open(file, capacity);
      input.seek(startoffset[idx]);
    }
    @Override
    public boolean nextKeyValue() throws IOException {
      if (bytesRead >= bytesTotal) {
        return false;
      }
      final int len = (int)
        Math.min(bytesTotal - bytesRead, inBytes.getCapacity());
      int kvread = 0;
      while (kvread < len) {
        assert lengths[idx] >= 0;
        if (lengths[idx] <= 0) {
          nextSource();
          continue;
        }
        final int srcRead = (int) Math.min(len - kvread, lengths[idx]);
        IOUtils.readFully(input, inBytes.getBytes(), kvread, srcRead);
        //LOG.trace("Read " + srcRead + " bytes from " + paths[idx]);
        lengths[idx] -= srcRead;
        kvread += srcRead;
      }
      bytesRead += kvread;
      return true;
    }
    @Override
    public float getProgress() throws IOException {
      return bytesRead / ((float)bytesTotal);
    }
    @Override
    public IntWritable getCurrentKey() { return key; }
    @Override
    public BytesWritable getCurrentValue() { return inBytes; }
    @Override
    public void close() throws IOException {
      IOUtils.cleanup(null, input);
    }
  }

  static class GridmixSplit extends CombineFileSplit {
    private int id;
    private int nSpec;
    private int maps;
    private int reduces;
    private long inputRecords;
    private long outputBytes;
    private long outputRecords;
    private long maxMemory;
    private double[] reduceBytes = new double[0];
    private double[] reduceRecords = new double[0];

    // Spec for reduces id mod this
    private long[] reduceOutputBytes = new long[0];
    private long[] reduceOutputRecords = new long[0];

    GridmixSplit() {
      super();
    }

    public GridmixSplit(CombineFileSplit cfsplit, int maps, int id,
        long inputBytes, long inputRecords, long outputBytes,
        long outputRecords, double[] reduceBytes, double[] reduceRecords,
        long[] reduceOutputBytes, long[] reduceOutputRecords)
        throws IOException {
      super(cfsplit);
      this.id = id;
      this.maps = maps;
      reduces = reduceBytes.length;
      this.inputRecords = inputRecords;
      this.outputBytes = outputBytes;
      this.outputRecords = outputRecords;
      this.reduceBytes = Arrays.copyOf(reduceBytes, reduces);
      this.reduceRecords = Arrays.copyOf(reduceRecords, reduces);
      nSpec = reduceOutputBytes.length;
      this.reduceOutputBytes = reduceOutputBytes;
      this.reduceOutputRecords = reduceOutputRecords;
    }
    public int getId() {
      return id;
    }
    public int getMapCount() {
      return maps;
    }
    public long getInputRecords() {
      return inputRecords;
    }
    public long[] getOutputBytes() {
      final long[] ret = new long[reduces];
      for (int i = 0; i < reduces; ++i) {
        ret[i] = Math.round(outputBytes * reduceBytes[i]);
      }
      return ret;
    }
    public long[] getOutputRecords() {
      final long[] ret = new long[reduces];
      for (int i = 0; i < reduces; ++i) {
        ret[i] = Math.round(outputRecords * reduceRecords[i]);
      }
      return ret;
    }
    public long getReduceBytes(int i) {
      return reduceOutputBytes[i];
    }
    public long getReduceRecords(int i) {
      return reduceOutputRecords[i];
    }
    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      WritableUtils.writeVInt(out, id);
      WritableUtils.writeVInt(out, maps);
      WritableUtils.writeVLong(out, inputRecords);
      WritableUtils.writeVLong(out, outputBytes);
      WritableUtils.writeVLong(out, outputRecords);
      WritableUtils.writeVLong(out, maxMemory);
      WritableUtils.writeVInt(out, reduces);
      for (int i = 0; i < reduces; ++i) {
        out.writeDouble(reduceBytes[i]);
        out.writeDouble(reduceRecords[i]);
      }
      WritableUtils.writeVInt(out, nSpec);
      for (int i = 0; i < nSpec; ++i) {
        out.writeLong(reduceOutputBytes[i]);
        out.writeLong(reduceOutputRecords[i]);
      }
    }
    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      id = WritableUtils.readVInt(in);
      maps = WritableUtils.readVInt(in);
      inputRecords = WritableUtils.readVLong(in);
      outputBytes = WritableUtils.readVLong(in);
      outputRecords = WritableUtils.readVLong(in);
      maxMemory = WritableUtils.readVLong(in);
      reduces = WritableUtils.readVInt(in);
      if (reduceBytes.length < reduces) {
        reduceBytes = new double[reduces];
        reduceRecords = new double[reduces];
      }
      for (int i = 0; i < reduces; ++i) {
        reduceBytes[i] = in.readDouble();
        reduceRecords[i] = in.readDouble();
      }
      nSpec = WritableUtils.readVInt(in);
      if (reduceOutputBytes.length < nSpec) {
        reduceOutputBytes = new long[nSpec];
        reduceOutputRecords = new long[nSpec];
      }
      for (int i = 0; i < nSpec; ++i) {
        reduceOutputBytes[i] = in.readLong();
        reduceOutputRecords[i] = in.readLong();
      }
    }
  }

  static class GridmixInputFormat
      extends InputFormat<IntWritable,BytesWritable> {

    @Override
    public List<InputSplit> getSplits(JobContext jobCtxt) throws IOException {
      return pullDescription(jobCtxt.getConfiguration().getInt(
            "gridmix.job.seq", -1));
    }
    @Override
    public RecordReader<IntWritable,BytesWritable> createRecordReader(
        InputSplit split, final TaskAttemptContext taskContext)
        throws IOException {
      return new GridmixRecordReader();
    }
  }

  static class RawBytesOutputFormat
      extends FileOutputFormat<NullWritable,BytesWritable> {

    @Override
    public RecordWriter<NullWritable,BytesWritable> getRecordWriter(
        TaskAttemptContext job) throws IOException {

      Path file = getDefaultWorkFile(job, "");
      FileSystem fs = file.getFileSystem(job.getConfiguration());
      final FSDataOutputStream fileOut = fs.create(file, false);
      return new RecordWriter<NullWritable,BytesWritable>() {
        @Override
        public void write(NullWritable key, BytesWritable value)
            throws IOException {
          //LOG.trace("WROTE " + value.getLength() + " bytes");
          fileOut.write(value.getBytes(), 0, value.getLength());
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
        LOG.debug(String.format("SPEC(%d) %d -> %d %d %d", id(), i,
            i + j * maps, info.getOutputRecords(), info.getOutputBytes()));
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

  static class InputStriper {
    int idx;
    long currentStart;
    FileStatus current;
    final List<FileStatus> files = new ArrayList<FileStatus>();

    InputStriper(FilePool inputDir, long mapBytes)
        throws IOException {
      final long inputBytes = inputDir.getInputFiles(mapBytes, files);
      if (mapBytes > inputBytes) {
        LOG.warn("Using " + inputBytes + "/" + mapBytes + " bytes");
      }
      current = files.get(0);
    }

    CombineFileSplit splitFor(FilePool inputDir, long bytes, int nLocs)
        throws IOException {
      final ArrayList<Path> paths = new ArrayList<Path>();
      final ArrayList<Long> start = new ArrayList<Long>();
      final ArrayList<Long> length = new ArrayList<Long>();
      final HashMap<String,Double> sb = new HashMap<String,Double>();
      while (bytes > 0) {
        paths.add(current.getPath());
        start.add(currentStart);
        final long fromFile = Math.min(bytes, current.getLen() - currentStart);
        length.add(fromFile);
        for (BlockLocation loc :
            inputDir.locationsFor(current, currentStart, fromFile)) {
          final double tedium = loc.getLength() / (1.0 * bytes);
          for (String l : loc.getHosts()) {
            Double j = sb.get(l);
            if (null == j) {
              sb.put(l, tedium);
            } else {
              sb.put(l, j.doubleValue() + tedium);
            }
          }
        }
        currentStart += fromFile;
        bytes -= fromFile;
        if (current.getLen() - currentStart == 0) {
          current = files.get(++idx % files.size());
          currentStart = 0;
        }
      }
      final ArrayList<Entry<String,Double>> sort =
        new ArrayList<Entry<String,Double>>(sb.entrySet());
      Collections.sort(sort, hostRank);
      final String[] hosts = new String[Math.min(nLocs, sort.size())];
      for (int i = 0; i < nLocs && i < sort.size(); ++i) {
        hosts[i] = sort.get(i).getKey();
      }
      return new CombineFileSplit(paths.toArray(new Path[0]),
          toLongArray(start), toLongArray(length), hosts);
    }

    private long[] toLongArray(final ArrayList<Long> sigh) {
      final long[] ret = new long[sigh.size()];
      for (int i = 0; i < ret.length; ++i) {
        ret[i] = sigh.get(i);
      }
      return ret;
    }

    final Comparator<Entry<String,Double>> hostRank =
      new Comparator<Entry<String,Double>>() {
        public int compare(Entry<String,Double> a, Entry<String,Double> b) {
            final double va = a.getValue();
            final double vb = b.getValue();
            return va > vb ? -1 : va < vb ? 1 : 0;
          }
      };
  }
}
