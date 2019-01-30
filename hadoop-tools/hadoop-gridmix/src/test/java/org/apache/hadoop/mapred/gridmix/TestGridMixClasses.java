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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.CustomOutputCommitter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.gridmix.GridmixKey.Spec;
import org.apache.hadoop.mapred.gridmix.SleepJob.SleepReducer;
import org.apache.hadoop.mapred.gridmix.SleepJob.SleepSplit;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.hadoop.mapreduce.task.ReduceContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl.DummyReporter;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.rumen.JobStory;
import org.apache.hadoop.tools.rumen.JobStoryProducer;
import org.apache.hadoop.tools.rumen.ResourceUsageMetrics;
import org.apache.hadoop.tools.rumen.ZombieJobProducer;
import org.apache.hadoop.util.Progress;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.*;

import static org.junit.Assert.*;

public class TestGridMixClasses {
  private static final Logger LOG = LoggerFactory.getLogger(TestGridMixClasses.class);

  /*
   * simple test LoadSplit (getters,copy, write, read...)
   */
  @Test (timeout=1000)
  public void testLoadSplit() throws Exception {

    LoadSplit test = getLoadSplit();

    ByteArrayOutputStream data = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(data);
    test.write(out);
    LoadSplit copy = new LoadSplit();
    copy.readFields(new DataInputStream(new ByteArrayInputStream(data
            .toByteArray())));

    // data should be the same
    assertEquals(test.getId(), copy.getId());
    assertEquals(test.getMapCount(), copy.getMapCount());
    assertEquals(test.getInputRecords(), copy.getInputRecords());

    assertEquals(test.getOutputBytes()[0], copy.getOutputBytes()[0]);
    assertEquals(test.getOutputRecords()[0], copy.getOutputRecords()[0]);
    assertEquals(test.getReduceBytes(0), copy.getReduceBytes(0));
    assertEquals(test.getReduceRecords(0), copy.getReduceRecords(0));
    assertEquals(test.getMapResourceUsageMetrics().getCumulativeCpuUsage(),
            copy.getMapResourceUsageMetrics().getCumulativeCpuUsage());
    assertEquals(test.getReduceResourceUsageMetrics(0).getCumulativeCpuUsage(),
            copy.getReduceResourceUsageMetrics(0).getCumulativeCpuUsage());

  }

  /*
   * simple test GridmixSplit (copy, getters, write, read..)
   */
  @Test (timeout=1000)
  public void testGridmixSplit() throws Exception {
    Path[] files = {new Path("one"), new Path("two")};
    long[] start = {1, 2};
    long[] lengths = {100, 200};
    String[] locations = {"locOne", "loctwo"};

    CombineFileSplit cfSplit = new CombineFileSplit(files, start, lengths,
            locations);
    ResourceUsageMetrics metrics = new ResourceUsageMetrics();
    metrics.setCumulativeCpuUsage(200);

    double[] reduceBytes = {8.1d, 8.2d};
    double[] reduceRecords = {9.1d, 9.2d};
    long[] reduceOutputBytes = {101L, 102L};
    long[] reduceOutputRecords = {111L, 112L};

    GridmixSplit test = new GridmixSplit(cfSplit, 2, 3, 4L, 5L, 6L, 7L,
            reduceBytes, reduceRecords, reduceOutputBytes, reduceOutputRecords);

    ByteArrayOutputStream data = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(data);
    test.write(out);
    GridmixSplit copy = new GridmixSplit();
    copy.readFields(new DataInputStream(new ByteArrayInputStream(data
            .toByteArray())));

    // data should be the same
    assertEquals(test.getId(), copy.getId());
    assertEquals(test.getMapCount(), copy.getMapCount());
    assertEquals(test.getInputRecords(), copy.getInputRecords());

    assertEquals(test.getOutputBytes()[0], copy.getOutputBytes()[0]);
    assertEquals(test.getOutputRecords()[0], copy.getOutputRecords()[0]);
    assertEquals(test.getReduceBytes(0), copy.getReduceBytes(0));
    assertEquals(test.getReduceRecords(0), copy.getReduceRecords(0));

  }

  /*
   * test LoadMapper loadMapper should write to writer record for each reduce
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  @Test (timeout=10000)
  public void testLoadMapper() throws Exception {

    Configuration conf = new Configuration();
    conf.setInt(JobContext.NUM_REDUCES, 2);

    CompressionEmulationUtil.setCompressionEmulationEnabled(conf, true);
    conf.setBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, true);

    TaskAttemptID taskId = new TaskAttemptID();
    RecordReader<NullWritable, GridmixRecord> reader = new FakeRecordReader();

    LoadRecordGkGrWriter writer = new LoadRecordGkGrWriter();

    OutputCommitter committer = new CustomOutputCommitter();
    StatusReporter reporter = new TaskAttemptContextImpl.DummyReporter();
    LoadSplit split = getLoadSplit();

    MapContext<NullWritable, GridmixRecord, GridmixKey, GridmixRecord> mapContext = new MapContextImpl<NullWritable, GridmixRecord, GridmixKey, GridmixRecord>(
            conf, taskId, reader, writer, committer, reporter, split);
    // context
    Context ctx = new WrappedMapper<NullWritable, GridmixRecord, GridmixKey, GridmixRecord>()
            .getMapContext(mapContext);

    reader.initialize(split, ctx);
    ctx.getConfiguration().setBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, true);
    CompressionEmulationUtil.setCompressionEmulationEnabled(
            ctx.getConfiguration(), true);

    LoadJob.LoadMapper mapper = new LoadJob.LoadMapper();
    // setup, map, clean
    mapper.run(ctx);

    Map<GridmixKey, GridmixRecord> data = writer.getData();
    // check result
    assertEquals(2, data.size());

  }

  private LoadSplit getLoadSplit() throws Exception {

    Path[] files = {new Path("one"), new Path("two")};
    long[] start = {1, 2};
    long[] lengths = {100, 200};
    String[] locations = {"locOne", "loctwo"};

    CombineFileSplit cfSplit = new CombineFileSplit(files, start, lengths,
            locations);
    ResourceUsageMetrics metrics = new ResourceUsageMetrics();
    metrics.setCumulativeCpuUsage(200);
    ResourceUsageMetrics[] rMetrics = {metrics};

    double[] reduceBytes = {8.1d, 8.2d};
    double[] reduceRecords = {9.1d, 9.2d};
    long[] reduceOutputBytes = {101L, 102L};
    long[] reduceOutputRecords = {111L, 112L};

    return new LoadSplit(cfSplit, 2, 1, 4L, 5L, 6L, 7L,
            reduceBytes, reduceRecords, reduceOutputBytes, reduceOutputRecords,
            metrics, rMetrics);
  }

  private class FakeRecordLLReader extends
          RecordReader<LongWritable, LongWritable> {

    int counter = 10;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      counter--;
      return counter > 0;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException,
            InterruptedException {

      return new LongWritable(counter);
    }

    @Override
    public LongWritable getCurrentValue() throws IOException,
            InterruptedException {
      return new LongWritable(counter * 10);
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return counter / 10.0f;
    }

    @Override
    public void close() throws IOException {
      // restore data
      counter = 10;
    }
  }

  private class FakeRecordReader extends
          RecordReader<NullWritable, GridmixRecord> {

    int counter = 10;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      counter--;
      return counter > 0;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException,
            InterruptedException {

      return NullWritable.get();
    }

    @Override
    public GridmixRecord getCurrentValue() throws IOException,
            InterruptedException {
      return new GridmixRecord(100, 100L);
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return counter / 10.0f;
    }

    @Override
    public void close() throws IOException {
      // restore data
      counter = 10;
    }
  }

  private class LoadRecordGkGrWriter extends
          RecordWriter<GridmixKey, GridmixRecord> {
    private Map<GridmixKey, GridmixRecord> data = new HashMap<GridmixKey, GridmixRecord>();

    @Override
    public void write(GridmixKey key, GridmixRecord value) throws IOException,
            InterruptedException {
      data.put(key, value);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
            InterruptedException {
    }

    public Map<GridmixKey, GridmixRecord> getData() {
      return data;
    }

  }

  private class LoadRecordGkNullWriter extends
          RecordWriter<GridmixKey, NullWritable> {
    private Map<GridmixKey, NullWritable> data = new HashMap<GridmixKey, NullWritable>();

    @Override
    public void write(GridmixKey key, NullWritable value) throws IOException,
            InterruptedException {
      data.put(key, value);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
            InterruptedException {
    }

    public Map<GridmixKey, NullWritable> getData() {
      return data;
    }

  }

  private class LoadRecordWriter extends
          RecordWriter<NullWritable, GridmixRecord> {
    private Map<NullWritable, GridmixRecord> data = new HashMap<NullWritable, GridmixRecord>();

    @Override
    public void write(NullWritable key, GridmixRecord value)
            throws IOException, InterruptedException {
      data.put(key, value);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
            InterruptedException {
    }

    public Map<NullWritable, GridmixRecord> getData() {
      return data;
    }

  }

  /*
   * test LoadSortComparator
   */
  @Test (timeout=3000)
  public void testLoadJobLoadSortComparator() throws Exception {
    LoadJob.LoadSortComparator test = new LoadJob.LoadSortComparator();

    ByteArrayOutputStream data = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(data);
    WritableUtils.writeVInt(dos, 2);
    WritableUtils.writeVInt(dos, 1);
    WritableUtils.writeVInt(dos, 4);
    WritableUtils.writeVInt(dos, 7);
    WritableUtils.writeVInt(dos, 4);

    byte[] b1 = data.toByteArray();

    byte[] b2 = data.toByteArray();

    // the same data should be equals
    assertEquals(0, test.compare(b1, 0, 1, b2, 0, 1));
    b2[2] = 5;
    // compare like GridMixKey first byte: shift count -1=4-5
    assertEquals(-1, test.compare(b1, 0, 1, b2, 0, 1));
    b2[2] = 2;
    // compare like GridMixKey first byte: shift count 2=4-2
    assertEquals(2, test.compare(b1, 0, 1, b2, 0, 1));
    // compare arrays by first byte witch offset (2-1) because 4==4
    b2[2] = 4;
    assertEquals(1, test.compare(b1, 0, 1, b2, 1, 1));

  }

  /*
   * test SpecGroupingComparator
   */
  @Test (timeout=3000)
  public void testGridmixJobSpecGroupingComparator() throws Exception {
    GridmixJob.SpecGroupingComparator test = new GridmixJob.SpecGroupingComparator();

    ByteArrayOutputStream data = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(data);
    WritableUtils.writeVInt(dos, 2);
    WritableUtils.writeVInt(dos, 1);
    // 0: REDUCE SPEC
    WritableUtils.writeVInt(dos, 0);
    WritableUtils.writeVInt(dos, 7);
    WritableUtils.writeVInt(dos, 4);

    byte[] b1 = data.toByteArray();

    byte[] b2 = data.toByteArray();

    // the same object should be equals
    assertEquals(0, test.compare(b1, 0, 1, b2, 0, 1));
    b2[2] = 1;
    // for Reduce
    assertEquals(-1, test.compare(b1, 0, 1, b2, 0, 1));
    // by Reduce spec
    b2[2] = 1; // 1: DATA SPEC
    assertEquals(-1, test.compare(b1, 0, 1, b2, 0, 1));
    // compare GridmixKey the same objects should be equals
    assertEquals(0, test.compare(new GridmixKey(GridmixKey.DATA, 100, 2),
            new GridmixKey(GridmixKey.DATA, 100, 2)));
    // REDUSE SPEC
    assertEquals(-1, test.compare(
            new GridmixKey(GridmixKey.REDUCE_SPEC, 100, 2), new GridmixKey(
            GridmixKey.DATA, 100, 2)));
    assertEquals(1, test.compare(new GridmixKey(GridmixKey.DATA, 100, 2),
            new GridmixKey(GridmixKey.REDUCE_SPEC, 100, 2)));
    // only DATA
    assertEquals(2, test.compare(new GridmixKey(GridmixKey.DATA, 102, 2),
            new GridmixKey(GridmixKey.DATA, 100, 2)));

  }

  /*
   * test CompareGridmixJob only equals and compare
   */
  @Test (timeout=30000)
  public void testCompareGridmixJob() throws Exception {
    Configuration conf = new Configuration();
    Path outRoot = new Path("target");
    JobStory jobDesc = mock(JobStory.class);
    when(jobDesc.getName()).thenReturn("JobName");
    when(jobDesc.getJobConf()).thenReturn(new JobConf(conf));
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    GridmixJob j1 = new LoadJob(conf, 1000L, jobDesc, outRoot, ugi, 0);
    GridmixJob j2 = new LoadJob(conf, 1000L, jobDesc, outRoot, ugi, 0);
    GridmixJob j3 = new LoadJob(conf, 1000L, jobDesc, outRoot, ugi, 1);
    GridmixJob j4 = new LoadJob(conf, 1000L, jobDesc, outRoot, ugi, 1);

    assertTrue(j1.equals(j2));
    assertEquals(0, j1.compareTo(j2));
    // Only one parameter matters
    assertFalse(j1.equals(j3));
    // compare id and submissionMillis
    assertEquals(-1, j1.compareTo(j3));
    assertEquals(-1, j1.compareTo(j4));

  }

  /*
   * test ReadRecordFactory. should read all data from inputstream
   */
  @Test (timeout=3000)
  public void testReadRecordFactory() throws Exception {

    // RecordFactory factory, InputStream src, Configuration conf
    RecordFactory rf = new FakeRecordFactory();
    FakeInputStream input = new FakeInputStream();
    ReadRecordFactory test = new ReadRecordFactory(rf, input,
            new Configuration());
    GridmixKey key = new GridmixKey(GridmixKey.DATA, 100, 2);
    GridmixRecord val = new GridmixRecord(200, 2);
    while (test.next(key, val)) {

    }
    // should be read 10* (GridmixKey.size +GridmixRecord.value)
    assertEquals(3000, input.getCounter());
    // should be -1 because all data readed;
    assertEquals(-1, rf.getProgress(), 0.01);

    test.close();
  }

  private class FakeRecordFactory extends RecordFactory {

    private int counter = 10;

    @Override
    public void close() throws IOException {

    }

    @Override
    public boolean next(GridmixKey key, GridmixRecord val) throws IOException {
      counter--;
      return counter >= 0;
    }

    @Override
    public float getProgress() throws IOException {
      return counter;
    }

  }

  private class FakeInputStream extends InputStream implements Seekable,
          PositionedReadable {
    private long counter;

    @Override
    public int read() throws IOException {
      return 0;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int realLen = len - off;
      counter += realLen;
      for (int i = 0; i < b.length; i++) {
        b[i] = 0;
      }
      return realLen;
    }

    public long getCounter() {
      return counter;
    }

    @Override
    public void seek(long pos) throws IOException {

    }

    @Override
    public long getPos() throws IOException {
      return counter;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length)
            throws IOException {
      return 0;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException {

    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {

    }
  }

  private class FakeFSDataInputStream extends FSDataInputStream {

    public FakeFSDataInputStream(InputStream in) throws IOException {
      super(in);

    }

  }

  /*
   * test LoadRecordReader. It class reads data from some files.
   */
  @Test (timeout=3000)
  public void testLoadJobLoadRecordReader() throws Exception {
    LoadJob.LoadRecordReader test = new LoadJob.LoadRecordReader();
    Configuration conf = new Configuration();

    FileSystem fs1 = mock(FileSystem.class);
    when(fs1.open(any(Path.class))).thenReturn(
            new FakeFSDataInputStream(new FakeInputStream()));
    Path p1 = mock(Path.class);
    when(p1.getFileSystem(any())).thenReturn(fs1);

    FileSystem fs2 = mock(FileSystem.class);
    when(fs2.open(any(Path.class))).thenReturn(
            new FakeFSDataInputStream(new FakeInputStream()));
    Path p2 = mock(Path.class);
    when(p2.getFileSystem(any())).thenReturn(fs2);

    Path[] paths = {p1, p2};

    long[] start = {0, 0};
    long[] lengths = {1000, 1000};
    String[] locations = {"temp1", "temp2"};
    CombineFileSplit cfsplit = new CombineFileSplit(paths, start, lengths,
            locations);
    double[] reduceBytes = {100, 100};
    double[] reduceRecords = {2, 2};
    long[] reduceOutputBytes = {500, 500};
    long[] reduceOutputRecords = {2, 2};
    ResourceUsageMetrics metrics = new ResourceUsageMetrics();
    ResourceUsageMetrics[] rMetrics = {new ResourceUsageMetrics(),
            new ResourceUsageMetrics()};
    LoadSplit input = new LoadSplit(cfsplit, 2, 3, 1500L, 2L, 3000L, 2L,
            reduceBytes, reduceRecords, reduceOutputBytes, reduceOutputRecords,
            metrics, rMetrics);
    TaskAttemptID taskId = new TaskAttemptID();
    TaskAttemptContext ctx = new TaskAttemptContextImpl(conf, taskId);
    test.initialize(input, ctx);
    GridmixRecord gr = test.getCurrentValue();
    int counter = 0;
    while (test.nextKeyValue()) {
      gr = test.getCurrentValue();
      if (counter == 0) {
        // read first file
        assertEquals(0.5, test.getProgress(), 0.001);
      } else if (counter == 1) {
        // read second file
        assertEquals(1.0, test.getProgress(), 0.001);
      }
      //
      assertEquals(1000, gr.getSize());
      counter++;
    }
    assertEquals(1000, gr.getSize());
    // Two files have been read
    assertEquals(2, counter);

    test.close();
  }

  /*
   * test LoadReducer
   */

  @Test (timeout=3000)
  public void testLoadJobLoadReducer() throws Exception {
    LoadJob.LoadReducer test = new LoadJob.LoadReducer();

    Configuration conf = new Configuration();
    conf.setInt(JobContext.NUM_REDUCES, 2);
    CompressionEmulationUtil.setCompressionEmulationEnabled(conf, true);
    conf.setBoolean(FileOutputFormat.COMPRESS, true);

    CompressionEmulationUtil.setCompressionEmulationEnabled(conf, true);
    conf.setBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, true);
    TaskAttemptID taskid = new TaskAttemptID();

    RawKeyValueIterator input = new FakeRawKeyValueIterator();

    Counter counter = new GenericCounter();
    Counter inputValueCounter = new GenericCounter();
    LoadRecordWriter output = new LoadRecordWriter();

    OutputCommitter committer = new CustomOutputCommitter();

    StatusReporter reporter = new DummyReporter();
    RawComparator<GridmixKey> comparator = new FakeRawComparator();

    ReduceContext<GridmixKey, GridmixRecord, NullWritable, GridmixRecord> reduceContext = new ReduceContextImpl<GridmixKey, GridmixRecord, NullWritable, GridmixRecord>(
            conf, taskid, input, counter, inputValueCounter, output, committer,
            reporter, comparator, GridmixKey.class, GridmixRecord.class);
    // read for previous data
    reduceContext.nextKeyValue();
    org.apache.hadoop.mapreduce.Reducer<GridmixKey, GridmixRecord, NullWritable, GridmixRecord>.Context context = new WrappedReducer<GridmixKey, GridmixRecord, NullWritable, GridmixRecord>()
            .getReducerContext(reduceContext);

    // test.setup(context);
    test.run(context);
    // have been readed 9 records (-1 for previous)
    assertEquals(9, counter.getValue());
    assertEquals(10, inputValueCounter.getValue());
    assertEquals(1, output.getData().size());
    GridmixRecord record = output.getData().values().iterator()
            .next();

    assertEquals(1593, record.getSize());
  }

  protected class FakeRawKeyValueIterator implements RawKeyValueIterator {

    int counter = 10;

    @Override
    public DataInputBuffer getKey() throws IOException {
      ByteArrayOutputStream dt = new ByteArrayOutputStream();
      GridmixKey key = new GridmixKey(GridmixKey.REDUCE_SPEC, 10 * counter, 1L);
      Spec spec = new Spec();
      spec.rec_in = counter;
      spec.rec_out = counter;
      spec.bytes_out = counter * 100;

      key.setSpec(spec);
      key.write(new DataOutputStream(dt));
      DataInputBuffer result = new DataInputBuffer();
      byte[] b = dt.toByteArray();
      result.reset(b, 0, b.length);
      return result;
    }

    @Override
    public DataInputBuffer getValue() throws IOException {
      ByteArrayOutputStream dt = new ByteArrayOutputStream();
      GridmixRecord key = new GridmixRecord(100, 1);
      key.write(new DataOutputStream(dt));
      DataInputBuffer result = new DataInputBuffer();
      byte[] b = dt.toByteArray();
      result.reset(b, 0, b.length);
      return result;
    }

    @Override
    public boolean next() throws IOException {
      counter--;
      return counter >= 0;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public Progress getProgress() {
      return null;
    }

  }

  private class FakeRawComparator implements RawComparator<GridmixKey> {

    @Override
    public int compare(GridmixKey o1, GridmixKey o2) {
      return o1.compareTo(o2);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      if ((l1 - s1) != (l2 - s2)) {
        return (l1 - s1) - (l2 - s2);
      }
      int len = l1 - s1;
      for (int i = 0; i < len; i++) {
        if (b1[s1 + i] != b2[s2 + i]) {
          return b1[s1 + i] - b2[s2 + i];
        }
      }
      return 0;
    }

  }

  /*
   * test SerialJobFactory
   */
  @Test (timeout=120000)
  public void testSerialReaderThread() throws Exception {

    Configuration conf = new Configuration();
    File fin = new File("src" + File.separator + "test" + File.separator
            + "resources" + File.separator + "data" + File.separator
            + "wordcount2.json");
    // read couple jobs from wordcount2.json
    JobStoryProducer jobProducer = new ZombieJobProducer(new Path(
            fin.getAbsolutePath()), null, conf);
    CountDownLatch startFlag = new CountDownLatch(1);
    UserResolver resolver = new SubmitterUserResolver();
    FakeJobSubmitter submitter = new FakeJobSubmitter();
    File ws = new File("target" + File.separator + this.getClass().getName());
    if (!ws.exists()) {
      Assert.assertTrue(ws.mkdirs());
    }

    SerialJobFactory jobFactory = new SerialJobFactory(submitter, jobProducer,
            new Path(ws.getAbsolutePath()), conf, startFlag, resolver);

    Path ioPath = new Path(ws.getAbsolutePath());
    jobFactory.setDistCacheEmulator(new DistributedCacheEmulator(conf, ioPath));
    Thread test = jobFactory.createReaderThread();
    test.start();
    Thread.sleep(1000);
    // SerialReaderThread waits startFlag
    assertEquals(0, submitter.getJobs().size());
    // start!
    startFlag.countDown();
    while (test.isAlive()) {
      Thread.sleep(1000);
      jobFactory.update(null);
    }
    // submitter was called twice
    assertEquals(2, submitter.getJobs().size());
  }

  private class FakeJobSubmitter extends JobSubmitter {
    // counter for submitted jobs
    private List<GridmixJob> jobs = new ArrayList<GridmixJob>();

    public FakeJobSubmitter() {
      super(null, 1, 1, null, null);

    }

    @Override
    public void add(GridmixJob job) throws InterruptedException {
      jobs.add(job);
    }

    public List<GridmixJob> getJobs() {
      return jobs;
    }
  }

  /*
   * test SleepMapper
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test (timeout=30000)
  public void testSleepMapper() throws Exception {
    SleepJob.SleepMapper test = new SleepJob.SleepMapper();

    Configuration conf = new Configuration();
    conf.setInt(JobContext.NUM_REDUCES, 2);

    CompressionEmulationUtil.setCompressionEmulationEnabled(conf, true);
    conf.setBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, true);
    TaskAttemptID taskId = new TaskAttemptID();
    FakeRecordLLReader reader = new FakeRecordLLReader();
    LoadRecordGkNullWriter writer = new LoadRecordGkNullWriter();
    OutputCommitter committer = new CustomOutputCommitter();
    StatusReporter reporter = new TaskAttemptContextImpl.DummyReporter();
    SleepSplit split = getSleepSplit();
    MapContext<LongWritable, LongWritable, GridmixKey, NullWritable> mapcontext = new MapContextImpl<LongWritable, LongWritable, GridmixKey, NullWritable>(
            conf, taskId, reader, writer, committer, reporter, split);
    Context context = new WrappedMapper<LongWritable, LongWritable, GridmixKey, NullWritable>()
            .getMapContext(mapcontext);

    long start = System.currentTimeMillis();
    LOG.info("start:" + start);
    LongWritable key = new LongWritable(start + 2000);
    LongWritable value = new LongWritable(start + 2000);
    // should slip 2 sec
    test.map(key, value, context);
    LOG.info("finish:" + System.currentTimeMillis());
    assertTrue(System.currentTimeMillis() >= (start + 2000));

    test.cleanup(context);
    assertEquals(1, writer.getData().size());
  }

  private SleepSplit getSleepSplit() throws Exception {

    String[] locations = {"locOne", "loctwo"};

    long[] reduceDurations = {101L, 102L};

    return new SleepSplit(0, 2000L, reduceDurations, 2, locations);
  }

  /*
   * test SleepReducer
   */
  @Test (timeout=3000)
  public void testSleepReducer() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(JobContext.NUM_REDUCES, 2);
    CompressionEmulationUtil.setCompressionEmulationEnabled(conf, true);
    conf.setBoolean(FileOutputFormat.COMPRESS, true);

    CompressionEmulationUtil.setCompressionEmulationEnabled(conf, true);
    conf.setBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, true);
    TaskAttemptID taskId = new TaskAttemptID();

    RawKeyValueIterator input = new FakeRawKeyValueReducerIterator();

    Counter counter = new GenericCounter();
    Counter inputValueCounter = new GenericCounter();
    RecordWriter<NullWritable, NullWritable> output = new LoadRecordReduceWriter();

    OutputCommitter committer = new CustomOutputCommitter();

    StatusReporter reporter = new DummyReporter();
    RawComparator<GridmixKey> comparator = new FakeRawComparator();

    ReduceContext<GridmixKey, NullWritable, NullWritable, NullWritable> reducecontext = new ReduceContextImpl<GridmixKey, NullWritable, NullWritable, NullWritable>(
            conf, taskId, input, counter, inputValueCounter, output, committer,
            reporter, comparator, GridmixKey.class, NullWritable.class);
    org.apache.hadoop.mapreduce.Reducer<GridmixKey, NullWritable, NullWritable, NullWritable>.Context context = new WrappedReducer<GridmixKey, NullWritable, NullWritable, NullWritable>()
            .getReducerContext(reducecontext);

    SleepReducer test = new SleepReducer();
    long start = System.currentTimeMillis();
    test.setup(context);
    long sleeper = context.getCurrentKey().getReduceOutputBytes();
    // status has been changed
    assertEquals("Sleeping... " + sleeper + " ms left", context.getStatus());
    // should sleep 0.9 sec

    assertTrue(System.currentTimeMillis() >= (start + sleeper));
    test.cleanup(context);
    // status has been changed again

    assertEquals("Slept for " + sleeper, context.getStatus());

  }

  private class LoadRecordReduceWriter extends
          RecordWriter<NullWritable, NullWritable> {

    @Override
    public void write(NullWritable key, NullWritable value) throws IOException,
            InterruptedException {
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException,
            InterruptedException {
    }

  }

  protected class FakeRawKeyValueReducerIterator implements RawKeyValueIterator {

    int counter = 10;

    @Override
    public DataInputBuffer getKey() throws IOException {
      ByteArrayOutputStream dt = new ByteArrayOutputStream();
      GridmixKey key = new GridmixKey(GridmixKey.REDUCE_SPEC, 10 * counter, 1L);
      Spec spec = new Spec();
      spec.rec_in = counter;
      spec.rec_out = counter;
      spec.bytes_out = counter * 100;

      key.setSpec(spec);
      key.write(new DataOutputStream(dt));
      DataInputBuffer result = new DataInputBuffer();
      byte[] b = dt.toByteArray();
      result.reset(b, 0, b.length);
      return result;
    }

    @Override
    public DataInputBuffer getValue() throws IOException {
      ByteArrayOutputStream dt = new ByteArrayOutputStream();
      NullWritable key = NullWritable.get();
      key.write(new DataOutputStream(dt));
      DataInputBuffer result = new DataInputBuffer();
      byte[] b = dt.toByteArray();
      result.reset(b, 0, b.length);
      return result;
    }

    @Override
    public boolean next() throws IOException {
      counter--;
      return counter >= 0;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public Progress getProgress() {
      return null;
    }

  }
}
