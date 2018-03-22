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

package org.apache.hadoop.mapred.pipes;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskLog;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestPipeApplication {
  private static File workSpace = new File("target",
          TestPipeApplication.class.getName() + "-workSpace");

  private static String taskName = "attempt_001_02_r03_04_05";

  /**
   * test PipesMapRunner    test the transfer data from reader
   *
   * @throws Exception
   */
  @Test
  public void testRunner() throws Exception {

    // clean old password files
    JobConf conf = new JobConf();
    File[] psw = cleanTokenPasswordFile(conf);
    try {
      RecordReader<FloatWritable, NullWritable> rReader = new ReaderPipesMapRunner();
      conf.set(Submitter.IS_JAVA_RR, "true");
      // for stdour and stderror

      conf.set(MRJobConfig.TASK_ATTEMPT_ID, taskName);

      CombineOutputCollector<IntWritable, Text> output = new CombineOutputCollector<IntWritable, Text>(
              new Counters.Counter(), new Progress());
      FileSystem fs = new RawLocalFileSystem();
      fs.initialize(FsConstants.LOCAL_FS_URI, conf);
      Writer<IntWritable, Text> wr = new Writer<IntWritable, Text>(conf, fs.create(
              new Path(workSpace + File.separator + "outfile")), IntWritable.class,
              Text.class, null, null, true);
      output.setWriter(wr);
      // stub for client
      File fCommand = getFileCommand("org.apache.hadoop.mapred.pipes.PipeApplicationRunnableStub");

      conf.set(MRJobConfig.CACHE_LOCALFILES, fCommand.getAbsolutePath());
      // token for authorization
      Token<AMRMTokenIdentifier> token = new Token<AMRMTokenIdentifier>(
              "user".getBytes(), "password".getBytes(), new Text("kind"), new Text(
              "service"));
      TokenCache.setJobToken(token,  conf.getCredentials());
      conf.setBoolean(MRJobConfig.SKIP_RECORDS, true);
      TestTaskReporter reporter = new TestTaskReporter();
      PipesMapRunner<FloatWritable, NullWritable, IntWritable, Text> runner = new PipesMapRunner<FloatWritable, NullWritable, IntWritable, Text>();

      initStdOut(conf);

      runner.configure(conf);
      runner.run(rReader, output, reporter);

      String stdOut = readStdOut(conf);

      // test part of translated data. As common file for client and test -
      // clients stdOut
      // check version
      assertTrue(stdOut.contains("CURRENT_PROTOCOL_VERSION:0"));
      // check key and value classes
      assertTrue(stdOut
              .contains("Key class:org.apache.hadoop.io.FloatWritable"));
      assertTrue(stdOut
              .contains("Value class:org.apache.hadoop.io.NullWritable"));
      // test have sent all data from reader
      assertTrue(stdOut.contains("value:0.0"));
      assertTrue(stdOut.contains("value:9.0"));

    } finally {
      if (psw != null) {
        // remove password files
        for (File file : psw) {
          file.deleteOnExit();
        }
      }

    }
  }

  /**
   * test org.apache.hadoop.mapred.pipes.Application
   * test a internal functions: MessageType.REGISTER_COUNTER,  INCREMENT_COUNTER, STATUS, PROGRESS...
   *
   * @throws Throwable
   */

  @Test
  public void testApplication() throws Throwable {
    JobConf conf = new JobConf();

    RecordReader<FloatWritable, NullWritable> rReader = new Reader();

    // client for test
    File fCommand = getFileCommand("org.apache.hadoop.mapred.pipes.PipeApplicationStub");

    TestTaskReporter reporter = new TestTaskReporter();

    File[] psw = cleanTokenPasswordFile(conf);
    try {

      conf.set(MRJobConfig.TASK_ATTEMPT_ID, taskName);
      conf.set(MRJobConfig.CACHE_LOCALFILES, fCommand.getAbsolutePath());

      // token for authorization
      Token<AMRMTokenIdentifier> token = new Token<AMRMTokenIdentifier>(
              "user".getBytes(), "password".getBytes(), new Text("kind"), new Text(
              "service"));

      TokenCache.setJobToken(token, conf.getCredentials());
      FakeCollector output = new FakeCollector(new Counters.Counter(),
              new Progress());
      FileSystem fs = new RawLocalFileSystem();
      fs.initialize(FsConstants.LOCAL_FS_URI, conf);
      Writer<IntWritable, Text> wr = new Writer<IntWritable, Text>(conf, fs.create(
              new Path(workSpace.getAbsolutePath() + File.separator + "outfile")),
              IntWritable.class, Text.class, null, null, true);
      output.setWriter(wr);
      conf.set(Submitter.PRESERVE_COMMANDFILE, "true");

      initStdOut(conf);

      Application<WritableComparable<IntWritable>, Writable, IntWritable, Text> application = new Application<WritableComparable<IntWritable>, Writable, IntWritable, Text>(
              conf, rReader, output, reporter, IntWritable.class, Text.class);
      application.getDownlink().flush();

      application.getDownlink().mapItem(new IntWritable(3), new Text("txt"));

      application.getDownlink().flush();

      application.waitForFinish();

      wr.close();

      // test getDownlink().mapItem();
      String stdOut = readStdOut(conf);
      assertTrue(stdOut.contains("key:3"));
      assertTrue(stdOut.contains("value:txt"));

      // reporter test counter, and status should be sended
      // test MessageType.REGISTER_COUNTER and INCREMENT_COUNTER
      assertEquals(1.0, reporter.getProgress(), 0.01);
      assertNotNull(reporter.getCounter("group", "name"));
      // test status MessageType.STATUS
      assertEquals(reporter.getStatus(), "PROGRESS");
      stdOut = readFile(new File(workSpace.getAbsolutePath() + File.separator
              + "outfile"));
      // check MessageType.PROGRESS
      assertEquals(0.55f, rReader.getProgress(), 0.001);
      application.getDownlink().close();
      // test MessageType.OUTPUT
      Entry<IntWritable, Text> entry = output.getCollect().entrySet()
              .iterator().next();
      assertEquals(123, entry.getKey().get());
      assertEquals("value", entry.getValue().toString());
      try {
        // try to abort
        application.abort(new Throwable());
        fail();
      } catch (IOException e) {
        // abort works ?
        assertEquals("pipe child exception", e.getMessage());
      }
    } finally {
      if (psw != null) {
        // remove password files
        for (File file : psw) {
          file.deleteOnExit();
        }
      }
    }
  }

  /**
   * test org.apache.hadoop.mapred.pipes.Submitter
   *
   * @throws Exception
   */
  @Test
  public void testSubmitter() throws Exception {

    JobConf conf = new JobConf();

    File[] psw = cleanTokenPasswordFile(conf);

    System.setProperty("test.build.data",
            "target/tmp/build/TEST_SUBMITTER_MAPPER/data");
    conf.set("hadoop.log.dir", "target/tmp");

    // prepare configuration
    Submitter.setIsJavaMapper(conf, false);
    Submitter.setIsJavaReducer(conf, false);
    Submitter.setKeepCommandFile(conf, false);
    Submitter.setIsJavaRecordReader(conf, false);
    Submitter.setIsJavaRecordWriter(conf, false);
    PipesPartitioner<IntWritable, Text> partitioner = new PipesPartitioner<IntWritable, Text>();
    partitioner.configure(conf);

    Submitter.setJavaPartitioner(conf, partitioner.getClass());

    assertEquals(PipesPartitioner.class, (Submitter.getJavaPartitioner(conf)));
    // test going to call main method with System.exit(). Change Security
    SecurityManager securityManager = System.getSecurityManager();
    // store System.out
    PrintStream oldps = System.out;
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ExitUtil.disableSystemExit();
    // test without parameters
    try {
      System.setOut(new PrintStream(out));
      Submitter.main(new String[0]);
      fail();
    } catch (ExitUtil.ExitException e) {
      // System.exit prohibited! output message test
      assertTrue(out.toString().contains(""));
      assertTrue(out.toString(), out.toString().contains("pipes"));
      assertTrue(out.toString().contains("[-input <path>] // Input directory"));
      assertTrue(out.toString()
              .contains("[-output <path>] // Output directory"));
      assertTrue(out.toString().contains("[-jar <jar file> // jar filename"));
      assertTrue(out.toString().contains(
              "[-inputformat <class>] // InputFormat class"));
      assertTrue(out.toString().contains("[-map <class>] // Java Map class"));
      assertTrue(out.toString().contains(
              "[-partitioner <class>] // Java Partitioner"));
      assertTrue(out.toString().contains(
              "[-reduce <class>] // Java Reduce class"));
      assertTrue(out.toString().contains(
              "[-writer <class>] // Java RecordWriter"));
      assertTrue(out.toString().contains(
              "[-program <executable>] // executable URI"));
      assertTrue(out.toString().contains(
              "[-reduces <num>] // number of reduces"));
      assertTrue(out.toString().contains(
              "[-lazyOutput <true/false>] // createOutputLazily"));

      assertTrue(out.toString().contains(
          "-conf <configuration file>        specify an application "
              + "configuration file"));
      assertTrue(out.toString().contains(
          "-D <property=value>               define a value for a given "
              + "property"));
      assertTrue(out.toString()
          .contains("-fs <file:///|hdfs://namenode:port> "
              + "specify default filesystem URL to use, overrides "
              + "'fs.defaultFS' property from configurations."));
      assertTrue(out.toString().contains(
          "-jt <local|resourcemanager:port>  specify a ResourceManager"));
      assertTrue(out.toString().contains(
          "-files <file1,...>                specify a comma-separated list of "
              + "files to be copied to the map reduce cluster"));
      assertTrue(out.toString().contains(
          "-libjars <jar1,...>               specify a comma-separated list of "
              + "jar files to be included in the classpath"));
      assertTrue(out.toString().contains(
          "-archives <archive1,...>          specify a comma-separated list of "
              + "archives to be unarchived on the compute machines"));
    } finally {
      System.setOut(oldps);
      // restore
      System.setSecurityManager(securityManager);
      if (psw != null) {
        // remove password files
        for (File file : psw) {
          file.deleteOnExit();
        }
      }
    }
    // test call Submitter form command line
    try {
      File fCommand = getFileCommand(null);
      String[] args = new String[22];
      File input = new File(workSpace + File.separator + "input");
      if (!input.exists()) {
        Assert.assertTrue(input.createNewFile());
      }
      File outPut = new File(workSpace + File.separator + "output");
      FileUtil.fullyDelete(outPut);

      args[0] = "-input";
      args[1] = input.getAbsolutePath();// "input";
      args[2] = "-output";
      args[3] = outPut.getAbsolutePath();// "output";
      args[4] = "-inputformat";
      args[5] = "org.apache.hadoop.mapred.TextInputFormat";
      args[6] = "-map";
      args[7] = "org.apache.hadoop.mapred.lib.IdentityMapper";
      args[8] = "-partitioner";
      args[9] = "org.apache.hadoop.mapred.pipes.PipesPartitioner";
      args[10] = "-reduce";
      args[11] = "org.apache.hadoop.mapred.lib.IdentityReducer";
      args[12] = "-writer";
      args[13] = "org.apache.hadoop.mapred.TextOutputFormat";
      args[14] = "-program";
      args[15] = fCommand.getAbsolutePath();// "program";
      args[16] = "-reduces";
      args[17] = "2";
      args[18] = "-lazyOutput";
      args[19] = "lazyOutput";
      args[20] = "-jobconf";
      args[21] = "mapreduce.pipes.isjavarecordwriter=false,mapreduce.pipes.isjavarecordreader=false";

      Submitter.main(args);
      fail();
    } catch (ExitUtil.ExitException e) {
      // status should be 0
      assertEquals(e.status, 0);

    } finally {
      System.setOut(oldps);
      System.setSecurityManager(securityManager);
    }

  }

  /**
   * test org.apache.hadoop.mapred.pipes.PipesReducer
   * test the transfer of data: key and value
   *
   * @throws Exception
   */
  @Test
  public void testPipesReduser() throws Exception {

    JobConf conf = new JobConf();
    File[] psw = cleanTokenPasswordFile(conf);
    try {
      Token<AMRMTokenIdentifier> token = new Token<AMRMTokenIdentifier>(
              "user".getBytes(), "password".getBytes(), new Text("kind"), new Text(
              "service"));
      TokenCache.setJobToken(token, conf.getCredentials());

      File fCommand = getFileCommand("org.apache.hadoop.mapred.pipes.PipeReducerStub");
      conf.set(MRJobConfig.CACHE_LOCALFILES, fCommand.getAbsolutePath());

      PipesReducer<BooleanWritable, Text, IntWritable, Text> reducer = new PipesReducer<BooleanWritable, Text, IntWritable, Text>();
      reducer.configure(conf);
      BooleanWritable bw = new BooleanWritable(true);

      conf.set(MRJobConfig.TASK_ATTEMPT_ID, taskName);
      initStdOut(conf);
      conf.setBoolean(MRJobConfig.SKIP_RECORDS, true);
      CombineOutputCollector<IntWritable, Text> output = new CombineOutputCollector<IntWritable, Text>(
              new Counters.Counter(), new Progress());
      Reporter reporter = new TestTaskReporter();
      List<Text> texts = new ArrayList<Text>();
      texts.add(new Text("first"));
      texts.add(new Text("second"));
      texts.add(new Text("third"));

      reducer.reduce(bw, texts.iterator(), output, reporter);
      reducer.close();
      String stdOut = readStdOut(conf);
      // test data: key
      assertTrue(stdOut.contains("reducer key :true"));
      // and values
      assertTrue(stdOut.contains("reduce value  :first"));
      assertTrue(stdOut.contains("reduce value  :second"));
      assertTrue(stdOut.contains("reduce value  :third"));

    } finally {
      if (psw != null) {
        // remove password files
        for (File file : psw) {
          file.deleteOnExit();
        }
      }
    }

  }

  /**
   * test PipesPartitioner
   * test set and get data from  PipesPartitioner
   */
  @Test
  public void testPipesPartitioner() {

    PipesPartitioner<IntWritable, Text> partitioner = new PipesPartitioner<IntWritable, Text>();
    JobConf configuration = new JobConf();
    Submitter.getJavaPartitioner(configuration);
    partitioner.configure(new JobConf());
    IntWritable iw = new IntWritable(4);
    // the cache empty
    assertEquals(0, partitioner.getPartition(iw, new Text("test"), 2));
    // set data into cache
    PipesPartitioner.setNextPartition(3);
    // get data from cache
    assertEquals(3, partitioner.getPartition(iw, new Text("test"), 2));
  }

  /**
   * clean previous std error and outs
   */

  private void initStdOut(JobConf configuration) {
    TaskAttemptID taskId = TaskAttemptID.forName(configuration
            .get(MRJobConfig.TASK_ATTEMPT_ID));
    File stdOut = TaskLog.getTaskLogFile(taskId, false, TaskLog.LogName.STDOUT);
    File stdErr = TaskLog.getTaskLogFile(taskId, false, TaskLog.LogName.STDERR);
    // prepare folder
    if (!stdOut.getParentFile().exists()) {
      stdOut.getParentFile().mkdirs();
    } else { // clean logs
      stdOut.deleteOnExit();
      stdErr.deleteOnExit();
    }
  }

  private String readStdOut(JobConf conf) throws Exception {
    TaskAttemptID taskId = TaskAttemptID.forName(conf
            .get(MRJobConfig.TASK_ATTEMPT_ID));
    File stdOut = TaskLog.getTaskLogFile(taskId, false, TaskLog.LogName.STDOUT);

    return readFile(stdOut);

  }

  private String readFile(File file) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    InputStream is = new FileInputStream(file);
    byte[] buffer = new byte[1024];
    int counter = 0;
    while ((counter = is.read(buffer)) >= 0) {
      out.write(buffer, 0, counter);
    }

    is.close();

    return out.toString();

  }

  private class Progress implements Progressable {

    @Override
    public void progress() {

    }

  }

  private File[] cleanTokenPasswordFile(JobConf conf) throws Exception {
    File[] result = new File[2];
    result[0] = new File(conf.get(MRConfig.LOCAL_DIR) + Path.SEPARATOR
        + "jobTokenPassword");
    if (result[0].exists()) {
      FileUtil.chmod(result[0].getAbsolutePath(), "700");
      assertTrue(result[0].delete());
    }
    result[1] = new File(conf.get(MRConfig.LOCAL_DIR) + Path.SEPARATOR
        + ".jobTokenPassword.crc");
    if (result[1].exists()) {
      FileUtil.chmod(result[1].getAbsolutePath(), "700");
      result[1].delete();
    }
    return result;
  }

  private File getFileCommand(String clazz) throws Exception {
    String classpath = System.getProperty("java.class.path");
    File fCommand = new File(workSpace + File.separator + "cache.sh");
    fCommand.deleteOnExit();
    if (!fCommand.getParentFile().exists()) {
      fCommand.getParentFile().mkdirs();
    }
    fCommand.createNewFile();
    OutputStream os = new FileOutputStream(fCommand);
    os.write("#!/bin/sh \n".getBytes());
    if (clazz == null) {
      os.write(("ls ").getBytes());
    } else {
      os.write(("java -cp " + classpath + " " + clazz).getBytes());
    }
    os.flush();
    os.close();
    FileUtil.chmod(fCommand.getAbsolutePath(), "700");
    return fCommand;
  }

  private class CombineOutputCollector<K, V extends Object> implements
          OutputCollector<K, V> {
    private Writer<K, V> writer;
    private Counters.Counter outCounter;
    private Progressable progressable;

    public CombineOutputCollector(Counters.Counter outCounter,
                                  Progressable progressable) {
      this.outCounter = outCounter;
      this.progressable = progressable;
    }

    public synchronized void setWriter(Writer<K, V> writer) {
      this.writer = writer;
    }

    public synchronized void collect(K key, V value) throws IOException {
      outCounter.increment(1);
      writer.append(key, value);
      progressable.progress();
    }
  }

  public static class FakeSplit implements InputSplit {
    public void write(DataOutput out) throws IOException {
    }

    public void readFields(DataInput in) throws IOException {
    }

    public long getLength() {
      return 0L;
    }

    public String[] getLocations() {
      return new String[0];
    }
  }

  private class TestTaskReporter implements Reporter {
    private int recordNum = 0; // number of records processed
    private String status = null;
    private Counters counters = new Counters();
    private InputSplit split = new FakeSplit();

    @Override
    public void progress() {

      recordNum++;
    }

    @Override
    public void setStatus(String status) {
      this.status = status;

    }

    public String getStatus() {
      return this.status;

    }

    public Counters.Counter getCounter(String group, String name) {
      Counters.Counter counter = null;
      if (counters != null) {
        counter = counters.findCounter(group, name);
        if (counter == null) {
          Group grp = counters.addGroup(group, group);
          counter = grp.addCounter(name, name, 10);
        }
      }
      return counter;
    }

    public Counters.Counter getCounter(Enum<?> name) {
      return counters == null ? null : counters.findCounter(name);
    }

    public void incrCounter(Enum<?> key, long amount) {
      if (counters != null) {
        counters.incrCounter(key, amount);
      }
    }

    public void incrCounter(String group, String counter, long amount) {

      if (counters != null) {
        counters.incrCounter(group, counter, amount);
      }

    }

    @Override
    public InputSplit getInputSplit() throws UnsupportedOperationException {
      return split;
    }

    @Override
    public float getProgress() {
      return recordNum;
    }

  }

  private class Reader implements RecordReader<FloatWritable, NullWritable> {
    private int index = 0;
    private FloatWritable progress;

    @Override
    public boolean next(FloatWritable key, NullWritable value)
            throws IOException {
      progress = key;
      index++;
      return index <= 10;
    }

    @Override
    public float getProgress() throws IOException {
      return progress.get();
    }

    @Override
    public long getPos() throws IOException {

      return index;
    }

    @Override
    public NullWritable createValue() {

      return NullWritable.get();
    }

    @Override
    public FloatWritable createKey() {
      FloatWritable result = new FloatWritable(index);
      return result;
    }

    @Override
    public void close() throws IOException {

    }
  }


  private class ReaderPipesMapRunner implements RecordReader<FloatWritable, NullWritable> {
    private int index = 0;

    @Override
    public boolean next(FloatWritable key, NullWritable value)
            throws IOException {
      key.set(index++);
      return index <= 10;
    }

    @Override
    public float getProgress() throws IOException {
      return index;
    }

    @Override
    public long getPos() throws IOException {

      return index;
    }

    @Override
    public NullWritable createValue() {

      return NullWritable.get();
    }

    @Override
    public FloatWritable createKey() {
      FloatWritable result = new FloatWritable(index);
      return result;
    }

    @Override
    public void close() throws IOException {

    }
  }

  private class FakeCollector extends
          CombineOutputCollector<IntWritable, Text> {

    final private Map<IntWritable, Text> collect = new HashMap<IntWritable, Text>();

    public FakeCollector(Counter outCounter, Progressable progressable) {
      super(outCounter, progressable);
    }

    @Override
    public synchronized void collect(IntWritable key, Text value)
            throws IOException {
      collect.put(key, value);
      super.collect(key, value);
    }

    public Map<IntWritable, Text> getCollect() {
      return collect;
    }
  }
}
