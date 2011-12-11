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

package org.apache.hadoop.tools.rumen;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.TestNoJobSetupCleanup.MyOutputFormat;
import org.apache.hadoop.mapreduce.jobhistory.HistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobHistory;
import org.apache.hadoop.mapreduce.jobhistory.TaskAttemptFinishedEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskAttemptUnsuccessfulCompletionEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskStartedEvent;
import org.apache.hadoop.mapreduce.server.tasktracker.TTConfig;
import org.apache.hadoop.tools.rumen.TraceBuilder.MyOptions;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestRumenJobTraces {
  @Test
  public void testSmallTrace() throws Exception {
    performSingleTest("sample-job-tracker-logs.gz",
        "job-tracker-logs-topology-output", "job-tracker-logs-trace-output.gz");
  }

  @Test
  public void testTruncatedTask() throws Exception {
    performSingleTest("truncated-job-tracker-log", "truncated-topology-output",
        "truncated-trace-output");
  }

  private void performSingleTest(String jtLogName, String goldTopology,
      String goldTrace) throws Exception {
    final Configuration conf = new Configuration();
    final FileSystem lfs = FileSystem.getLocal(conf);

    final Path rootInputDir =
        new Path(System.getProperty("test.tools.input.dir", "")).makeQualified(
            lfs.getUri(), lfs.getWorkingDirectory());
    final Path rootTempDir =
        new Path(System.getProperty("test.build.data", "/tmp")).makeQualified(
            lfs.getUri(), lfs.getWorkingDirectory());

    final Path rootInputFile = new Path(rootInputDir, "rumen/small-trace-test");
    final Path tempDir = new Path(rootTempDir, "TestRumenJobTraces");
    lfs.delete(tempDir, true);

    final Path topologyFile = new Path(tempDir, jtLogName + "-topology.json");
    final Path traceFile = new Path(tempDir, jtLogName + "-trace.json");

    final Path inputFile = new Path(rootInputFile, jtLogName);

    System.out.println("topology result file = " + topologyFile);
    System.out.println("trace result file = " + traceFile);

    String[] args = new String[6];

    args[0] = "-v1";

    args[1] = "-write-topology";
    args[2] = topologyFile.toString();

    args[3] = "-write-job-trace";
    args[4] = traceFile.toString();

    args[5] = inputFile.toString();

    final Path topologyGoldFile = new Path(rootInputFile, goldTopology);
    final Path traceGoldFile = new Path(rootInputFile, goldTrace);

    @SuppressWarnings("deprecation")
    HadoopLogsAnalyzer analyzer = new HadoopLogsAnalyzer();
    int result = ToolRunner.run(analyzer, args);
    assertEquals("Non-zero exit", 0, result);

    TestRumenJobTraces
        .<LoggedNetworkTopology> jsonFileMatchesGold(conf, topologyFile,
            topologyGoldFile, LoggedNetworkTopology.class, "topology");
    TestRumenJobTraces.<LoggedJob> jsonFileMatchesGold(conf, traceFile,
        traceGoldFile, LoggedJob.class, "trace");
  }

  @Test
  public void testRumenViaDispatch() throws Exception {
    final Configuration conf = new Configuration();
    final FileSystem lfs = FileSystem.getLocal(conf);

    final Path rootInputDir =
        new Path(System.getProperty("test.tools.input.dir", "")).makeQualified(
            lfs.getUri(), lfs.getWorkingDirectory());
    final Path rootTempDir =
        new Path(System.getProperty("test.build.data", "/tmp")).makeQualified(
            lfs.getUri(), lfs.getWorkingDirectory());

    final Path rootInputPath = new Path(rootInputDir, "rumen/small-trace-test");
    final Path tempDir = new Path(rootTempDir, "TestRumenViaDispatch");
    lfs.delete(tempDir, true);

    final Path topologyPath = new Path(tempDir, "dispatch-topology.json");
    final Path tracePath = new Path(tempDir, "dispatch-trace.json");

    final Path inputPath =
        new Path(rootInputPath, "dispatch-sample-v20-jt-log.gz");

    System.out.println("topology result file = " + topologyPath);
    System.out.println("testRumenViaDispatch() trace result file = " + tracePath);

    String demuxerClassName = ConcatenatedInputFilesDemuxer.class.getName();

    String[] args =
        { "-demuxer", demuxerClassName, tracePath.toString(),
            topologyPath.toString(), inputPath.toString() };

    final Path topologyGoldFile =
        new Path(rootInputPath, "dispatch-topology-output.json.gz");
    final Path traceGoldFile =
        new Path(rootInputPath, "dispatch-trace-output.json.gz");

    Tool analyzer = new TraceBuilder();
    int result = ToolRunner.run(analyzer, args);
    assertEquals("Non-zero exit", 0, result);

    TestRumenJobTraces
        .<LoggedNetworkTopology> jsonFileMatchesGold(conf, topologyPath,
            topologyGoldFile, LoggedNetworkTopology.class, "topology");
    TestRumenJobTraces.<LoggedJob> jsonFileMatchesGold(conf, tracePath,
        traceGoldFile, LoggedJob.class, "trace");
  }

  @Test
  public void testBracketedCounters() throws Exception {
    final Configuration conf = new Configuration();
    final FileSystem lfs = FileSystem.getLocal(conf);

    final Path rootInputDir =
        new Path(System.getProperty("test.tools.input.dir", "")).makeQualified(
            lfs.getUri(), lfs.getWorkingDirectory());
    final Path rootTempDir =
        new Path(System.getProperty("test.build.data", "/tmp")).makeQualified(
            lfs.getUri(), lfs.getWorkingDirectory());

    final Path rootInputPath = new Path(rootInputDir, "rumen/small-trace-test");
    final Path tempDir = new Path(rootTempDir, "TestBracketedCounters");
    lfs.delete(tempDir, true);

    final Path topologyPath = new Path(tempDir, "dispatch-topology.json");
    final Path tracePath = new Path(tempDir, "dispatch-trace.json");

    final Path inputPath = new Path(rootInputPath, "counters-format-test-logs");

    System.out.println("topology result file = " + topologyPath);
    System.out.println("testBracketedCounters() trace result file = " + tracePath);

    final Path goldPath =
        new Path(rootInputPath, "counters-test-trace.json.gz");

    String[] args =
        { tracePath.toString(), topologyPath.toString(), inputPath.toString() };

    Tool analyzer = new TraceBuilder();
    int result = ToolRunner.run(analyzer, args);
    assertEquals("Non-zero exit", 0, result);

    TestRumenJobTraces.<LoggedJob> jsonFileMatchesGold(conf, tracePath,
        goldPath, LoggedJob.class, "trace");
  }

  @Test
  public void testHadoop20JHParser() throws Exception {
    final Configuration conf = new Configuration();
    final FileSystem lfs = FileSystem.getLocal(conf);

    final Path rootInputDir =
        new Path(System.getProperty("test.tools.input.dir", "")).makeQualified(
            lfs.getUri(), lfs.getWorkingDirectory());

    final Path rootInputPath = new Path(rootInputDir, "rumen/small-trace-test");

    // history file to be parsed to get events
    final Path inputPath = new Path(rootInputPath, "v20-single-input-log.gz");

    RewindableInputStream ris = getRewindableInputStream(inputPath, conf);
    assertNotNull(ris);

    Hadoop20JHParser parser = null;

    try {
      assertEquals("Hadoop20JHParser can't parse the test file " +
          inputPath, true, Hadoop20JHParser.canParse(ris));

      ris.rewind();
      parser = new Hadoop20JHParser(ris);
      ArrayList<String> seenEvents = new ArrayList<String>(150);

      getHistoryEvents(parser, seenEvents); // get events into seenEvents

      // Validate the events seen by history parser from
      // history file v20-single-input-log.gz
      validateSeenHistoryEvents(seenEvents, goldLines);
    } finally {
      if (parser != null) {
        parser.close();
      }
      ris.close();
    }
  }

  /**
   * Tests if {@link TraceBuilder} can correctly identify and parse jobhistory
   * filenames. The testcase checks if {@link TraceBuilder}
   *   - correctly identifies a jobhistory filename without suffix
   *   - correctly parses a jobhistory filename without suffix to extract out 
   *     the jobid
   *   - correctly identifies a jobhistory filename with suffix
   *   - correctly parses a jobhistory filename with suffix to extract out the 
   *     jobid
   *   - correctly identifies a job-configuration filename stored along with the 
   *     jobhistory files
   */
  @Test
  public void testJobHistoryFilenameParsing() throws IOException {
    final Configuration conf = new Configuration();
    final FileSystem lfs = FileSystem.getLocal(conf);
    String user = "test";
    org.apache.hadoop.mapred.JobID jid = 
      new org.apache.hadoop.mapred.JobID("12345", 1);
    final Path rootInputDir =
      new Path(System.getProperty("test.tools.input.dir", ""))
            .makeQualified(lfs.getUri(), lfs.getWorkingDirectory());
    
    // Check if jobhistory filename are detected properly
    Path jhFilename = JobHistory.getJobHistoryFile(rootInputDir, jid, user);
    JobID extractedJID = 
      JobID.forName(TraceBuilder.extractJobID(jhFilename.getName()));
    assertEquals("TraceBuilder failed to parse the current JH filename", 
                 jid, extractedJID);
    // test jobhistory filename with old/stale file suffix
    jhFilename = jhFilename.suffix(JobHistory.getOldFileSuffix("123"));
    extractedJID =
      JobID.forName(TraceBuilder.extractJobID(jhFilename.getName()));
    assertEquals("TraceBuilder failed to parse the current JH filename"
                 + "(old-suffix)", 
                 jid, extractedJID);
    
    // Check if the conf filename in jobhistory are detected properly
    Path jhConfFilename = JobHistory.getConfFile(rootInputDir, jid);
    assertTrue("TraceBuilder failed to parse the current JH conf filename", 
               TraceBuilder.isJobConfXml(jhConfFilename.getName(), null));
    // test jobhistory conf filename with old/stale file suffix
    jhConfFilename = jhConfFilename.suffix(JobHistory.getOldFileSuffix("123"));
    assertTrue("TraceBuilder failed to parse the current JH conf filename" 
               + " (old suffix)", 
               TraceBuilder.isJobConfXml(jhConfFilename.getName(), null));
  }

  /**
   * Test if {@link TraceBuilder} can process globbed input file paths.
   */
  @Test
  public void testGlobbedInput() throws Exception {
    final Configuration conf = new Configuration();
    final FileSystem lfs = FileSystem.getLocal(conf);
    
    // define the test's root temporary directory
    final Path rootTempDir =
      new Path(System.getProperty("test.build.data", "/tmp"))
            .makeQualified(lfs.getUri(), lfs.getWorkingDirectory());
    // define the test's root input directory
    Path testRootInputDir = new Path(rootTempDir, "TestGlobbedInputPath");
    // define the nested input directory
    Path nestedInputDir = new Path(testRootInputDir, "1/2/3/4");
    // define the globbed version of the nested input directory
    Path globbedInputNestedDir = 
      lfs.makeQualified(new Path(testRootInputDir, "*/*/*/*/*"));
    
    // define a file in the nested test input directory
    Path inputPath1 = new Path(nestedInputDir, "test.txt");
    // define a sub-folder in the nested test input directory
    Path inputPath2Parent = new Path(nestedInputDir, "test");
    lfs.mkdirs(inputPath2Parent);
    // define a file in the sub-folder within the nested test input directory
    Path inputPath2 = new Path(inputPath2Parent, "test.txt");
    
    // create empty input files
    lfs.createNewFile(inputPath1);
    lfs.createNewFile(inputPath2);
    
    // define the output trace and topology files
    Path outputTracePath = new Path(testRootInputDir, "test.json");
    Path outputTopologyTracePath = new Path(testRootInputDir, "topology.json");
    
    String[] args = 
      new String[] {outputTracePath.toString(), 
                    outputTopologyTracePath.toString(), 
                    globbedInputNestedDir.toString() };
    
    // invoke TraceBuilder's MyOptions command options parsing module/utility
    MyOptions options = new TraceBuilder.MyOptions(args, conf);
    
    lfs.delete(testRootInputDir, true);
    
    assertEquals("Error in detecting globbed input FileSystem paths", 
                 2, options.inputs.size());
    
    assertTrue("Missing input file " + inputPath1, 
               options.inputs.contains(inputPath1));
    assertTrue("Missing input file " + inputPath2, 
               options.inputs.contains(inputPath2));
  }
  
  /**
   * Test if {@link CurrentJHParser} can read events from current JH files.
   */
  @Test
  public void testCurrentJHParser() throws Exception {
    final Configuration conf = new Configuration();
    final FileSystem lfs = FileSystem.getLocal(conf);

    final Path rootTempDir =
      new Path(System.getProperty("test.build.data", "/tmp")).makeQualified(
          lfs.getUri(), lfs.getWorkingDirectory());

    final Path tempDir = new Path(rootTempDir, "TestCurrentJHParser");
    lfs.delete(tempDir, true);
    
    // Run a MR job
    // create a MR cluster
    conf.setInt(TTConfig.TT_MAP_SLOTS, 1);
    conf.setInt(TTConfig.TT_REDUCE_SLOTS, 1);
    MiniMRCluster mrCluster = new MiniMRCluster(1, "file:///", 1, null, null, 
                                                new JobConf(conf));
    
    // run a job
    Path inDir = new Path(tempDir, "input");
    Path outDir = new Path(tempDir, "output");
    JobHistoryParser parser = null;
    RewindableInputStream ris = null;
    ArrayList<String> seenEvents = new ArrayList<String>(15);
    
    try {
      JobConf jConf = mrCluster.createJobConf();
      // construct a job with 1 map and 1 reduce task.
      Job job = MapReduceTestUtil.createJob(jConf, inDir, outDir, 1, 1);
      // disable setup/cleanup
      job.setJobSetupCleanupNeeded(false);
      // set the output format to take care of the _temporary folder
      job.setOutputFormatClass(MyOutputFormat.class);
      // wait for the job to complete
      job.waitForCompletion(false);
      
      assertTrue("Job failed", job.isSuccessful());

      JobID id = job.getJobID();
      JobClient jc = new JobClient(jConf);
      String user = jc.getAllJobs()[0].getUsername();

      // get the jobhistory filepath
      Path jhPath = 
        new Path(mrCluster.getJobTrackerRunner().getJobTracker()
                          .getJobHistoryDir());
      Path inputPath = JobHistory.getJobHistoryFile(jhPath, id, user);
      // wait for 10 secs for the jobhistory file to move into the done folder
      for (int i = 0; i < 100; ++i) {
        if (lfs.exists(inputPath)) {
          break;
        }
        TimeUnit.MILLISECONDS.wait(100);
      }
    
      assertTrue("Missing job history file", lfs.exists(inputPath));

      ris = getRewindableInputStream(inputPath, conf);

      // Test if the JobHistoryParserFactory can detect the parser correctly
      parser = JobHistoryParserFactory.getParser(ris);

      getHistoryEvents(parser, seenEvents); // get events into seenEvents

      // Check against the gold standard
      System.out.println("testCurrentJHParser validating using gold std ");
      // The list of history events expected when parsing the above job's
      // history log file
      String[] goldLinesExpected = new String[] {
          JSE, JPCE, JIE, JSCE, TSE, ASE, MFE, TFE, TSE, ASE, RFE, TFE, JFE
      };

      validateSeenHistoryEvents(seenEvents, goldLinesExpected);
    } finally {
      // stop the MR cluster
      mrCluster.shutdown();
      
      if (ris != null) {
          ris.close();
      }
      if (parser != null) {
        parser.close();
      }
      
      // cleanup the filesystem
      lfs.delete(tempDir, true);
    }
  }

  @Test
  public void testJobConfigurationParser() throws Exception {

    // Validate parser with old mapred config properties from
    // sample-conf-file.xml
    String[] oldProps1 = { "mapred.job.queue.name", "mapred.job.name",
        "mapred.child.java.opts" };

    String[] oldProps2 = { "mapred.job.queue.name", "mapred.child.java.opts" };

    validateJobConfParser(oldProps1, oldProps2, "sample-conf.file.xml", false);

    // Validate parser with new mapreduce config properties from
    // sample-conf-file.new.xml
    String[] newProps1 = { MRJobConfig.QUEUE_NAME, MRJobConfig.JOB_NAME,
        MRJobConfig.MAP_JAVA_OPTS, MRJobConfig.REDUCE_JAVA_OPTS };

    String[] newProps2 = { MRJobConfig.QUEUE_NAME,
        MRJobConfig.MAP_JAVA_OPTS, MRJobConfig.REDUCE_JAVA_OPTS };

    validateJobConfParser(newProps1, newProps2,
        "sample-conf.file.new.xml", true);
  }

  private void validateJobConfParser(String[] list1, String[] list2,
      String confFile, boolean newConfig)
      throws Exception {
    List<String> interested1 = new ArrayList<String>();
    for (String interested : list1) {
      interested1.add(interested);
    }

    List<String> interested2 = new ArrayList<String>();
    for (String interested : list2) {
      interested2.add(interested);
    }

    JobConfigurationParser jcp1 = new JobConfigurationParser(interested1);
    JobConfigurationParser jcp2 = new JobConfigurationParser(interested2);

    final Configuration conf = new Configuration();
    final FileSystem lfs = FileSystem.getLocal(conf);

    @SuppressWarnings("deprecation")
    final Path rootInputDir =
        new Path(System.getProperty("test.tools.input.dir", ""))
            .makeQualified(lfs);

    final Path rootInputPath = new Path(rootInputDir, "rumen/small-trace-test");

    final Path inputPath = new Path(rootInputPath, confFile);

    InputStream inputConfStream =
        new PossiblyDecompressedInputStream(inputPath, conf);

    try {
      Properties props1 = jcp1.parse(inputConfStream);
      inputConfStream.close();

      inputConfStream = new PossiblyDecompressedInputStream(inputPath, conf);
      Properties props2 = jcp2.parse(inputConfStream);

      assertEquals("testJobConfigurationParser: wrong number of properties",
          list1.length, props1.size());
      assertEquals("testJobConfigurationParser: wrong number of properties",
          list2.length, props2.size());

      // Make sure that parser puts the interested properties into props1 and
      // props2 as defined by list1 and list2.
      String oldOrNew = newConfig ? "New" : "Old";
      assertEquals(oldOrNew + " config property for job queue name is not "
          + " extracted properly.", "TheQueue",
          JobBuilder.extract(props1, JobConfPropertyNames.QUEUE_NAMES
          .getCandidates(), null));
      assertEquals(oldOrNew + " config property for job name is not "
          + " extracted properly.", "MyMRJob",
          JobBuilder.extract(props1, JobConfPropertyNames.JOB_NAMES
          .getCandidates(), null));

      assertEquals(oldOrNew + " config property for job queue name is not "
          + " extracted properly.", "TheQueue",
          JobBuilder.extract(props2, JobConfPropertyNames.QUEUE_NAMES
          .getCandidates(), null));
      
      // This config property is not interested for props2. So props should not
      // contain this.
      assertNull("Uninterested " + oldOrNew + " config property for job name "
          + " is extracted.",
          JobBuilder.extract(props2, JobConfPropertyNames.JOB_NAMES
          .getCandidates(), null));

      validateChildJavaOpts(newConfig, props1);
      validateChildJavaOpts(newConfig, props2);

    } finally {
      inputConfStream.close();
    }
  }

  // Validate child java opts in properties.
  // newConfigProperties: boolean that specifies if the config properties to be
  // validated are new OR old.
  private void validateChildJavaOpts(boolean newConfigProperties,
      Properties props) {
    if (newConfigProperties) {
      assertEquals("New config property " + MRJobConfig.MAP_JAVA_OPTS
          + " is not extracted properly.",
          "-server -Xmx640m -Djava.net.preferIPv4Stack=true",
          JobBuilder.extract(props, JobConfPropertyNames.MAP_JAVA_OPTS_S
          .getCandidates(), null));
      assertEquals("New config property " + MRJobConfig.REDUCE_JAVA_OPTS
          + " is not extracted properly.",
          "-server -Xmx650m -Djava.net.preferIPv4Stack=true",
          JobBuilder.extract(props, JobConfPropertyNames.REDUCE_JAVA_OPTS_S
          .getCandidates(), null));
    }
    else {
      // if old property mapred.child.java.opts is set, then extraction of all
      // the following 3 properties should give that value.
      assertEquals("mapred.child.java.opts is not extracted properly.",
          "-server -Xmx640m -Djava.net.preferIPv4Stack=true",
          JobBuilder.extract(props, JobConfPropertyNames.TASK_JAVA_OPTS_S
          .getCandidates(), null));
      assertEquals("New config property " + MRJobConfig.MAP_JAVA_OPTS
          + " is not extracted properly when the old config property "
          + "mapred.child.java.opts is set.",
          "-server -Xmx640m -Djava.net.preferIPv4Stack=true",
          JobBuilder.extract(props, JobConfPropertyNames.MAP_JAVA_OPTS_S
          .getCandidates(), null));
      assertEquals("New config property " + MRJobConfig.REDUCE_JAVA_OPTS
              + " is not extracted properly when the old config property "
              + "mapred.child.java.opts is set.",
          "-server -Xmx640m -Djava.net.preferIPv4Stack=true",
          JobBuilder.extract(props, JobConfPropertyNames.REDUCE_JAVA_OPTS_S
          .getCandidates(), null));
    }
  }

  @Test
  public void testTopologyBuilder() throws Exception {
    final TopologyBuilder subject = new TopologyBuilder();

    // currently we extract no host names from the Properties
    subject.process(new Properties());

    subject.process(new TaskAttemptFinishedEvent(TaskAttemptID
        .forName("attempt_200904211745_0003_m_000004_0"), TaskType
        .valueOf("MAP"), "STATUS", 1234567890L,
        "/194\\.6\\.134\\.64/cluster50261\\.secondleveldomain\\.com",
        "SUCCESS", null));
    subject.process(new TaskAttemptUnsuccessfulCompletionEvent(TaskAttemptID
        .forName("attempt_200904211745_0003_m_000004_1"), TaskType
        .valueOf("MAP"), "STATUS", 1234567890L,
        "/194\\.6\\.134\\.80/cluster50262\\.secondleveldomain\\.com",
        "MACHINE_EXPLODED"));
    subject.process(new TaskAttemptUnsuccessfulCompletionEvent(TaskAttemptID
        .forName("attempt_200904211745_0003_m_000004_2"), TaskType
        .valueOf("MAP"), "STATUS", 1234567890L,
        "/194\\.6\\.134\\.80/cluster50263\\.secondleveldomain\\.com",
        "MACHINE_EXPLODED"));
    subject.process(new TaskStartedEvent(TaskID
        .forName("task_200904211745_0003_m_000004"), 1234567890L, TaskType
        .valueOf("MAP"),
        "/194\\.6\\.134\\.80/cluster50263\\.secondleveldomain\\.com"));

    final LoggedNetworkTopology topology = subject.build();

    List<LoggedNetworkTopology> racks = topology.getChildren();

    assertEquals("Wrong number of racks", 2, racks.size());

    boolean sawSingleton = false;
    boolean sawDoubleton = false;

    for (LoggedNetworkTopology rack : racks) {
      List<LoggedNetworkTopology> nodes = rack.getChildren();
      if (rack.getName().endsWith(".64")) {
        assertEquals("The singleton rack has the wrong number of elements", 1,
            nodes.size());
        sawSingleton = true;
      } else if (rack.getName().endsWith(".80")) {
        assertEquals("The doubleton rack has the wrong number of elements", 2,
            nodes.size());
        sawDoubleton = true;
      } else {
        assertTrue("Unrecognized rack name", false);
      }
    }

    assertTrue("Did not see singleton rack", sawSingleton);
    assertTrue("Did not see doubleton rack", sawDoubleton);
  }

  static private <T extends DeepCompare> void jsonFileMatchesGold(
      Configuration conf, Path result, Path gold, Class<? extends T> clazz,
      String fileDescription) throws IOException {
    JsonObjectMapperParser<T> goldParser =
        new JsonObjectMapperParser<T>(gold, clazz, conf);
    JsonObjectMapperParser<T> resultParser =
        new JsonObjectMapperParser<T>(result, clazz, conf);
    try {
      while (true) {
        DeepCompare goldJob = goldParser.getNext();
        DeepCompare resultJob = resultParser.getNext();
        if ((goldJob == null) || (resultJob == null)) {
          assertTrue(goldJob == resultJob);
          break;
        }

        try {
          resultJob.deepCompare(goldJob, new TreePath(null, "<root>"));
        } catch (DeepInequalityException e) {
          String error = e.path.toString();

          assertFalse(fileDescription + " mismatches: " + error, true);
        }
      }
    } finally {
      IOUtils.cleanup(null, goldParser, resultParser);
    }
  }

  /**
   * Creates {@link RewindableInputStream} for the given file path.
   * @param inputPath the input file path
   * @param conf configuration
   * @return {@link RewindableInputStream}
   * @throws IOException
   */
  private RewindableInputStream getRewindableInputStream(Path inputPath,
      Configuration conf) throws IOException {

    PossiblyDecompressedInputStream in =
        new PossiblyDecompressedInputStream(inputPath, conf);

    return new RewindableInputStream(in, BUFSIZE);
  }

  /**
   * Allows given history parser to parse the history events and places in
   * the given list
   * @param parser the job history parser
   * @param events the job history events seen while parsing
   * @throws IOException
   */
  private void getHistoryEvents(JobHistoryParser parser,
      ArrayList<String> events) throws IOException {
    HistoryEvent e;
    while ((e = parser.nextEvent()) != null) {
      String eventString = e.getClass().getSimpleName();
      System.out.println(eventString);
      events.add(eventString);
    }
  }

  /**
   * Validate if history events seen are as expected
   * @param seenEvents the list of history events seen
   * @param goldLinesExpected  the expected history events
   */
  private void validateSeenHistoryEvents(ArrayList<String> seenEvents,
      String[] goldLinesExpected) {

    // Check the output with gold std
    assertEquals("Number of events expected is different from the events seen"
        + " by the history parser.",
        goldLinesExpected.length, seenEvents.size());

    int index = 0;
    for (String goldLine : goldLinesExpected) {
      assertEquals("History Event mismatch at line " + (index + 1),
          goldLine, seenEvents.get(index));
      index++;
    }
  }

  final static int BUFSIZE = 8192; // 8K

  // Any Map Reduce Job History Event should be 1 of the following 16
  final static String JSE = "JobSubmittedEvent";
  final static String JPCE = "JobPriorityChangeEvent";
  final static String JSCE = "JobStatusChangedEvent";
  final static String JIE = "JobInitedEvent";
  final static String JICE = "JobInfoChangeEvent";
  static String TSE = "TaskStartedEvent";
  static String ASE = "TaskAttemptStartedEvent";
  static String AFE = "TaskAttemptFinishedEvent";
  static String MFE = "MapAttemptFinishedEvent";
  static String TUE = "TaskUpdatedEvent";
  static String TFE = "TaskFinishedEvent";
  static String JUCE = "JobUnsuccessfulCompletionEvent";
  static String RFE = "ReduceAttemptFinishedEvent";
  static String AUCE = "TaskAttemptUnsuccessfulCompletionEvent";
  static String TFLE = "TaskFailedEvent";
  static String JFE = "JobFinishedEvent";

  // The expected job history events(in order) when parsing
  // the job history file v20-single-input-log.gz
  final static String[] goldLines = new String[] {
      JSE, JPCE, JSCE, JIE, JICE, TSE, ASE, AFE, MFE, TUE, TFE, JSCE, TSE,
      TSE, TSE, TSE, TSE, TSE, TSE, TSE, TSE, TSE, TSE, TSE, TSE, TSE, TSE,
      TSE, TSE, TSE, TSE, TSE, ASE, AFE, MFE, TUE, TFE, ASE, AFE, MFE, TUE,
      TFE, ASE, AFE, MFE, TUE, TFE, TSE, ASE, AFE, MFE, TUE, TFE, ASE, AFE,
      MFE, TUE, TFE, ASE, AFE, MFE, TUE, TFE, ASE, AFE, MFE, TUE, TFE, ASE,
      AFE, MFE, TUE, TFE, ASE, AFE, MFE, TUE, TFE, ASE, AFE, MFE, TUE, TFE,
      ASE, AFE, MFE, TUE, TFE, ASE, AFE, MFE, TUE, TFE, ASE, AUCE, ASE, AFE,
      MFE, TUE, TFE, ASE, AFE, MFE, TUE, TFE, ASE, AFE, MFE, TUE, TFE, ASE,
      AFE, MFE, TUE, TFE, ASE, AFE, MFE, TUE, TFE, ASE, AFE, MFE, TUE, TFE,
      ASE, AFE, MFE, TUE, TFE, ASE, AFE, MFE, TUE, TFE, ASE, AFE, RFE, TUE,
      TFE, TSE, ASE, AFE, MFE, TUE, TFE, JSCE, JFE
  };

}
