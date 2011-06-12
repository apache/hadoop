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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.jobhistory.HistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskAttemptFinishedEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskAttemptUnsuccessfulCompletionEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskStartedEvent;
import org.apache.hadoop.util.LineReader;
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

    boolean success = false;

    final Path rootInputDir =
        new Path(System.getProperty("test.tools.input.dir", "")).makeQualified(
            lfs.getUri(), lfs.getWorkingDirectory());
    final Path rootTempDir =
        new Path(System.getProperty("test.build.data", "/tmp")).makeQualified(
            lfs.getUri(), lfs.getWorkingDirectory());

    final Path rootInputPath = new Path(rootInputDir, "rumen/small-trace-test");
    final Path tempDir = new Path(rootTempDir, "TestRumenViaDispatch");
    lfs.delete(tempDir, true);

    final Path inputPath = new Path(rootInputPath, "v20-single-input-log.gz");
    final Path goldPath =
        new Path(rootInputPath, "v20-single-input-log-event-classes.text.gz");

    InputStream inputLogStream =
        new PossiblyDecompressedInputStream(inputPath, conf);

    InputStream inputGoldStream =
        new PossiblyDecompressedInputStream(goldPath, conf);

    BufferedInputStream bis = new BufferedInputStream(inputLogStream);
    bis.mark(10000);
    Hadoop20JHParser parser = new Hadoop20JHParser(bis);

    final Path resultPath = new Path(tempDir, "result.text");

    System.out.println("testHadoop20JHParser sent its output to " + resultPath);

    Compressor compressor;

    FileSystem fs = resultPath.getFileSystem(conf);
    CompressionCodec codec =
        new CompressionCodecFactory(conf).getCodec(resultPath);
    OutputStream output;
    if (codec != null) {
      compressor = CodecPool.getCompressor(codec);
      output = codec.createOutputStream(fs.create(resultPath), compressor);
    } else {
      output = fs.create(resultPath);
    }

    PrintStream printStream = new PrintStream(output);

    try {
      assertEquals("Hadoop20JHParser can't parse the test file", true,
          Hadoop20JHParser.canParse(inputLogStream));

      bis.reset();

      HistoryEvent event = parser.nextEvent();

      while (event != null) {
        printStream.println(event.getClass().getCanonicalName());
        event = parser.nextEvent();
      }

      printStream.close();

      LineReader goldLines = new LineReader(inputGoldStream);
      LineReader resultLines =
          new LineReader(new PossiblyDecompressedInputStream(resultPath, conf));

      int lineNumber = 1;

      try {
        Text goldLine = new Text();
        Text resultLine = new Text();

        int goldRead = goldLines.readLine(goldLine);
        int resultRead = resultLines.readLine(resultLine);

        while (goldRead * resultRead != 0) {
          if (!goldLine.equals(resultLine)) {
            assertEquals("Type mismatch detected", goldLine, resultLine);
            break;
          }

          goldRead = goldLines.readLine(goldLine);
          resultRead = resultLines.readLine(resultLine);

          ++lineNumber;
        }

        if (goldRead != resultRead) {
          assertEquals("the " + (goldRead > resultRead ? "gold" : resultRead)
              + " file contains more text at line " + lineNumber, goldRead,
              resultRead);
        }

        success = true;
      } finally {
        goldLines.close();
        resultLines.close();

        if (success) {
          lfs.delete(resultPath, false);
        }
      }

    } finally {
      if (parser == null) {
        inputLogStream.close();
      } else {
        if (parser != null) {
          parser.close();
        }
      }

      if (inputGoldStream != null) {
        inputGoldStream.close();
      }

      // it's okay to do this twice [if we get an error on input]
      printStream.close();
    }
  }

  @Test
  public void testJobConfigurationParser() throws Exception {
    String[] list1 =
        { "mapred.job.queue.name", "mapreduce.job.name",
            "mapred.child.java.opts" };

    String[] list2 = { "mapred.job.queue.name", "mapred.child.java.opts" };

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

    final Path inputPath = new Path(rootInputPath, "sample-conf.file.xml");

    InputStream inputConfStream =
        new PossiblyDecompressedInputStream(inputPath, conf);

    try {
      Properties props1 = jcp1.parse(inputConfStream);
      inputConfStream.close();

      inputConfStream = new PossiblyDecompressedInputStream(inputPath, conf);
      Properties props2 = jcp2.parse(inputConfStream);

      assertEquals("testJobConfigurationParser: wrong number of properties", 3,
          props1.size());
      assertEquals("testJobConfigurationParser: wrong number of properties", 2,
          props2.size());

      assertEquals("prop test 1", "TheQueue", props1
          .get("mapred.job.queue.name"));
      assertEquals("prop test 2", "job_0001", props1.get("mapreduce.job.name"));
      assertEquals("prop test 3",
          "-server -Xmx640m -Djava.net.preferIPv4Stack=true", props1
              .get("mapred.child.java.opts"));
      assertEquals("prop test 4", "TheQueue", props2
          .get("mapred.job.queue.name"));
      assertEquals("prop test 5",
          "-server -Xmx640m -Djava.net.preferIPv4Stack=true", props2
              .get("mapred.child.java.opts"));

    } finally {
      inputConfStream.close();
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
}
