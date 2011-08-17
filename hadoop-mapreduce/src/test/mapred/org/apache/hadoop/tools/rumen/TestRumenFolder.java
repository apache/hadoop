package org.apache.hadoop.tools.rumen;

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

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ToolRunner;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestRumenFolder {
  @Test
  public void testFoldingSmallTrace() throws Exception {
    final Configuration conf = new Configuration();
    final FileSystem lfs = FileSystem.getLocal(conf);

    @SuppressWarnings("deprecation")
    final Path rootInputDir =
        new Path(System.getProperty("test.tools.input.dir", ""))
            .makeQualified(lfs);
    @SuppressWarnings("deprecation")
    final Path rootTempDir =
        new Path(System.getProperty("test.build.data", "/tmp"))
            .makeQualified(lfs);

    final Path rootInputFile = new Path(rootInputDir, "rumen/small-trace-test");
    final Path tempDir = new Path(rootTempDir, "TestRumenJobTraces");
    lfs.delete(tempDir, true);

    final Path foldedTracePath = new Path(tempDir, "folded-trace.json");

    final Path inputFile =
        new Path(rootInputFile, "folder-input-trace.json.gz");

    System.out.println("folded trace result path = " + foldedTracePath);

    String[] args =
        { "-input-cycle", "100S", "-output-duration", "300S",
            "-skew-buffer-length", "1", "-seed", "100", "-concentration", "2",
            inputFile.toString(), foldedTracePath.toString() };

    final Path foldedGoldFile =
        new Path(rootInputFile, "goldFoldedTrace.json.gz");

    Folder folder = new Folder();
    int result = ToolRunner.run(folder, args);
    assertEquals("Non-zero exit", 0, result);

    TestRumenFolder.<LoggedJob> jsonFileMatchesGold(conf, lfs, foldedTracePath,
        foldedGoldFile, LoggedJob.class, "trace");
  }
  
  @Test
  public void testStartsAfterOption() throws Exception {
    final Configuration conf = new Configuration();
    final FileSystem lfs = FileSystem.getLocal(conf);

    @SuppressWarnings("deprecation")
    final Path rootInputDir =
      new Path(System.getProperty("test.tools.input.dir", ""))
               .makeQualified(lfs);
    @SuppressWarnings("deprecation")
    final Path rootTempDir =
      new Path(System.getProperty("test.build.data", "/tmp"))
               .makeQualified(lfs);

    final Path rootInputFile = new Path(rootInputDir, "rumen/small-trace-test");
    final Path tempDir = new Path(rootTempDir, "TestRumenJobTraces");
    lfs.delete(tempDir, true);

    final Path inputFile =
      new Path(rootInputFile, "goldFoldedTrace.json.gz");
    
    final Path foldedTracePath = new Path(tempDir, 
                                          "folded-skippedjob-trace.json");
    String[] args =
       { "-input-cycle", "300S", "-output-duration", "300S",
         "-starts-after", "30S", 
         inputFile.toString(), foldedTracePath.toString() };

    Folder folder = new Folder();
    int result = ToolRunner.run(folder, args);
    assertEquals("Non-zero exit", 0, result);

    TestRumenFolder.<LoggedJob> checkValidityAfterSkippingJobs(conf, lfs, foldedTracePath,
          inputFile, LoggedJob.class, "trace", 30000, 300000);
  }
  
  static private <T extends DeepCompare> void 
            checkValidityAfterSkippingJobs(Configuration conf, 
            FileSystem lfs, Path result, Path inputFile,
            Class<? extends T> clazz, String fileDescription, 
            long startsAfter, long duration) throws IOException {
    
    JsonObjectMapperParser<T> inputFileParser =
        new JsonObjectMapperParser<T>(inputFile, clazz, conf);
    InputStream resultStream = lfs.open(result);
    JsonObjectMapperParser<T> resultParser =
        new JsonObjectMapperParser<T>(resultStream, clazz);
    List<Long> gpSubmitTimes = new LinkedList<Long>(); 
    List<Long> rpSubmitTimes = new LinkedList<Long>();
    try {
      //Get submitTime of first job
      LoggedJob firstJob = (LoggedJob)inputFileParser.getNext();
      gpSubmitTimes.add(firstJob.getSubmitTime());
      long absoluteStartsAfterTime = firstJob.getSubmitTime() + startsAfter;
      
      //total duration
      long endTime = firstJob.getSubmitTime() + duration;
      
      //read original trace
      LoggedJob oriJob = null;
      while((oriJob = (LoggedJob)inputFileParser.getNext()) != null) {
      	gpSubmitTimes.add(oriJob.getSubmitTime());
      }
      
      //check if retained jobs have submittime > starts-after
      LoggedJob job = null;
      while((job = (LoggedJob) resultParser.getNext()) != null) {
        assertTrue("job's submit time in the output trace is less " +
                   "than the specified value of starts-after", 
                   (job.getSubmitTime() >= absoluteStartsAfterTime));
                   rpSubmitTimes.add(job.getSubmitTime());
      }
      
      List<Long> skippedJobs = new LinkedList<Long>();
      skippedJobs.addAll(gpSubmitTimes);
      skippedJobs.removeAll(rpSubmitTimes);
      
      //check if the skipped job submittime < starts-after
      for(Long submitTime : skippedJobs) {
        assertTrue("skipped job submit time " + submitTime + 
                   " in the trace is greater " +
                   "than the specified value of starts-after " 
                   + absoluteStartsAfterTime, 
                   (submitTime < absoluteStartsAfterTime));
      }
    } finally {
      IOUtils.cleanup(null, inputFileParser, resultParser);
    }
  }

  static private <T extends DeepCompare> void jsonFileMatchesGold(
      Configuration conf, FileSystem lfs, Path result, Path gold,
      Class<? extends T> clazz, String fileDescription) throws IOException {
    JsonObjectMapperParser<T> goldParser =
        new JsonObjectMapperParser<T>(gold, clazz, conf);
    InputStream resultStream = lfs.open(result);
    JsonObjectMapperParser<T> resultParser =
        new JsonObjectMapperParser<T>(resultStream, clazz);
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
