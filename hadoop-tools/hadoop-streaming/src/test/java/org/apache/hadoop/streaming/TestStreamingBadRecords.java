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

package org.apache.hadoop.streaming;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SkipBadRecords;
import org.apache.hadoop.mapred.Utils;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestStreamingBadRecords extends ClusterMapReduceTestCase
{

  private static final Logger LOG =
    LoggerFactory.getLogger(TestStreamingBadRecords.class);
  
  private static final List<String> MAPPER_BAD_RECORDS = 
    Arrays.asList("hey022","hey023","hey099");
  
  private static final List<String> REDUCER_BAD_RECORDS = 
    Arrays.asList("hey001","hey018");
  
  private static final String badMapper = 
    UtilTest.makeJavaCommand(BadApp.class, new String[]{});
  private static final String badReducer = 
    UtilTest.makeJavaCommand(BadApp.class, new String[]{"true"});
  private static final int INPUTSIZE=100;
  
  public TestStreamingBadRecords() throws IOException
  {
    UtilTest utilTest = new UtilTest(getClass().getName());
    utilTest.checkUserDir();
    utilTest.redirectIfAntJunit();
  }

  @Before
  public void setUp() throws Exception {
    Properties props = new Properties();
    props.setProperty(JTConfig.JT_RETIREJOBS, "false");
    props.setProperty(JTConfig.JT_PERSIST_JOBSTATUS, "false");
    startCluster(true, props);
  }

  private void createInput() throws Exception {
    OutputStream os = getFileSystem().create(new Path(getInputDir(), 
        "text.txt"));
    Writer wr = new OutputStreamWriter(os);
    //increasing the record size so that we have stream flushing
    String prefix = new String(new byte[20*1024]);
    for(int i=1;i<=INPUTSIZE;i++) {
      String str = ""+i;
      int zerosToPrepend = 3 - str.length();
      for(int j=0;j<zerosToPrepend;j++){
        str = "0"+str;
      }
      wr.write(prefix + "hey"+str+"\n");
    }wr.close();
  }
  
  private void validateOutput(RunningJob runningJob, boolean validateCount) 
    throws Exception {
    LOG.info(runningJob.getCounters().toString());
    assertTrue(runningJob.isSuccessful());
    
    if(validateCount) {
     //validate counters
      String counterGrp = "org.apache.hadoop.mapred.Task$Counter";
      Counters counters = runningJob.getCounters();
      assertEquals(counters.findCounter(counterGrp, "MAP_SKIPPED_RECORDS").
          getCounter(),MAPPER_BAD_RECORDS.size());
      
      int mapRecs = INPUTSIZE - MAPPER_BAD_RECORDS.size();
      assertEquals(counters.findCounter(counterGrp, "MAP_INPUT_RECORDS").
          getCounter(),mapRecs);
      assertEquals(counters.findCounter(counterGrp, "MAP_OUTPUT_RECORDS").
          getCounter(),mapRecs);
      
      int redRecs = mapRecs - REDUCER_BAD_RECORDS.size();
      assertEquals(counters.findCounter(counterGrp, "REDUCE_SKIPPED_RECORDS").
          getCounter(),REDUCER_BAD_RECORDS.size());
      assertEquals(counters.findCounter(counterGrp, "REDUCE_SKIPPED_GROUPS").
          getCounter(),REDUCER_BAD_RECORDS.size());
      assertEquals(counters.findCounter(counterGrp, "REDUCE_INPUT_GROUPS").
          getCounter(),redRecs);
      assertEquals(counters.findCounter(counterGrp, "REDUCE_INPUT_RECORDS").
          getCounter(),redRecs);
      assertEquals(counters.findCounter(counterGrp, "REDUCE_OUTPUT_RECORDS").
          getCounter(),redRecs);
    }
    
    List<String> badRecs = new ArrayList<String>();
    badRecs.addAll(MAPPER_BAD_RECORDS);
    badRecs.addAll(REDUCER_BAD_RECORDS);
    Path[] outputFiles = FileUtil.stat2Paths(
        getFileSystem().listStatus(getOutputDir(),
        new Utils.OutputFileUtils.OutputFilesFilter()));
    
    if (outputFiles.length > 0) {
      InputStream is = getFileSystem().open(outputFiles[0]);
      BufferedReader reader = new BufferedReader(new InputStreamReader(is));
      String line = reader.readLine();
      int counter = 0;
      while (line != null) {
        counter++;
        StringTokenizer tokeniz = new StringTokenizer(line, "\t");
        String value = tokeniz.nextToken();
        int index = value.indexOf("hey");
        assertTrue(index>-1);
        if(index>-1) {
          String heyStr = value.substring(index);
          assertTrue(!badRecs.contains(heyStr));
        }
        
        line = reader.readLine();
      }
      reader.close();
      if(validateCount) {
        assertEquals(INPUTSIZE-badRecs.size(), counter);
      }
    }
  }

  /*
   * Disable test as skipping bad records not supported in 0.23
   */
  /*
  public void testSkip() throws Exception {
    JobConf clusterConf = createJobConf();
    createInput();
    int attSkip =0;
    SkipBadRecords.setAttemptsToStartSkipping(clusterConf,attSkip);
    //the no of attempts to successfully complete the task depends 
    //on the no of bad records.
    int mapperAttempts = attSkip+1+MAPPER_BAD_RECORDS.size();
    int reducerAttempts = attSkip+1+REDUCER_BAD_RECORDS.size();
    
    String[] args =  new String[] {
      "-input", (new Path(getInputDir(), "text.txt")).toString(),
      "-output", getOutputDir().toString(),
      "-mapper", badMapper,
      "-reducer", badReducer,
      "-verbose",
      "-inputformat", "org.apache.hadoop.mapred.KeyValueTextInputFormat",
      "-jobconf", "mapreduce.task.skip.start.attempts="+attSkip,
      "-jobconf", "mapreduce.job.skip.outdir=none",
      "-jobconf", "mapreduce.map.maxattempts="+mapperAttempts,
      "-jobconf", "mapreduce.reduce.maxattempts="+reducerAttempts,
      "-jobconf", "mapreduce.map.skip.maxrecords="+Long.MAX_VALUE,
      "-jobconf", "mapreduce.reduce.skip.maxgroups="+Long.MAX_VALUE,
      "-jobconf", "mapreduce.job.maps=1",
      "-jobconf", "mapreduce.job.reduces=1",
      "-jobconf", "fs.default.name="+clusterConf.get("fs.default.name"),
      "-jobconf", "mapreduce.jobtracker.address=" + 
                   clusterConf.get(JTConfig.JT_IPC_ADDRESS),
      "-jobconf", "mapreduce.jobtracker.http.address="
                    +clusterConf.get(JTConfig.JT_HTTP_ADDRESS),
      "-jobconf", "mapreduce.task.files.preserve.failedtasks=true",
      "-jobconf", "stream.tmpdir="+System.getProperty("test.build.data","/tmp"),
      "-jobconf", "mapred.jar=" + TestStreaming.STREAMING_JAR,
      "-jobconf", "mapreduce.framework.name=yarn"
    };
    StreamJob job = new StreamJob(args, false);      
    job.go();
    validateOutput(job.running_, false);
    //validate that there is no skip directory as it has been set to "none"
    assertTrue(SkipBadRecords.getSkipOutputPath(job.jobConf_)==null);
  }
  */
  
  /*
   * Disable test as skipping bad records not supported in 0.23
   */
  /*
  public void testNarrowDown() throws Exception {
    createInput();
    JobConf clusterConf = createJobConf();
    String[] args =  new String[] {
      "-input", (new Path(getInputDir(), "text.txt")).toString(),
      "-output", getOutputDir().toString(),
      "-mapper", badMapper,
      "-reducer", badReducer,
      "-verbose",
      "-inputformat", "org.apache.hadoop.mapred.KeyValueTextInputFormat",
      "-jobconf", "mapreduce.task.skip.start.attempts=1",
      //actually fewer attempts are required than specified
      //but to cater to the case of slow processed counter update, need to 
      //have more attempts
      "-jobconf", "mapreduce.map.maxattempts=20",
      "-jobconf", "mapreduce.reduce.maxattempts=15",
      "-jobconf", "mapreduce.map.skip.maxrecords=1",
      "-jobconf", "mapreduce.reduce.skip.maxgroups=1",
      "-jobconf", "mapreduce.job.maps=1",
      "-jobconf", "mapreduce.job.reduces=1",
      "-jobconf", "fs.default.name="+clusterConf.get("fs.default.name"),
      "-jobconf", "mapreduce.jobtracker.address="+clusterConf.get(JTConfig.JT_IPC_ADDRESS),
      "-jobconf", "mapreduce.jobtracker.http.address="
                    +clusterConf.get(JTConfig.JT_HTTP_ADDRESS),
      "-jobconf", "mapreduce.task.files.preserve.failedtasks=true",
      "-jobconf", "stream.tmpdir="+System.getProperty("test.build.data","/tmp"),
      "-jobconf", "mapred.jar=" + TestStreaming.STREAMING_JAR,
      "-jobconf", "mapreduce.framework.name=yarn"
    };
    StreamJob job = new StreamJob(args, false);      
    job.go();
    
    validateOutput(job.running_, true);
    assertTrue(SkipBadRecords.getSkipOutputPath(job.jobConf_)!=null);
  }
  */

  @Test
  public void testNoOp() {
    // Added to avoid warnings when running this disabled test
  }
  
  static class App{
    boolean isReducer;
    
    public App(String[] args) throws Exception{
      if(args.length>0) {
        isReducer = Boolean.parseBoolean(args[0]);
      }
      String counter = SkipBadRecords.COUNTER_MAP_PROCESSED_RECORDS;
      if(isReducer) {
        counter = SkipBadRecords.COUNTER_REDUCE_PROCESSED_GROUPS;
      }
      BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
      String line;
      int count = 0;
      while ((line = in.readLine()) != null) {
        processLine(line);
        count++;
        if(count>=10) {
          System.err.println("reporter:counter:"+SkipBadRecords.COUNTER_GROUP+
              ","+counter+","+count);
          count = 0;
        }
      }
    }
    
    protected void processLine(String line) throws Exception{
      System.out.println(line);
    }
    
    
    public static void main(String[] args) throws Exception{
      new App(args);
    }
  }
  
  static class BadApp extends App{
    
    public BadApp(String[] args) throws Exception {
      super(args);
    }

    protected void processLine(String line) throws Exception {
      List<String> badRecords = MAPPER_BAD_RECORDS;
      if(isReducer) {
        badRecords = REDUCER_BAD_RECORDS;
      }
      if(badRecords.size()>0 && line.contains(badRecords.get(0))) {
        LOG.warn("Encountered BAD record");
        System.exit(-1);
      }
      else if(badRecords.size()>1 && line.contains(badRecords.get(1))) {
        LOG.warn("Encountered BAD record");
        throw new Exception("Got bad record..crashing");
      }
      else if(badRecords.size()>2 && line.contains(badRecords.get(2))) {
        LOG.warn("Encountered BAD record");
        System.exit(-1);
      }
      super.processLine(line);
    }
    
    public static void main(String[] args) throws Exception{
      new BadApp(args);
    }
  }
  
  

}
