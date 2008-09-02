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
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SkipBadRecords;

public class TestStreamingBadRecords extends ClusterMapReduceTestCase
{

  private static final Log LOG = 
    LogFactory.getLog(TestStreamingBadRecords.class);
  
  private static final List<String> MAPPER_BAD_RECORDS = 
    Arrays.asList("hey022","hey023","hey099");
  
  private static final List<String> REDUCER_BAD_RECORDS = 
    Arrays.asList("hey001","hey024");
  
  private static final String badMapper = 
    StreamUtil.makeJavaCommand(BadApp.class, new String[]{});
  private static final String badReducer = 
    StreamUtil.makeJavaCommand(BadApp.class, new String[]{"true"});
  private static final int INPUTSIZE=100;
  
  public TestStreamingBadRecords() throws IOException
  {
    UtilTest utilTest = new UtilTest(getClass().getName());
    utilTest.checkUserDir();
    utilTest.redirectIfAntJunit();
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
    List<String> badRecs = new ArrayList<String>();
    badRecs.addAll(MAPPER_BAD_RECORDS);
    badRecs.addAll(REDUCER_BAD_RECORDS);
    Path[] outputFiles = FileUtil.stat2Paths(
        getFileSystem().listStatus(getOutputDir(),
        new OutputLogFilter()));
    
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

  public void testDisableSkip() throws Exception {
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
      "-jobconf", "mapred.skip.attempts.to.start.skipping="+attSkip,
      "-jobconf", "mapred.map.max.attempts="+mapperAttempts,
      "-jobconf", "mapred.reduce.max.attempts="+reducerAttempts,
      "-jobconf", "mapred.skip.mode.enabled=false",
      "-jobconf", "mapred.map.tasks=1",
      "-jobconf", "mapred.reduce.tasks=1",
      "-jobconf", "mapred.task.timeout=30000",
      "-jobconf", "fs.default.name="+clusterConf.get("fs.default.name"),
      "-jobconf", "mapred.job.tracker="+clusterConf.get("mapred.job.tracker"),
      "-jobconf", "mapred.job.tracker.http.address="
                    +clusterConf.get("mapred.job.tracker.http.address"),
      "-jobconf", "stream.debug=set",
      "-jobconf", "keep.failed.task.files=true",
      "-jobconf", "stream.tmpdir="+System.getProperty("test.build.data","/tmp")
    };
    StreamJob job = new StreamJob(args, false);      
    job.go();
    assertFalse(job.running_.isSuccessful());
  }
  
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
      "-jobconf", "mapred.skip.attempts.to.start.skipping="+attSkip,
      "-jobconf", "mapred.map.max.attempts="+mapperAttempts,
      "-jobconf", "mapred.reduce.max.attempts="+reducerAttempts,
      "-jobconf", "mapred.skip.mode.enabled=true",
      "-jobconf", "mapred.map.tasks=1",
      "-jobconf", "mapred.reduce.tasks=1",
      "-jobconf", "mapred.task.timeout=30000",
      "-jobconf", "fs.default.name="+clusterConf.get("fs.default.name"),
      "-jobconf", "mapred.job.tracker="+clusterConf.get("mapred.job.tracker"),
      "-jobconf", "mapred.job.tracker.http.address="
                    +clusterConf.get("mapred.job.tracker.http.address"),
      "-jobconf", "stream.debug=set",
      "-jobconf", "keep.failed.task.files=true",
      "-jobconf", "stream.tmpdir="+System.getProperty("test.build.data","/tmp")
    };
    StreamJob job = new StreamJob(args, false);      
    job.go();
    validateOutput(job.running_, false);
  }
  
  static class App{
    boolean isReducer;
    
    public App(String[] args) throws Exception{
      if(args.length>0) {
        isReducer = Boolean.parseBoolean(args[0]);
      }
      String counter = Counters.Application.MAP_PROCESSED_RECORDS;
      if(isReducer) {
        counter = Counters.Application.REDUCE_PROCESSED_RECORDS;
      }
      BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
      String line;
      int count = 0;
      while ((line = in.readLine()) != null) {
        processLine(line);
        count++;
        if(count>=10) {
          System.err.println("reporter:counter:"+Counters.Application.GROUP+","+
              counter+","+count);
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
        Thread.sleep(15*60*1000);
      }
      super.processLine(line);
    }
    
    public static void main(String[] args) throws Exception{
      new BadApp(args);
    }
  }
  
  

}
