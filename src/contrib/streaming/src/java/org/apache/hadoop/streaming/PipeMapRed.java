/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.io.*;
import java.nio.channels.*;
import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.*;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.OutputCollector;

import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;

/** Shared functionality for PipeMapper, PipeReducer.
 *  @author Michel Tourn
 */
public abstract class PipeMapRed {

  /** The command to be spawned as a subprocess.
   * Mapper/Reducer operations will delegate to it
   */
  abstract String getPipeCommand(JobConf job);
  /*
  */
  abstract String getKeyColPropName();
  

  /**
   * @returns ow many TABS before the end of the key part 
   * usually: 1 or "ALL"
   * used both for tool output of both Map and Reduce
   * configured via tool's argv: splitKeyVal=ALL or 1..
   * although it is interpreted here, not by tool
   */
  int getKeyColsFromPipeCommand(String cmd)
  {          
    String key = getKeyColPropName();
    Pattern kcPat = Pattern.compile(".*" + key + "=([^\\s]*).*");
    Matcher match = kcPat.matcher(cmd);
    String kc;
    if(!match.matches()) {
      kc = null;
    } else {
      kc = match.group(1);
    }

    int cols;
    if(kc== null) {
      // default value is 1 and the Stream applications could instead 
      // add/remove the \t separator on lines to get the same effect as value 0, 1, ALL
      cols = 1;
    } else if(kc.equals("ALL")) {
      cols = ALL_COLS;
    } else {
      try {
        cols = Integer.parseInt(kc);    
      } catch(NumberFormatException nf) {
        cols = Integer.MAX_VALUE;
      }
    }

    System.out.println("getKeyColsFromPipeCommand:" + key + " parse:" + cols + " from cmd=" + cmd);
    
    return cols;
  }
  
  String[] splitArgs(String args)
  {
    String regex = "\\s(?=(?:[^\"]*\"[^\"]*\")*[^\"]*\\z)";
    String[] split = args.split(regex);
    // remove outer quotes
    for(int i=0; i<split.length; i++) {
        String si = split[i].trim();
        if(si.charAt(0)=='"' && si.charAt(si.length()-1)=='"') {
            si = si.substring(1, si.length()-1);
            split[i] = si;
        }
    }
    return split;
  }
  public void configure(JobConf job)
  {

    try {
      String argv = getPipeCommand(job);
      keyCols_ = getKeyColsFromPipeCommand(argv);
      
      doPipe_ = (argv != null);
      if(!doPipe_) return;

      setStreamJobDetails(job);
      setStreamProperties();
            
      String[] argvSplit = splitArgs(argv);
      String prog = argvSplit[0];
      String userdir = System.getProperty("user.dir");
      if(new File(prog).isAbsolute()) {
        // we don't own it. Hope it is executable
      } else {
        new MustangFile(prog).setExecutable(true, true);
      }
      
      // argvSplit[0]: 
      // An absolute path should be a preexisting valid path on all TaskTrackers
	  // A  relative path should match in the unjarred Job data
      // In this case, force an absolute path to make sure exec finds it.
      argvSplit[0] = new File(argvSplit[0]).getAbsolutePath();
      log_.println("PipeMapRed exec " + Arrays.toString(argvSplit));
            
      
      Environment childEnv = (Environment)StreamUtil.env().clone();
      addEnvironment(childEnv, job.get("stream.addenvironment"));
      sim = Runtime.getRuntime().exec(argvSplit, childEnv.toArray());
      
      /* // This way required jdk1.5
      ProcessBuilder processBuilder = new ProcessBuilder(argvSplit);
      Map<String, String> env = processBuilder.environment();
      addEnvironment(env, job.get("stream.addenvironment"));
      sim = processBuilder.start();
      */
      
      clientOut_ = new DataOutputStream(new BufferedOutputStream(sim.getOutputStream()));
      clientIn_  = new BufferedReader(new InputStreamReader(sim.getInputStream()));
      clientErr_ = new DataInputStream(sim.getErrorStream());
      doneLock_  = new Object();
      
    } catch(Exception e) {
        e.printStackTrace();
        e.printStackTrace(log_);
    } 
  }
  
  void setStreamJobDetails(JobConf job)
  {
    jobLog_ = job.get("stream.jobLog_");
    String s = job.get("stream.minRecWrittenToEnableSkip_");
    if(s != null) {
      minRecWrittenToEnableSkip_ = Long.parseLong(s);
      log_.println("JobConf set minRecWrittenToEnableSkip_ =" + minRecWrittenToEnableSkip_);
    }
  }
  
  void setStreamProperties()
  {
    taskid_ = System.getProperty("stream.taskid");
    if(taskid_ == null) {
      taskid_ = "noid" + System.currentTimeMillis();
    }
    String s = System.getProperty("stream.port");
    if(s != null) {
      reportPortPlusOne_ = Integer.parseInt(s);
    }
    
  }
    
  void addEnvironment(Properties env, String nameVals)
  {
    // encoding "a=b c=d" from StreamJob
    if(nameVals == null) return;
    String[] nv = nameVals.split(" ");
    for(int i=0; i<nv.length; i++) {
      String[] pair = nv[i].split("=", 2);
      if(pair.length != 2) {
        log_.println("Skip ev entry:" + nv[i]);
      } else {
        log_.println("Add  ev entry:" + nv[i]);
        env.put(pair[0], pair[1]);
      }
    }
  }
  
  /** .. and if successful: delete the task log */
  void appendLogToJobLog(String status)
  {
    if(jobLog_ == null) {
      return; // not using a common joblog
    }
    StreamUtil.exec("/bin/rm " + LOGNAME, log_);
    // TODO socket-based aggregator (in JobTrackerInfoServer)
  }
  
  
  void startOutputThreads(OutputCollector output, Reporter reporter)
  {
      outputDone_ = false;
      errorDone_ = false;
      outThread_ = new MROutputThread(output, reporter);
      outThread_.start();
      errThread_ = new MRErrorThread(reporter);
      errThread_.start();
  }
    
  void splitKeyVal(String line, UTF8 key, UTF8 val)
  {
    int pos;
    if(keyCols_ == ALL_COLS) {
      pos = -1;
    } else {
      pos = line.indexOf('\t');
    }    
    if(pos == -1) {
      key.set(line);
      val.set("");      
    } else {
      key.set(line.substring(0, pos));
      val.set(line.substring(pos+1));
    }
  }

  class MROutputThread extends Thread
  {
    MROutputThread(OutputCollector output, Reporter reporter)
    {
      setDaemon(true);
      this.output = output;
      this.reporter = reporter;
    }
    public void run() {
      try {
            try {
              UTF8 EMPTY = new UTF8("");
              UTF8 key = new UTF8();
              UTF8 val = new UTF8();
              // 3/4 Tool to Hadoop
              while((answer = clientIn_.readLine()) != null) {
                // 4/4 Hadoop out 
                splitKeyVal(answer, key, val);
                output.collect(key, val);
                numRecWritten_++;
                if(numRecWritten_ % 100 == 0) {
                  log_.println(numRecRead_+"/"+numRecWritten_);
                  log_.flush();
                }
              }
            } catch(IOException io) {
              io.printStackTrace(log_);
            }
            log_.println("MROutputThread done");
      } finally {
          outputDone_ = true;
          synchronized(doneLock_) {
            doneLock_.notifyAll();
          }
      }
    }
    OutputCollector output;
    Reporter reporter;
    String answer;
  }

  class MRErrorThread extends Thread
  {
    public MRErrorThread(Reporter reporter)
    {
      this.reporter = reporter;
      setDaemon(true);
    }
    public void run()
    {
      String line;
      try {
        long num = 0;
        int bucket = 100;
        while((line=clientErr_.readLine()) != null) {
          num++;
          log_.println(line);
          if(num < 10) {
            String hline = "MRErr: " + line;
            System.err.println(hline);
            reporter.setStatus(hline);
          }
        }
      } catch(IOException io) {
        io.printStackTrace(log_);
      } finally {
        errorDone_ = true;
        synchronized(doneLock_) {
          doneLock_.notifyAll();
        }
      }
    }
    Reporter reporter;
  }

  public void mapRedFinished()
  {
    log_.println("mapRedFinished");
    try {
    if(!doPipe_) return;
    try {
      if(clientOut_ != null) {
      	clientOut_.close();
      }
    } catch(IOException io) {
    }
    if(outThread_ == null) {
      // no input records: threads were never spawned
    } else {
      try {
        while(!outputDone_ || !errorDone_) {
          synchronized(doneLock_) {
            doneLock_.wait();
          }
        }
      } catch(InterruptedException ie) {
        ie.printStackTrace();
      }
    }
      sim.destroy();
    } catch(RuntimeException e) {
      e.printStackTrace(log_);
      throw e;
    }
  }
  
  void maybeLogRecord()
  {
    if(numRecRead_ >= nextRecReadLog_) {
      log_.println(numRecInfo());
      log_.flush();      
      nextRecReadLog_ *= 10;
      //nextRecReadLog_ += 1000;
    }    
  }
  
  public String getContext()
  {
    
    String s = numRecInfo() + "\n";
    s += "minRecWrittenToEnableSkip_=" + minRecWrittenToEnableSkip_ + " ";
    s += "LOGNAME=" + LOGNAME + "\n";
    s += envline("HOST");
    s += envline("USER");
    s += envline("HADOOP_USER");
    //s += envline("PWD"); // =/home/crawler/hadoop/trunk 
    s += "last Hadoop input: |" + mapredKey_ + "|\n";
    s += "last tool output: |" + outThread_.answer + "|\n";
    s += "Date: " + new Date() + "\n";
    // s += envline("HADOOP_HOME");
    // s += envline("REMOTE_HOST");
    return s;
  }
  
  String envline(String var)
  {
    return var + "=" + StreamUtil.env().get(var) + "\n";
  }
  
  String numRecInfo()
  {
    return "R/W/S=" + numRecRead_+"/"+numRecWritten_+"/"+numRecSkipped_;
  }
  String logFailure(Exception e)
  {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);    
      String msg = "log:" + jobLog_ + "\n" + getContext() + sw + "\n";
      log_.println(msg);
      return msg;  
  }
    

  long numRecRead_ = 0;
  long numRecWritten_ = 0;
  long numRecSkipped_ = 0;
  
  long nextRecReadLog_ = 1;
  
  long minRecWrittenToEnableSkip_ = Long.MAX_VALUE;
  
  int keyCols_;
  final static int ALL_COLS = Integer.MAX_VALUE;
  
  // generic MapRed parameters passed on by hadoopStreaming
  String taskid_;
  int reportPortPlusOne_;

  boolean doPipe_;
  
  Process sim;
  Object doneLock_;
  MROutputThread outThread_;
  MRErrorThread errThread_;
  boolean outputDone_;
  boolean errorDone_;
  DataOutputStream clientOut_;
  DataInputStream  clientErr_;
  BufferedReader   clientIn_;

  String jobLog_;
  // set in PipeMapper/PipeReducer subclasses
  String mapredKey_;
  int numExceptions_;
  
  String LOGNAME;
  PrintStream log_;
  
  { // instance initializer
    try {
      int id = (int)((System.currentTimeMillis()/2000) % 10);
      String sid = id+ "." + StreamUtil.env().get("USER");
      LOGNAME = "/tmp/PipeMapRed." + sid + ".log";
      log_ = new PrintStream(new FileOutputStream(LOGNAME));
      log_.println(new java.util.Date());
      log_.flush();
    } catch(IOException io) {
      System.err.println("LOGNAME=" + LOGNAME);
      io.printStackTrace();
    } finally {
      if(log_ == null) {
        log_ = System.err;
      }
    }    
  }
}
