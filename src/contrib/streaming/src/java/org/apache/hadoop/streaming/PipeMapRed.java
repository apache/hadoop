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
import java.util.Iterator;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Properties;
import java.util.regex.*;

import org.apache.commons.logging.*;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.OutputCollector;

import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;

/** Shared functionality for PipeMapper, PipeReducer.
 *  @author Michel Tourn
 */
public abstract class PipeMapRed {

  protected static final Log LOG = LogFactory.getLog(PipeMapRed.class.getName());

  /** The command to be spawned as a subprocess.
   * Mapper/Reducer operations will delegate to it
   */
  abstract String getPipeCommand(JobConf job);
  /*
  */
  abstract String getKeyColPropName();

  /** Write output as side-effect files rather than as map outputs.
      This is useful to do "Map" tasks rather than "MapReduce" tasks. */
  boolean getUseSideEffect()
  {
    return false;
  }

  /**
   * @returns how many TABS before the end of the key part
   * usually: 1 or "ALL"
   * used for tool output of both Map and Reduce
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

  final static int OUTSIDE = 1;
  final static int SINGLEQ = 2;
  final static int DOUBLEQ = 3;

  static String[] splitArgs(String args)
  {
    ArrayList argList = new ArrayList();
    char[] ch = args.toCharArray();
    int clen = ch.length;
    int state = OUTSIDE;
    int argstart = 0;
    for(int c=0; c<=clen; c++) {
        boolean last = (c==clen);
        int lastState = state;
        boolean endToken = false;
        if(!last) {
          if(ch[c]=='\'') {
            if(state == OUTSIDE) {
              state = SINGLEQ;
            } else if(state == SINGLEQ) {
              state = OUTSIDE;
            }
            endToken = (state != lastState);
          } else if(ch[c]=='"') {
            if(state == OUTSIDE) {
              state = DOUBLEQ;
            } else if(state == DOUBLEQ) {
              state = OUTSIDE;
            }
            endToken = (state != lastState);
          } else if(ch[c]==' ') {
            if(state == OUTSIDE) {
              endToken = true;
            }
          }
        }
        if(last || endToken) {
          if(c == argstart) {
            // unquoted space
          } else {
            String a;
            a = args.substring(argstart, c);
            argList.add(a);
          }
          argstart = c+1;
          lastState = state;
        }
    }
    return (String[])argList.toArray(new String[0]);
  }

  public void configure(JobConf job)
  {

    try {
      String argv = getPipeCommand(job);
      keyCols_ = getKeyColsFromPipeCommand(argv);

      job_ = job;

      // Currently: null is identity reduce. REDUCE_NONE is no-map-outputs.
      doPipe_ = (argv != null) && !StreamJob.REDUCE_NONE.equals(argv);
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


      if(job_.getInputValueClass().equals(BytesWritable.class)) {
        // TODO expose as separate config:
        // job or semistandard inputformat property
        optUseKey_ = false;
      }

      optSideEffect_ = getUseSideEffect();

      if(optSideEffect_) {
        String fileName = job_.get("mapred.task.id");
        sideEffectPath_ = new Path(job_.getOutputPath(), fileName);
        FileSystem fs = FileSystem.get(job_);
        sideEffectOut_ = fs.create(sideEffectPath_);
      }

      // argvSplit[0]:
      // An absolute path should be a preexisting valid path on all TaskTrackers
      // A  relative path should match in the unjarred Job data
      // In this case, force an absolute path to make sure exec finds it.
      argvSplit[0] = new File(argvSplit[0]).getAbsolutePath();
      logprintln("PipeMapRed exec " + Arrays.asList(argvSplit));
      logprintln("sideEffectPath_=" + sideEffectPath_);

      Environment childEnv = (Environment)StreamUtil.env().clone();
      addJobConfToEnvironment(job_, childEnv);
      addEnvironment(childEnv, job_.get("stream.addenvironment"));
      sim = Runtime.getRuntime().exec(argvSplit, childEnv.toArray());

      /* // This way required jdk1.5
      ProcessBuilder processBuilder = new ProcessBuilder(argvSplit);
      Map<String, String> env = processBuilder.environment();
      addEnvironment(env, job_.get("stream.addenvironment"));
      sim = processBuilder.start();
      */

      clientOut_ = new DataOutputStream(new BufferedOutputStream(sim.getOutputStream()));
      clientIn_  = new DataInputStream(new BufferedInputStream(sim.getInputStream()));
      clientErr_ = new DataInputStream(new BufferedInputStream(sim.getErrorStream()));
      doneLock_  = new Object();
      startTime_ = System.currentTimeMillis();

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
      logprintln("JobConf set minRecWrittenToEnableSkip_ =" + minRecWrittenToEnableSkip_);
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

  void logprintln(String s)
  {
    if(log_ != null) {
      log_.println(s);
    } else {
      System.err.println(s); // or LOG.info()
    }
  }

  void logflush()
  {
    if(log_ != null) {
      log_.flush();
    }
  }

  void addJobConfToEnvironment(JobConf conf, Properties env)
  {
    logprintln("addJobConfToEnvironment: begin");
    Iterator it = conf.entries();
    while(it.hasNext()) {
        Map.Entry en = (Map.Entry)it.next();
        String name = (String)en.getKey();
        String value = (String)en.getValue();
        name = safeEnvVarName(name);
        envPut(env, name, value);
    }
    logprintln("addJobConfToEnvironment: end");
  }
  
  String safeEnvVarName(String var)
  {
    StringBuffer safe = new StringBuffer();
    int len = var.length();
    for(int i=0; i<len; i++) {
        char c = var.charAt(i);
        char s;
        if((c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')) {
          s = c;
        } else {
          s = '_';
        }
        safe.append(s);
    }
    return safe.toString();
  }

  void addEnvironment(Properties env, String nameVals)
  {
    // encoding "a=b c=d" from StreamJob
    if(nameVals == null) return;
    String[] nv = nameVals.split(" ");
    for(int i=0; i<nv.length; i++) {
      String[] pair = nv[i].split("=", 2);
      if(pair.length != 2) {
        logprintln("Skip ev entry:" + nv[i]);
      } else {
        envPut(env, pair[0], pair[1]);
      }
    }
  }

  void envPut(Properties env, String name, String value)
  {
    logprintln("Add  ev entry:" + name + "=" + value);
    env.put(name, value);
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
                if(optSideEffect_) {
                  sideEffectOut_.write(answer.getBytes());
                  sideEffectOut_.write('\n');
                } else {
                  splitKeyVal(answer, key, val);
                  output.collect(key, val);
                  numRecWritten_++;
                  if(numRecWritten_ % 100 == 0) {
                    logprintln(numRecRead_+"/"+numRecWritten_);
                    logflush();
                  }
                }
              }
            } catch(IOException io) {
              io.printStackTrace(log_);
            }
            logprintln("MROutputThread done");
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
          logprintln(line);
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
    logprintln("mapRedFinished");
    try {
    if(!doPipe_) return;
    try {
      if(optSideEffect_) {
        logprintln("closing " + sideEffectPath_);
        sideEffectOut_.close();
        logprintln("closed  " + sideEffectPath_);
      }
    } catch(IOException io) {
      io.printStackTrace();
    }
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
      String info = numRecInfo();
      logprintln(info);
      logflush();
      System.err.println(info);
      //nextRecReadLog_ *= 10;
      nextRecReadLog_ += 100;
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
    long elapsed = (System.currentTimeMillis() - startTime_)/1000;
    long total = numRecRead_+numRecWritten_+numRecSkipped_;
    return "R/W/S=" + numRecRead_+"/"+numRecWritten_+"/"+numRecSkipped_
     + " in:"  + safeDiv(numRecRead_, elapsed) + " [rec/s]"
     + " out:" + safeDiv(numRecWritten_, elapsed) + " [rec/s]";
  }
  String safeDiv(long n, long d)
  {
    return (d==0) ? "NA" : ""+n/d + "=" + n + "/" + d;
  }
  String logFailure(Exception e)
  {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
      String msg = "log:" + jobLog_ + "\n" + getContext() + sw + "\n";
      logprintln(msg);
      return msg;
  }


  long startTime_;
  long numRecRead_ = 0;
  long numRecWritten_ = 0;
  long numRecSkipped_ = 0;
  long nextRecReadLog_ = 1;

  long minRecWrittenToEnableSkip_ = Long.MAX_VALUE;

  int keyCols_;
  final static int ALL_COLS = Integer.MAX_VALUE;

  JobConf job_;

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
  DataInputStream   clientIn_;

  String jobLog_;
  // set in PipeMapper/PipeReducer subclasses
  String mapredKey_;
  int numExceptions_;

  boolean optUseKey_ = true;

  boolean optSideEffect_;
  Path sideEffectPath_;
  FSDataOutputStream sideEffectOut_;

  String LOGNAME;
  PrintStream log_;

  /* curr. going to stderr so that it is preserved
  { // instance initializer
    try {
      int id = (int)((System.currentTimeMillis()/2000) % 10);
      String sid = id+ "." + StreamUtil.env().get("USER");
      LOGNAME = "/tmp/PipeMapRed." + sid + ".log";
      log_ = new PrintStream(new FileOutputStream(LOGNAME));
      logprintln(new java.util.Date());
      logflush();
    } catch(IOException io) {
      System.err.println("LOGNAME=" + LOGNAME);
      io.printStackTrace();
    } finally {
      if(log_ == null) {
        log_ = System.err;
      }
    }
  }
  */
}
