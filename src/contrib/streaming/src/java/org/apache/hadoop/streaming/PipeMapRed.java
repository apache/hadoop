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

import java.io.*;
import java.nio.charset.CharacterCodingException;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Iterator;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.commons.logging.*;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.TaskLog;
import org.apache.hadoop.mapred.LineRecordReader.LineReader;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BytesWritable;

import org.apache.hadoop.fs.FileSystem;

/** Shared functionality for PipeMapper, PipeReducer.
 */
public abstract class PipeMapRed {

  protected static final Log LOG = LogFactory.getLog(PipeMapRed.class.getName());

  /** The command to be spawned as a subprocess.
   * Mapper/Reducer operations will delegate to it
   */
  abstract String getPipeCommand(JobConf job);

  abstract char getFieldSeparator();
  
  abstract int getNumOfKeyFields();

  abstract boolean getDoPipe();

  final static int OUTSIDE = 1;
  final static int SINGLEQ = 2;
  final static int DOUBLEQ = 3;

  static String[] splitArgs(String args) {
    ArrayList argList = new ArrayList();
    char[] ch = args.toCharArray();
    int clen = ch.length;
    int state = OUTSIDE;
    int argstart = 0;
    for (int c = 0; c <= clen; c++) {
      boolean last = (c == clen);
      int lastState = state;
      boolean endToken = false;
      if (!last) {
        if (ch[c] == '\'') {
          if (state == OUTSIDE) {
            state = SINGLEQ;
          } else if (state == SINGLEQ) {
            state = OUTSIDE;
          }
          endToken = (state != lastState);
        } else if (ch[c] == '"') {
          if (state == OUTSIDE) {
            state = DOUBLEQ;
          } else if (state == DOUBLEQ) {
            state = OUTSIDE;
          }
          endToken = (state != lastState);
        } else if (ch[c] == ' ') {
          if (state == OUTSIDE) {
            endToken = true;
          }
        }
      }
      if (last || endToken) {
        if (c == argstart) {
          // unquoted space
        } else {
          String a;
          a = args.substring(argstart, c);
          argList.add(a);
        }
        argstart = c + 1;
        lastState = state;
      }
    }
    return (String[]) argList.toArray(new String[0]);
  }

  public void configure(JobConf job) {
    try {
      String argv = getPipeCommand(job);

      joinDelay_ = job.getLong("stream.joindelay.milli", 0);

      job_ = job;
      fs_ = FileSystem.get(job_);

      String mapOutputFieldSeparator = job_.get("stream.map.output.field.separator", "\t");
      String reduceOutputFieldSeparator = job_.get("stream.reduce.output.field.separator", "\t");
      this.mapOutputFieldSeparator = mapOutputFieldSeparator.charAt(0);
      this.reduceOutFieldSeparator = reduceOutputFieldSeparator.charAt(0);
      this.numOfMapOutputKeyFields = job_.getInt("stream.num.map.output.key.fields", 1);
      this.numOfReduceOutputKeyFields = job_.getInt("stream.num.reduce.output.key.fields", 1);

      nonZeroExitIsFailure_ = job_.getBoolean("stream.non.zero.exit.is.failure", false);
      
      doPipe_ = getDoPipe();
      if (!doPipe_) return;

      setStreamJobDetails(job);
      
      String[] argvSplit = splitArgs(argv);
      String prog = argvSplit[0];
      File currentDir = new File(".").getAbsoluteFile();
      if (new File(prog).isAbsolute()) {
        // we don't own it. Hope it is executable
      } else {
        FileUtil.chmod(new File(currentDir, prog).toString(), "a+x");
      }

      // 
      // argvSplit[0]:
      // An absolute path should be a preexisting valid path on all TaskTrackers
      // A relative path is converted into an absolute pathname by looking
      // up the PATH env variable. If it still fails, look it up in the
      // tasktracker's local working directory
      //
      if (!new File(argvSplit[0]).isAbsolute()) {
        PathFinder finder = new PathFinder("PATH");
        finder.prependPathComponent(currentDir.toString());
        File f = finder.getAbsolutePath(argvSplit[0]);
        if (f != null) {
          argvSplit[0] = f.getAbsolutePath();
        }
        f = null;
      }
      logprintln("PipeMapRed exec " + Arrays.asList(argvSplit));
      Environment childEnv = (Environment) StreamUtil.env().clone();
      addJobConfToEnvironment(job_, childEnv);
      addEnvironment(childEnv, job_.get("stream.addenvironment"));
      // add TMPDIR environment variable with the value of java.io.tmpdir
      envPut(childEnv, "TMPDIR", System.getProperty("java.io.tmpdir"));

      // Start the process
      ProcessBuilder builder = new ProcessBuilder(argvSplit);
      builder.environment().putAll(childEnv.toMap());
      sim = builder.start();

      clientOut_ = new DataOutputStream(new BufferedOutputStream(sim.getOutputStream()));
      clientIn_ = new DataInputStream(new BufferedInputStream(sim.getInputStream()));
      clientErr_ = new DataInputStream(new BufferedInputStream(sim.getErrorStream()));
      startTime_ = System.currentTimeMillis();

      errThread_ = new MRErrorThread();
      errThread_.start();
    } catch (Exception e) {
      logStackTrace(e);
      LOG.error("configuration exception", e);
      throw new RuntimeException("configuration exception", e);
    }
  }
  
  void setStreamJobDetails(JobConf job) {
    jobLog_ = job.get("stream.jobLog_");
    String s = job.get("stream.minRecWrittenToEnableSkip_");
    if (s != null) {
      minRecWrittenToEnableSkip_ = Long.parseLong(s);
      logprintln("JobConf set minRecWrittenToEnableSkip_ =" + minRecWrittenToEnableSkip_);
    }
    taskId_ = StreamUtil.getTaskInfo(job_);
  }

  void logStackTrace(Exception e) {
    if (e == null) return;
    e.printStackTrace();
    if (log_ != null) {
      e.printStackTrace(log_);
    }
  }

  void logprintln(String s) {
    if (log_ != null) {
      log_.println(s);
    } else {
      LOG.info(s); // or LOG.info()
    }
  }

  void logflush() {
    if (log_ != null) {
      log_.flush();
    }
  }

  void addJobConfToEnvironment(JobConf conf, Properties env) {
    if (debug_) {
      logprintln("addJobConfToEnvironment: begin");
    }
    Iterator it = conf.iterator();
    while (it.hasNext()) {
      Map.Entry en = (Map.Entry) it.next();
      String name = (String) en.getKey();
      //String value = (String)en.getValue(); // does not apply variable expansion
      String value = conf.get(name); // does variable expansion 
      name = safeEnvVarName(name);
      envPut(env, name, value);
    }
    if (debug_) {
      logprintln("addJobConfToEnvironment: end");
    }
  }

  String safeEnvVarName(String var) {
    StringBuffer safe = new StringBuffer();
    int len = var.length();
    for (int i = 0; i < len; i++) {
      char c = var.charAt(i);
      char s;
      if ((c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')) {
        s = c;
      } else {
        s = '_';
      }
      safe.append(s);
    }
    return safe.toString();
  }

  void addEnvironment(Properties env, String nameVals) {
    // encoding "a=b c=d" from StreamJob
    if (nameVals == null) return;
    String[] nv = nameVals.split(" ");
    for (int i = 0; i < nv.length; i++) {
      String[] pair = nv[i].split("=", 2);
      if (pair.length != 2) {
        logprintln("Skip ev entry:" + nv[i]);
      } else {
        envPut(env, pair[0], pair[1]);
      }
    }
  }

  void envPut(Properties env, String name, String value) {
    if (debug_) {
      logprintln("Add  ev entry:" + name + "=" + value);
    }
    env.put(name, value);
  }

  /** .. and if successful: delete the task log */
  void appendLogToJobLog(String status) {
    if (jobLog_ == null) {
      return; // not using a common joblog
    }
    if (log_ != null) {
      StreamUtil.exec("/bin/rm " + LOGNAME, log_);
    }
  }

  void startOutputThreads(OutputCollector output, Reporter reporter) {
    outThread_ = new MROutputThread(output, reporter);
    outThread_.start();
    errThread_.setReporter(reporter);
  }

  void waitOutputThreads() {
    try {
      int exitVal = sim.waitFor();
      // how'd it go?
      if (exitVal != 0) {
        if (nonZeroExitIsFailure_) {
          throw new RuntimeException("PipeMapRed.waitOutputThreads(): subprocess failed with code "
                                     + exitVal);
        } else {
          logprintln("PipeMapRed.waitOutputThreads(): subprocess exited with code " + exitVal
                     + " in " + PipeMapRed.class.getName());
        }
      }
      if (outThread_ != null) {
        outThread_.join(joinDelay_);
      }
      if (errThread_ != null) {
        errThread_.join(joinDelay_);
      }
    } catch (InterruptedException e) {
      //ignore
    }
  }

  /**
   * Split a line into key and value. Assume the delimitor is a tab.
   * @param line: a byte array of line containing UTF-8 bytes
   * @param key: key of a record
   * @param val: value of a record
   * @throws IOException
   */
  void splitKeyVal(byte[] line, int length, Text key, Text val)
  throws IOException {
    int pos = UTF8ByteArrayUtils.findNthByte(line, 0, length,
                (byte)this.getFieldSeparator(), this.getNumOfKeyFields());
    try {
      if (pos == -1) {
        key.set(line, 0, length);
        val.set("");
      } else {
        UTF8ByteArrayUtils.splitKeyVal(line, 0, length, key, val, pos);
      }
    } catch (CharacterCodingException e) {
      LOG.warn(StringUtils.stringifyException(e));
    }
  }

  class MROutputThread extends Thread {

    MROutputThread(OutputCollector output, Reporter reporter) {
      setDaemon(true);
      this.output = output;
      this.reporter = reporter;
    }

    public void run() {
      LineReader lineReader = null;
      try {
        Text key = new Text();
        Text val = new Text();
        Text line = new Text();
        lineReader = new LineReader((InputStream)clientIn_, job_);
        // 3/4 Tool to Hadoop
        while (lineReader.readLine(line) > 0) {
          answer = line.getBytes();
          splitKeyVal(answer, line.getLength(), key, val);
          output.collect(key, val);
          line.clear();
          numRecWritten_++;
          long now = System.currentTimeMillis();
          if (now-lastStdoutReport > reporterOutDelay_) {
            lastStdoutReport = now;
            String hline = "Records R/W=" + numRecRead_ + "/" + numRecWritten_;
            reporter.setStatus(hline);
            logprintln(hline);
            logflush();
          }
        }
        if (lineReader != null) {
          lineReader.close();
        }
        if (clientIn_ != null) {
          clientIn_.close();
          clientIn_ = null;
          LOG.info("MROutputThread done");
        }
      } catch (Throwable th) {
        outerrThreadsThrowable = th;
        LOG.warn(StringUtils.stringifyException(th));
        try {
          if (lineReader != null) {
            lineReader.close();
          }
          if (clientIn_ != null) {
            clientIn_.close();
            clientIn_ = null;
          }
        } catch (IOException io) {
          LOG.info(StringUtils.stringifyException(io));
        }
      }
    }

    OutputCollector output;
    Reporter reporter;
    byte[] answer;
    long lastStdoutReport = 0;
    
  }

  class MRErrorThread extends Thread {

    public MRErrorThread() {
      setDaemon(true);
    }
    
    public void setReporter(Reporter reporter) {
      this.reporter = reporter;
    }
      
    public void run() {
      Text line = new Text();
      LineReader lineReader = null;
      try {
        lineReader = new LineReader((InputStream)clientErr_, job_);
        while (lineReader.readLine(line) > 0) {
          System.err.println(line.toString());
          long now = System.currentTimeMillis(); 
          if (reporter != null && now-lastStderrReport > reporterErrDelay_) {
            lastStderrReport = now;
            reporter.progress();
          }
          line.clear();
        }
        if (lineReader != null) {
          lineReader.close();
        }
        if (clientErr_ != null) {
          clientErr_.close();
          clientErr_ = null;
          LOG.info("MRErrorThread done");
        }
      } catch (Throwable th) {
        outerrThreadsThrowable = th;
        LOG.warn(StringUtils.stringifyException(th));
        try {
          if (lineReader != null) {
            lineReader.close();
          }
          if (clientErr_ != null) {
            clientErr_.close();
            clientErr_ = null;
          }
        } catch (IOException io) {
          LOG.info(StringUtils.stringifyException(io));
        }
      }
    }
    long lastStderrReport = 0;
    volatile Reporter reporter;
  }

  public void mapRedFinished() {
    logprintln("mapRedFinished");
    try {
      if (!doPipe_) return;
      try {
        if (clientOut_ != null) {
          clientOut_.close();
        }
      } catch (IOException io) {
      }
      waitOutputThreads();
      if (sim != null) sim.destroy();
    } catch (RuntimeException e) {
      logStackTrace(e);
      throw e;
    }
    if (debugFailLate_) {
      throw new RuntimeException("debugFailLate_");
    }
  }

  void maybeLogRecord() {
    if (numRecRead_ >= nextRecReadLog_) {
      String info = numRecInfo();
      logprintln(info);
      logflush();
      if (nextRecReadLog_ < 100000) {
	  nextRecReadLog_ *= 10;
      } else {
	  nextRecReadLog_ += 100000;
      }
    }
  }

  public String getContext() {

    String s = numRecInfo() + "\n";
    s += "minRecWrittenToEnableSkip_=" + minRecWrittenToEnableSkip_ + " ";
    s += "LOGNAME=" + LOGNAME + "\n";
    s += envline("HOST");
    s += envline("USER");
    s += envline("HADOOP_USER");
    //s += envline("PWD"); // =/home/crawler/hadoop/trunk
    s += "last Hadoop input: |" + mapredKey_ + "|\n";
    if (outThread_ != null) {
      s += "last tool output: |" + outThread_.answer + "|\n";
    }
    s += "Date: " + new Date() + "\n";
    // s += envline("HADOOP_HOME");
    // s += envline("REMOTE_HOST");
    return s;
  }

  String envline(String var) {
    return var + "=" + StreamUtil.env().get(var) + "\n";
  }

  String numRecInfo() {
    long elapsed = (System.currentTimeMillis() - startTime_) / 1000;
    return "R/W/S=" + numRecRead_ + "/" + numRecWritten_ + "/" + numRecSkipped_ + " in:"
      + safeDiv(numRecRead_, elapsed) + " [rec/s]" + " out:" + safeDiv(numRecWritten_, elapsed)
      + " [rec/s]";
  }

  String safeDiv(long n, long d) {
    return (d == 0) ? "NA" : "" + n / d + "=" + n + "/" + d;
  }

  String logFailure(Exception e) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    e.printStackTrace(pw);
    String msg = "log:" + jobLog_ + "\n" + getContext() + sw + "\n";
    logprintln(msg);
    return msg;
  }

  /**
   * Write a value to the output stream using UTF-8 encoding
   * @param value output value
   * @throws IOException
   */
  void write(Object value) throws IOException {
    byte[] bval;
    int valSize;
    if (value instanceof BytesWritable) {
      BytesWritable val = (BytesWritable) value;
      bval = val.get();
      valSize = val.getSize();
    } else if (value instanceof Text) {
      Text val = (Text) value;
      bval = val.getBytes();
      valSize = val.getLength();
    } else {
      String sval = value.toString();
      bval = sval.getBytes("UTF-8");
      valSize = bval.length;
    }
    clientOut_.write(bval, 0, valSize);
  }

  long startTime_;
  long numRecRead_ = 0;
  long numRecWritten_ = 0;
  long numRecSkipped_ = 0;
  long nextRecReadLog_ = 1;

  
  long minRecWrittenToEnableSkip_ = Long.MAX_VALUE;

  long reporterOutDelay_ = 10*1000L; 
  long reporterErrDelay_ = 10*1000L; 
  long joinDelay_;
  JobConf job_;
  FileSystem fs_;

  boolean doPipe_;
  boolean debug_;
  boolean debugFailEarly_;
  boolean debugFailDuring_;
  boolean debugFailLate_;

  boolean nonZeroExitIsFailure_;
  
  Process sim;
  MROutputThread outThread_;
  String jobLog_;
  MRErrorThread errThread_;
  DataOutputStream clientOut_;
  DataInputStream clientErr_;
  DataInputStream clientIn_;

  // set in PipeMapper/PipeReducer subclasses
  String mapredKey_;
  int numExceptions_;
  StreamUtil.TaskId taskId_;

  protected volatile Throwable outerrThreadsThrowable;

  String LOGNAME;
  PrintStream log_;

  protected char mapOutputFieldSeparator = '\t';
  protected char reduceOutFieldSeparator = '\t';
  protected int numOfMapOutputKeyFields = 1;
  protected int numOfMapOutputPartitionFields = 1;
  protected int numOfReduceOutputKeyFields = 1;
  
}
