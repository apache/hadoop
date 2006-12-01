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
import java.net.Socket;
import java.net.URI;
import java.nio.channels.*;
import java.nio.charset.CharacterCodingException;
import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.Iterator;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Properties;
import java.util.regex.*;

import org.apache.commons.logging.*;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.PhasedFileSystem;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.fs.LocalFileSystem;
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
  boolean getUseSideEffect() {
    return false;
  }

  abstract boolean getDoPipe();

  /**
   * @returns how many TABS before the end of the key part
   * usually: 1 or "ALL"
   * used for tool output of both Map and Reduce
   * configured via tool's argv: splitKeyVal=ALL or 1..
   * although it is interpreted here, not by tool
   */
  int getKeyColsFromPipeCommand(String cmd) {
    String key = getKeyColPropName();
    Pattern kcPat = Pattern.compile(".*" + key + "=([^\\s]*).*");
    Matcher match = kcPat.matcher(cmd);
    String kc;
    if (!match.matches()) {
      kc = null;
    } else {
      kc = match.group(1);
    }

    int cols;
    if (kc == null) {
      // default value is 1 and the Stream applications could instead
      // add/remove the \t separator on lines to get the same effect as value 0, 1, ALL
      cols = 1;
    } else if (kc.equals("ALL")) {
      cols = ALL_COLS;
    } else {
      try {
        cols = Integer.parseInt(kc);
      } catch (NumberFormatException nf) {
        cols = Integer.MAX_VALUE;
      }
    }

    System.out.println("getKeyColsFromPipeCommand:" + key + " parse:" + cols + " from cmd=" + cmd);

    return cols;
  }

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

  OutputStream getURIOutputStream(URI uri, boolean allowSocket) throws IOException {
    final String SOCKET = "socket";
    if (uri.getScheme().equals(SOCKET)) {
      if (!allowSocket) {
        throw new IOException(SOCKET + " not allowed on outputstream " + uri);
      }
      final Socket sock = new Socket(uri.getHost(), uri.getPort());
      OutputStream out = new FilterOutputStream(sock.getOutputStream()) {
        public void close() throws IOException {
          sock.close();
          super.close();
        }
      };
      return out;
    } else {
      // a FSDataOutputStreamm, localFS or HDFS.
      // localFS file may be set up as a FIFO.
      return sideFs_.create(new Path(uri.getSchemeSpecificPart()));
    }
  }

  String getSideEffectFileName() {
    FileSplit split = StreamUtil.getCurrentSplit(job_);
    String leaf = split.getPath().getName();
    if (split.getStart() == 0) {
      return leaf;
    } else {
      return new FileSplit(new Path(leaf), split.getStart(), split.getLength()).toString();
    }
  }

  public void configure(JobConf job) {
    try {
      String argv = getPipeCommand(job);

      keyCols_ = getKeyColsFromPipeCommand(argv);

      debug_ = (job.get("stream.debug") != null);
      if (debug_) {
        System.out.println("PipeMapRed: stream.debug=true");
      }

      joinDelay_ = job.getLong("stream.joindelay.milli", 0);

      job_ = job;
      fs_ = FileSystem.get(job_);
      if (job_.getBoolean("stream.sideoutput.localfs", false)) {
        //sideFs_ = new LocalFileSystem(job_);
        sideFs_ = FileSystem.getNamed("local", job_);
      } else {
        sideFs_ = fs_;
      }

      if (debug_) {
        System.out.println("kind   :" + this.getClass());
        System.out.println("split  :" + StreamUtil.getCurrentSplit(job_));
        System.out.println("fs     :" + fs_.toString());
        System.out.println("sideFs :" + sideFs_.toString());
      }

      doPipe_ = getDoPipe();
      if (!doPipe_) return;

      setStreamJobDetails(job);
      setStreamProperties();

      if (debugFailEarly_) {
        throw new RuntimeException("debugFailEarly_");
      }
      String[] argvSplit = splitArgs(argv);
      String prog = argvSplit[0];
      String userdir = System.getProperty("user.dir");
      if (new File(prog).isAbsolute()) {
        // we don't own it. Hope it is executable
      } else {
        new MustangFile(prog).setExecutable(true, true);
      }

      if (job_.getInputValueClass().equals(BytesWritable.class)) {
        // TODO expose as separate config:
        // job or semistandard inputformat property
        optUseKey_ = false;
      }

      optSideEffect_ = getUseSideEffect();

      if (optSideEffect_) {
        // during work: use a completely unique filename to avoid HDFS namespace conflicts
        // after work: rename to a filename that depends only on the workload (the FileSplit)
        //   it's a friendly name and in case of reexecution it will clobber. 
        // reexecution can be due to: other job, failed task and speculative task
        // See StreamJob.setOutputSpec(): if reducerNone_ aka optSideEffect then: 
        // client has renamed outputPath and saved the argv's original output path as:
        if (useSingleSideOutputURI_) {
          finalOutputURI = new URI(sideOutputURI_);
          sideEffectPathFinal_ = null; // in-place, no renaming to final
        } else {
          sideFs_ = new PhasedFileSystem(sideFs_, job);
          String sideOutputPath = job_.get("stream.sideoutput.dir"); // was: job_.getOutputPath() 
          String fileName = getSideEffectFileName(); // see HADOOP-444 for rationale
          sideEffectPathFinal_ = new Path(sideOutputPath, fileName);
          finalOutputURI = new URI(sideEffectPathFinal_.toString()); // implicit dfs: 
        }
        // apply default scheme
        if(finalOutputURI.getScheme() == null) {
          finalOutputURI = new URI("file", finalOutputURI.getSchemeSpecificPart(), null);
        }
        boolean allowSocket = useSingleSideOutputURI_;
        sideEffectOut_ = getURIOutputStream(finalOutputURI, allowSocket);
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
          finder.prependPathComponent(".");
          File f = finder.getAbsolutePath(argvSplit[0]);
          if (f != null) {
              argvSplit[0] = f.getAbsolutePath();
          }
          f = null;
      }
      logprintln("PipeMapRed exec " + Arrays.asList(argvSplit));
      logprintln("sideEffectURI_=" + finalOutputURI);

      Environment childEnv = (Environment) StreamUtil.env().clone();
      addJobConfToEnvironment(job_, childEnv);
      addEnvironment(childEnv, job_.get("stream.addenvironment"));
      sim = Runtime.getRuntime().exec(argvSplit, childEnv.toArray());

      /* // This way required jdk1.5
       Builder processBuilder = new ProcessBuilder(argvSplit);
       Map<String, String> env = processBuilder.environment();
       addEnvironment(env, job_.get("stream.addenvironment"));
       sim = processBuilder.start();
       */

      clientOut_ = new DataOutputStream(new BufferedOutputStream(sim.getOutputStream()));
      clientIn_ = new DataInputStream(new BufferedInputStream(sim.getInputStream()));
      clientErr_ = new DataInputStream(new BufferedInputStream(sim.getErrorStream()));
      startTime_ = System.currentTimeMillis();

    } catch (Exception e) {
      logStackTrace(e);
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
    debugFailEarly_ = isDebugFail("early");
    debugFailDuring_ = isDebugFail("during");
    debugFailLate_ = isDebugFail("late");

    sideOutputURI_ = job_.get("stream.sideoutput.uri");
    useSingleSideOutputURI_ = (sideOutputURI_ != null);
  }

  boolean isDebugFail(String kind) {
    String execidlist = job_.get("stream.debugfail.reexec." + kind);
    if (execidlist == null) {
      return false;
    }
    String[] e = execidlist.split(",");
    for (int i = 0; i < e.length; i++) {
      int ei = Integer.parseInt(e[i]);
      if (taskId_.execid == ei) {
        return true;
      }
    }
    return false;
  }

  void setStreamProperties() {
    String s = System.getProperty("stream.port");
    if (s != null) {
      reportPortPlusOne_ = Integer.parseInt(s);
    }
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
    Iterator it = conf.entries();
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
    // TODO socket-based aggregator (in JobTrackerInfoServer)
  }

  void startOutputThreads(OutputCollector output, Reporter reporter) {
    outThread_ = new MROutputThread(output, reporter);
    outThread_.start();
    errThread_ = new MRErrorThread(reporter);
    errThread_.start();
  }

  void waitOutputThreads() {
    try {
      sim.waitFor();
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
  void splitKeyVal(byte[] line, Text key, Text val) throws IOException {
    int pos = -1;
    if (keyCols_ != ALL_COLS) {
      pos = UTF8ByteArrayUtils.findTab(line);
    }
    try {
      if (pos == -1) {
        key.set(line);
        val.set("");
      } else {
        UTF8ByteArrayUtils.splitKeyVal(line, key, val, pos);
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
      try {
        Text key = new Text();
        Text val = new Text();
        // 3/4 Tool to Hadoop
        while ((answer = UTF8ByteArrayUtils.readLine((InputStream) clientIn_)) != null) {
          // 4/4 Hadoop out
          if (optSideEffect_) {
            sideEffectOut_.write(answer);
            sideEffectOut_.write('\n');
            sideEffectOut_.flush();
          } else {
            splitKeyVal(answer, key, val);
            output.collect(key, val);
          }
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
      } catch (IOException io) {
        io.printStackTrace(log_);
      }
      logprintln("MROutputThread done");
    }

    OutputCollector output;
    Reporter reporter;
    byte[] answer;
    long lastStdoutReport = 0;
    
  }

  class MRErrorThread extends Thread {

    public MRErrorThread(Reporter reporter) {
      this.reporter = reporter;
      setDaemon(true);
    }

    public void run() {
      byte[] line;
      try {
        long num = 0;
        while ((line = UTF8ByteArrayUtils.readLine((InputStream) clientErr_)) != null) {
          num++;
          String lineStr = new String(line, "UTF-8");
          logprintln(lineStr);
          long now = System.currentTimeMillis(); 
          if (num < 20 || (now-lastStderrReport > reporterErrDelay_)) {
            lastStderrReport = now;
            String hline = "MRErr: " + lineStr;
            System.err.println(hline);
            reporter.setStatus(hline);
          }
        }
      } catch (IOException io) {
        logStackTrace(io);
      }
    }
    long lastStderrReport = 0;
    Reporter reporter;
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
      try {
        if (optSideEffect_) {
          logprintln("closing " + finalOutputURI);
          if (sideEffectOut_ != null) sideEffectOut_.close();
          logprintln("closed  " + finalOutputURI);
          if ( ! useSingleSideOutputURI_) {
            ((PhasedFileSystem)sideFs_).commit(); 
          }
        }
      } catch (IOException io) {
        io.printStackTrace();
      }
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
      System.err.println(info);
      //nextRecReadLog_ *= 10;
      nextRecReadLog_ += 100;
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
    long total = numRecRead_ + numRecWritten_ + numRecSkipped_;
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
   * Write a writable value to the output stream using UTF-8 encoding
   * @param value output value
   * @throws IOException
   */
  void write(Writable value) throws IOException {
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

  int keyCols_;
  final static int ALL_COLS = Integer.MAX_VALUE;

  long reporterOutDelay_ = 10*1000L; 
  long reporterErrDelay_ = 10*1000L; 
  long joinDelay_;
  JobConf job_;
  FileSystem fs_;
  FileSystem sideFs_;

  // generic MapRed parameters passed on by hadoopStreaming
  int reportPortPlusOne_;

  boolean doPipe_;
  boolean debug_;
  boolean debugFailEarly_;
  boolean debugFailDuring_;
  boolean debugFailLate_;

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

  boolean optUseKey_ = true;

  private boolean optSideEffect_;
  private URI finalOutputURI;
  private Path sideEffectPathFinal_;

  private boolean useSingleSideOutputURI_;
  private String sideOutputURI_;

  private OutputStream sideEffectOut_;

  String LOGNAME;
  PrintStream log_;

}
