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

package org.apache.hadoop.mapred;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.Flushable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SecureIOUtils;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.util.ProcessTree;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.log4j.Appender;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.base.Charsets;

/**
 * A simple logger to handle the task-specific user logs.
 * This class uses the system property <code>hadoop.log.dir</code>.
 * 
 */
@InterfaceAudience.Private
public class TaskLog {
  private static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(TaskLog.class);

  static final String USERLOGS_DIR_NAME = "userlogs";

  private static final File LOG_DIR = 
    new File(getBaseLogDir(), USERLOGS_DIR_NAME).getAbsoluteFile();
  
  // localFS is set in (and used by) writeToIndexFile()
  static LocalFileSystem localFS = null;
  
  public static String getMRv2LogDir() {
    return System.getProperty(YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR);
  }
  
  public static File getTaskLogFile(TaskAttemptID taskid, boolean isCleanup,
      LogName filter) {
    if (getMRv2LogDir() != null) {
      return new File(getMRv2LogDir(), filter.toString());
    } else {
      return new File(getAttemptDir(taskid, isCleanup), filter.toString());
    }
  }

  static File getRealTaskLogFileLocation(TaskAttemptID taskid,
      boolean isCleanup, LogName filter) {
    LogFileDetail l;
    try {
      l = getLogFileDetail(taskid, filter, isCleanup);
    } catch (IOException ie) {
      LOG.error("getTaskLogFileDetail threw an exception " + ie);
      return null;
    }
    return new File(l.location, filter.toString());
  }
  private static class LogFileDetail {
    final static String LOCATION = "LOG_DIR:";
    String location;
    long start;
    long length;
  }
  
  private static LogFileDetail getLogFileDetail(TaskAttemptID taskid, 
                                                LogName filter,
                                                boolean isCleanup) 
  throws IOException {
    File indexFile = getIndexFile(taskid, isCleanup);
    BufferedReader fis = new BufferedReader(new InputStreamReader(
      SecureIOUtils.openForRead(indexFile, obtainLogDirOwner(taskid), null),
      Charsets.UTF_8));
    //the format of the index file is
    //LOG_DIR: <the dir where the task logs are really stored>
    //stdout:<start-offset in the stdout file> <length>
    //stderr:<start-offset in the stderr file> <length>
    //syslog:<start-offset in the syslog file> <length>
    LogFileDetail l = new LogFileDetail();
    String str = null;
    try {
      str = fis.readLine();
      if (str == null) { // the file doesn't have anything
        throw new IOException("Index file for the log of " + taskid
            + " doesn't exist.");
      }
      l.location = str.substring(str.indexOf(LogFileDetail.LOCATION)
          + LogFileDetail.LOCATION.length());
      // special cases are the debugout and profile.out files. They are
      // guaranteed
      // to be associated with each task attempt since jvm reuse is disabled
      // when profiling/debugging is enabled
      if (filter.equals(LogName.DEBUGOUT) || filter.equals(LogName.PROFILE)) {
        l.length = new File(l.location, filter.toString()).length();
        l.start = 0;
        fis.close();
        return l;
      }
      str = fis.readLine();
      while (str != null) {
        // look for the exact line containing the logname
        if (str.contains(filter.toString())) {
          str = str.substring(filter.toString().length() + 1);
          String[] startAndLen = str.split(" ");
          l.start = Long.parseLong(startAndLen[0]);
          l.length = Long.parseLong(startAndLen[1]);
          break;
        }
        str = fis.readLine();
      }
      fis.close();
      fis = null;
    } finally {
      IOUtils.cleanupWithLogger(LOG, fis);
    }
    return l;
  }
  
  private static File getTmpIndexFile(TaskAttemptID taskid, boolean isCleanup) {
    return new File(getAttemptDir(taskid, isCleanup), "log.tmp");
  }

  static File getIndexFile(TaskAttemptID taskid, boolean isCleanup) {
    return new File(getAttemptDir(taskid, isCleanup), "log.index");
  }

  /**
   * Obtain the owner of the log dir. This is 
   * determined by checking the job's log directory.
   */
  static String obtainLogDirOwner(TaskAttemptID taskid) throws IOException {
    Configuration conf = new Configuration();
    FileSystem raw = FileSystem.getLocal(conf).getRaw();
    Path jobLogDir = new Path(getJobDir(taskid.getJobID()).getAbsolutePath());
    FileStatus jobStat = raw.getFileStatus(jobLogDir);
    return jobStat.getOwner();
  }

  static String getBaseLogDir() {
    return System.getProperty("hadoop.log.dir");
  }

  static File getAttemptDir(TaskAttemptID taskid, boolean isCleanup) {
    String cleanupSuffix = isCleanup ? ".cleanup" : "";
    return new File(getJobDir(taskid.getJobID()), taskid + cleanupSuffix);
  }
  private static long prevOutLength;
  private static long prevErrLength;
  private static long prevLogLength;
  
  private static synchronized 
  void writeToIndexFile(String logLocation,
                        boolean isCleanup) throws IOException {
    // To ensure atomicity of updates to index file, write to temporary index
    // file first and then rename.
    File tmpIndexFile = getTmpIndexFile(currentTaskid, isCleanup);

    BufferedOutputStream bos = null;
    DataOutputStream dos = null;
    try{
      bos = new BufferedOutputStream(
          SecureIOUtils.createForWrite(tmpIndexFile, 0644));
      dos = new DataOutputStream(bos);
      //the format of the index file is
      //LOG_DIR: <the dir where the task logs are really stored>
      //STDOUT: <start-offset in the stdout file> <length>
      //STDERR: <start-offset in the stderr file> <length>
      //SYSLOG: <start-offset in the syslog file> <length>   

      dos.writeBytes(LogFileDetail.LOCATION + logLocation + "\n"
          + LogName.STDOUT.toString() + ":");
      dos.writeBytes(Long.toString(prevOutLength) + " ");
      dos.writeBytes(Long.toString(new File(logLocation, LogName.STDOUT
          .toString()).length() - prevOutLength)
          + "\n" + LogName.STDERR + ":");
      dos.writeBytes(Long.toString(prevErrLength) + " ");
      dos.writeBytes(Long.toString(new File(logLocation, LogName.STDERR
          .toString()).length() - prevErrLength)
          + "\n" + LogName.SYSLOG.toString() + ":");
      dos.writeBytes(Long.toString(prevLogLength) + " ");
      dos.writeBytes(Long.toString(new File(logLocation, LogName.SYSLOG
          .toString()).length() - prevLogLength)
          + "\n");
      dos.close();
      dos = null;
      bos.close();
      bos = null;
    } finally {
      IOUtils.cleanupWithLogger(LOG, dos, bos);
    }

    File indexFile = getIndexFile(currentTaskid, isCleanup);
    Path indexFilePath = new Path(indexFile.getAbsolutePath());
    Path tmpIndexFilePath = new Path(tmpIndexFile.getAbsolutePath());

    if (localFS == null) {// set localFS once
      localFS = FileSystem.getLocal(new Configuration());
    }
    localFS.rename (tmpIndexFilePath, indexFilePath);
  }
  private static void resetPrevLengths(String logLocation) {
    prevOutLength = new File(logLocation, LogName.STDOUT.toString()).length();
    prevErrLength = new File(logLocation, LogName.STDERR.toString()).length();
    prevLogLength = new File(logLocation, LogName.SYSLOG.toString()).length();
  }
  private volatile static TaskAttemptID currentTaskid = null;

  @SuppressWarnings("unchecked")
  public synchronized static void syncLogs(String logLocation, 
                                           TaskAttemptID taskid,
                                           boolean isCleanup) 
  throws IOException {
    System.out.flush();
    System.err.flush();
    if (currentTaskid != taskid) {
      currentTaskid = taskid;
      resetPrevLengths(logLocation);
    }
    writeToIndexFile(logLocation, isCleanup);
  }

  public static synchronized void syncLogsShutdown(
    ScheduledExecutorService scheduler) 
  {
    // flush standard streams
    //
    System.out.flush();
    System.err.flush();

    if (scheduler != null) {
      scheduler.shutdownNow();
    }

    // flush & close all appenders
    LogManager.shutdown(); 
  }

  @SuppressWarnings("unchecked")
  public static synchronized void syncLogs() {
    // flush standard streams
    //
    System.out.flush();
    System.err.flush();

    // flush flushable appenders
    //
    final Logger rootLogger = Logger.getRootLogger();
    flushAppenders(rootLogger);
    final Enumeration<Logger> allLoggers = rootLogger.getLoggerRepository().
      getCurrentLoggers();
    while (allLoggers.hasMoreElements()) {
      final Logger l = allLoggers.nextElement();
      flushAppenders(l);
    }
  }

  @SuppressWarnings("unchecked")
  private static void flushAppenders(Logger l) {
    final Enumeration<Appender> allAppenders = l.getAllAppenders();
    while (allAppenders.hasMoreElements()) {
      final Appender a = allAppenders.nextElement();
      if (a instanceof Flushable) {
        try {
          ((Flushable) a).flush();
        } catch (IOException ioe) {
          System.err.println(a + ": Failed to flush!"
            + StringUtils.stringifyException(ioe));
        }
      }
    }
  }

  public static ScheduledExecutorService createLogSyncer() {
    final ScheduledExecutorService scheduler =
        HadoopExecutors.newSingleThreadScheduledExecutor(
            new ThreadFactory() {
              @Override
              public Thread newThread(Runnable r) {
                final Thread t = Executors.defaultThreadFactory().newThread(r);
                t.setDaemon(true);
                t.setName("Thread for syncLogs");
                return t;
              }
            });
    ShutdownHookManager.get().addShutdownHook(new Runnable() {
      @Override
      public void run() {
        TaskLog.syncLogsShutdown(scheduler);
      }
    }, 50);
    scheduler.scheduleWithFixedDelay(
        new Runnable() {
          @Override
          public void run() {
            TaskLog.syncLogs();
          }
        }, 0L, 5L, TimeUnit.SECONDS);
    return scheduler;
  }

  /**
   * The filter for userlogs.
   */
  @InterfaceAudience.Private
  public enum LogName {
    /** Log on the stdout of the task. */
    STDOUT ("stdout"),

    /** Log on the stderr of the task. */
    STDERR ("stderr"),
    
    /** Log on the map-reduce system logs of the task. */
    SYSLOG ("syslog"),
    
    /** The java profiler information. */
    PROFILE ("profile.out"),
    
    /** Log the debug script's stdout  */
    DEBUGOUT ("debugout");
        
    private String prefix;
    
    private LogName(String prefix) {
      this.prefix = prefix;
    }
    
    @Override
    public String toString() {
      return prefix;
    }
  }

  public static class Reader extends InputStream {
    private long bytesRemaining;
    private FileInputStream file;

    /**
     * Read a log file from start to end positions. The offsets may be negative,
     * in which case they are relative to the end of the file. For example,
     * Reader(taskid, kind, 0, -1) is the entire file and 
     * Reader(taskid, kind, -4197, -1) is the last 4196 bytes. 
     * @param taskid the id of the task to read the log file for
     * @param kind the kind of log to read
     * @param start the offset to read from (negative is relative to tail)
     * @param end the offset to read upto (negative is relative to tail)
     * @param isCleanup whether the attempt is cleanup attempt or not
     * @throws IOException
     */
    public Reader(TaskAttemptID taskid, LogName kind, 
                  long start, long end, boolean isCleanup) throws IOException {
      // find the right log file
      LogFileDetail fileDetail = getLogFileDetail(taskid, kind, isCleanup);
      // calculate the start and stop
      long size = fileDetail.length;
      if (start < 0) {
        start += size + 1;
      }
      if (end < 0) {
        end += size + 1;
      }
      start = Math.max(0, Math.min(start, size));
      end = Math.max(0, Math.min(end, size));
      start += fileDetail.start;
      end += fileDetail.start;
      bytesRemaining = end - start;
      String owner = obtainLogDirOwner(taskid);
      file = SecureIOUtils.openForRead(new File(fileDetail.location, kind.toString()), 
          owner, null);
      // skip upto start
      long pos = 0;
      while (pos < start) {
        long result = file.skip(start - pos);
        if (result < 0) {
          bytesRemaining = 0;
          break;
        }
        pos += result;
      }
    }
    
    @Override
    public int read() throws IOException {
      int result = -1;
      if (bytesRemaining > 0) {
        bytesRemaining -= 1;
        result = file.read();
      }
      return result;
    }
    
    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
      length = (int) Math.min(length, bytesRemaining);
      int bytes = file.read(buffer, offset, length);
      if (bytes > 0) {
        bytesRemaining -= bytes;
      }
      return bytes;
    }
    
    @Override
    public int available() throws IOException {
      return (int) Math.min(bytesRemaining, file.available());
    }

    @Override
    public void close() throws IOException {
      file.close();
    }
  }

  private static final String bashCommand = "bash";
  private static final String tailCommand = "tail";
  
  /**
   * Get the desired maximum length of task's logs.
   * @param conf the job to look in
   * @return the number of bytes to cap the log files at
   */
  public static long getTaskLogLength(JobConf conf) {
   return getTaskLogLimitBytes(conf);
  }

  public static long getTaskLogLimitBytes(Configuration conf) {
    return conf.getLong(JobContext.TASK_USERLOG_LIMIT, 0) * 1024;
  }

  
  /**
   * Wrap a command in a shell to capture stdout and stderr to files.
   * Setup commands such as setting memory limit can be passed which 
   * will be executed before exec.
   * If the tailLength is 0, the entire output will be saved.
   * @param setup The setup commands for the execed process.
   * @param cmd The command and the arguments that should be run
   * @param stdoutFilename The filename that stdout should be saved to
   * @param stderrFilename The filename that stderr should be saved to
   * @param tailLength The length of the tail to be saved.
   * @param useSetsid Should setsid be used in the command or not.
   * @return the modified command that should be run
   */
  public static List<String> captureOutAndError(List<String> setup,
                                                List<String> cmd, 
                                                File stdoutFilename,
                                                File stderrFilename,
                                                long tailLength,
                                                boolean useSetsid
                                               ) throws IOException {
    List<String> result = new ArrayList<String>(3);
    result.add(bashCommand);
    result.add("-c");
    String mergedCmd = buildCommandLine(setup, cmd, stdoutFilename,
                                                    stderrFilename, tailLength, 
                                                    useSetsid);
    result.add(mergedCmd);
    return result;
  }
  
  /**
   * Construct the command line for running the task JVM
   * @param setup The setup commands for the execed process.
   * @param cmd The command and the arguments that should be run
   * @param stdoutFilename The filename that stdout should be saved to
   * @param stderrFilename The filename that stderr should be saved to
   * @param tailLength The length of the tail to be saved.
   * @return the command line as a String
   * @throws IOException
   */
  static String buildCommandLine(List<String> setup, List<String> cmd, 
                                      File stdoutFilename,
                                      File stderrFilename,
                                      long tailLength, 
                                      boolean useSetsid)
                                throws IOException {
    
    String stdout = FileUtil.makeShellPath(stdoutFilename);
    String stderr = FileUtil.makeShellPath(stderrFilename);    
    StringBuffer mergedCmd = new StringBuffer();
    
    // Export the pid of taskJvm to env variable JVM_PID.
    // Currently pid is not used on Windows
    if (!Shell.WINDOWS) {
      mergedCmd.append(" export JVM_PID=`echo $$` ; ");
    }

    if (setup != null && setup.size() > 0) {
      mergedCmd.append(addCommand(setup, false));
      mergedCmd.append(";");
    }
    if (tailLength > 0) {
      mergedCmd.append("(");
    } else if(ProcessTree.isSetsidAvailable && useSetsid &&
        !Shell.WINDOWS) {
      mergedCmd.append("exec setsid ");
    } else {
      mergedCmd.append("exec ");
    }
    mergedCmd.append(addCommand(cmd, true));
    mergedCmd.append(" < /dev/null ");
    if (tailLength > 0) {
      mergedCmd.append(" | ");
      mergedCmd.append(tailCommand);
      mergedCmd.append(" -c ");
      mergedCmd.append(tailLength);
      mergedCmd.append(" >> ");
      mergedCmd.append(stdout);
      mergedCmd.append(" ; exit $PIPESTATUS ) 2>&1 | ");
      mergedCmd.append(tailCommand);
      mergedCmd.append(" -c ");
      mergedCmd.append(tailLength);
      mergedCmd.append(" >> ");
      mergedCmd.append(stderr);
      mergedCmd.append(" ; exit $PIPESTATUS");
    } else {
      mergedCmd.append(" 1>> ");
      mergedCmd.append(stdout);
      mergedCmd.append(" 2>> ");
      mergedCmd.append(stderr);
    }
    return mergedCmd.toString();
  }
  
  /**
   * Construct the command line for running the debug script
   * @param cmd The command and the arguments that should be run
   * @param stdoutFilename The filename that stdout should be saved to
   * @param stderrFilename The filename that stderr should be saved to
   * @param tailLength The length of the tail to be saved.
   * @return the command line as a String
   * @throws IOException
   */
  static String buildDebugScriptCommandLine(List<String> cmd, String debugout)
  throws IOException {
    StringBuilder mergedCmd = new StringBuilder();
    mergedCmd.append("exec ");
    boolean isExecutable = true;
    for(String s: cmd) {
      if (isExecutable) {
        // the executable name needs to be expressed as a shell path for the  
        // shell to find it.
        mergedCmd.append(FileUtil.makeShellPath(new File(s)));
        isExecutable = false; 
      } else {
        mergedCmd.append(s);
      }
      mergedCmd.append(" ");
    }
    mergedCmd.append(" < /dev/null ");
    mergedCmd.append(" >");
    mergedCmd.append(debugout);
    mergedCmd.append(" 2>&1 ");
    return mergedCmd.toString();
  }
  /**
   * Add quotes to each of the command strings and
   * return as a single string 
   * @param cmd The command to be quoted
   * @param isExecutable makes shell path if the first 
   * argument is executable
   * @return returns The quoted string. 
   * @throws IOException
   */
  public static String addCommand(List<String> cmd, boolean isExecutable) 
  throws IOException {
    StringBuffer command = new StringBuffer();
    for(String s: cmd) {
    	command.append('\'');
      if (isExecutable) {
        // the executable name needs to be expressed as a shell path for the  
        // shell to find it.
    	  command.append(FileUtil.makeShellPath(new File(s)));
        isExecutable = false; 
      } else {
    	  command.append(s);
      }
      command.append('\'');
      command.append(" ");
    }
    return command.toString();
  }
  
  
  /**
   * Method to return the location of user log directory.
   * 
   * @return base log directory
   */
  static File getUserLogDir() {
    if (!LOG_DIR.exists()) {
      boolean b = LOG_DIR.mkdirs();
      if (!b) {
        LOG.debug("mkdirs failed. Ignoring.");
      }
    }
    return LOG_DIR;
  }
  
  /**
   * Get the user log directory for the job jobid.
   * 
   * @param jobid
   * @return user log directory for the job
   */
  public static File getJobDir(JobID jobid) {
    return new File(getUserLogDir(), jobid.toString());
  }

} // TaskLog
